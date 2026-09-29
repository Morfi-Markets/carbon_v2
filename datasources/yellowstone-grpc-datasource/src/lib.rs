use {
    async_trait::async_trait,
    carbon_core::{
        datasource::{
            AccountUpdate, Datasource, DatasourceDisconnection, DatasourceId, TransactionUpdate,
            Update, UpdateType,
        },
        error::CarbonResult,
        metrics::{Counter, Histogram, MetricsRegistry},
        transformers::yellowstone::{create_tx_meta, create_tx_versioned},
    },
    chrono::{DateTime, Utc},
    futures::{sink::SinkExt, StreamExt},
    solana_account::Account,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        collections::HashMap,
        convert::TryFrom,
        sync::{Arc, LazyLock},
        time::Duration,
    },
    tokio::sync::{mpsc, mpsc::Sender},
    tokio_util::sync::CancellationToken,
    yellowstone_grpc_client::{
        GeyserGrpcBuilder, GeyserGrpcBuilderResult, GeyserGrpcClient, GeyserGrpcClientError,
        GeyserGrpcClientResult, GeyserStream, ReconnectConfig, SubscribeRequestSink,
    },
    yellowstone_grpc_proto::{
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
            SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocks,
            SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterTransactions,
            SubscribeRequestPing, SubscribeUpdateAccountInfo, SubscribeUpdateBlockMeta,
            SubscribeUpdateTransactionInfo,
        },
        prost_types::Timestamp,
        tonic::{codec::CompressionEncoding, transport::ClientTlsConfig, Status},
    },
};

static ACCOUNT_PROCESS_TIME_NANOS: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::new(
        "yellowstone_grpc_account_process_time_nanoseconds",
        "Time taken to process account updates in nanoseconds",
        vec![
            1_000.0,
            10_000.0,
            100_000.0,
            1_000_000.0,
            10_000_000.0,
            100_000_000.0,
            1_000_000_000.0,
        ],
    )
});
static ACCOUNT_UPDATES_RECEIVED: Counter = Counter::new(
    "yellowstone_grpc_account_updates_received_total",
    "Total account updates received from Yellowstone gRPC",
);
static ACCOUNT_DELETION_PROCESS_TIME_NANOS: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::new(
        "yellowstone_grpc_account_deletion_process_time_nanoseconds",
        "Time taken to process account deletions in nanoseconds",
        vec![
            1_000.0,
            10_000.0,
            100_000.0,
            1_000_000.0,
            10_000_000.0,
            100_000_000.0,
            1_000_000_000.0,
        ],
    )
});
static ACCOUNT_DELETIONS_RECEIVED: Counter = Counter::new(
    "yellowstone_grpc_account_deletions_received_total",
    "Total account deletions received from Yellowstone gRPC",
);
static TRANSACTION_PROCESS_TIME_NANOS: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::new(
        "yellowstone_grpc_transaction_process_time_nanoseconds",
        "Time taken to process transaction updates in nanoseconds",
        vec![
            1_000.0,
            10_000.0,
            100_000.0,
            1_000_000.0,
            10_000_000.0,
            100_000_000.0,
            1_000_000_000.0,
        ],
    )
});
static TRANSACTION_UPDATES_RECEIVED: Counter = Counter::new(
    "yellowstone_grpc_transaction_updates_received_total",
    "Total transaction updates received from Yellowstone gRPC",
);
static BLOCK_META_UPDATES_RECEIVED: Counter = Counter::new(
    "yellowstone_grpc_block_meta_updates_received_total",
    "Total block meta updates received from Yellowstone gRPC",
);

fn register_yellowstone_metrics() {
    let registry = MetricsRegistry::global();
    registry.register_counter(&ACCOUNT_UPDATES_RECEIVED);
    registry.register_counter(&ACCOUNT_DELETIONS_RECEIVED);
    registry.register_counter(&TRANSACTION_UPDATES_RECEIVED);
    registry.register_histogram(&ACCOUNT_PROCESS_TIME_NANOS);
    registry.register_histogram(&ACCOUNT_DELETION_PROCESS_TIME_NANOS);
    registry.register_histogram(&TRANSACTION_PROCESS_TIME_NANOS);
    registry.register_counter(&BLOCK_META_UPDATES_RECEIVED);
}

/// Default timeout for detecting stale connections (30 seconds)
pub const DEFAULT_STREAM_TIMEOUT_SECS: u64 = 30;

/// Initial delay before retrying a failed subscription attempt
const RECONNECT_INITIAL_DELAY_MS: u64 = 100;

/// Upper bound on the retry delay
const RECONNECT_MAX_DELAY_MS: u64 = 3_000;

/// A subscribe call can hang without erroring; retry it after this long.
const SUBSCRIBE_DEADLINE: Duration = Duration::from_secs(15);

/// Log/disconnect source label when no tag is set. Deliberately not the
/// endpoint, which may carry credentials.
const DEFAULT_TAG: &str = "yellowstone-grpc";

/// Returns the block time for a slot, if already known (e.g. from blocks_meta).
pub type BlockTimeResolver = Arc<dyn Fn(u64) -> Option<i64> + Send + Sync>;
/// Called with every blocks_meta update as soon as it is received, before
/// any pipeline queueing, so receive-time measurements stay accurate. The
/// second argument is the provider's `SubscribeUpdate.created_at`: set on the
/// provider's clock, and not guaranteed to be populated by every provider.
pub type BlockMetaObserver =
    Arc<dyn Fn(&SubscribeUpdateBlockMeta, Option<&Timestamp>) + Send + Sync>;

/// Per-transaction delivery timing, for provider latency telemetry.
#[derive(Debug, Clone)]
pub struct TransactionTiming {
    pub slot: u64,
    pub block_time: Option<i64>,
    /// Provider-side `SubscribeUpdate.created_at`, Unix seconds.
    pub provider_created_at: Option<i64>,
}

pub type TransactionTimingObserver = Arc<dyn Fn(&TransactionTiming) + Send + Sync>;

pub struct YellowstoneGrpcGeyserClient {
    pub endpoint: String,
    pub x_token: Option<String>,
    pub commitment: Option<CommitmentLevel>,
    pub account_filters: HashMap<String, SubscribeRequestFilterAccounts>,
    pub transaction_filters: HashMap<String, SubscribeRequestFilterTransactions>,
    pub block_filters: BlockFilters,
    pub geyser_config: YellowstoneGrpcClientConfig,
    pub disconnect_notifier: Option<mpsc::Sender<DatasourceDisconnection>>,
    /// Timeout for detecting hung/stale connections. Default: 30 seconds.
    pub stream_timeout: Duration,
    pub blocks_meta_filters: HashMap<String, SubscribeRequestFilterBlocksMeta>,
    /// Label for logs, readiness events, and disconnect notifications.
    pub tag: Option<String>,
    pub block_time_resolver: Option<BlockTimeResolver>,
    pub block_meta_observer: Option<BlockMetaObserver>,
    pub transaction_timing_observer: Option<TransactionTimingObserver>,
}

#[derive(Debug, Clone)]
pub struct YellowstoneGrpcClientConfig {
    pub compression: Option<CompressionEncoding>,
    pub connect_timeout: Option<Duration>,
    pub timeout: Option<Duration>,
    pub max_decoding_message_size: Option<usize>,
    pub tls_config: Option<ClientTlsConfig>,
    pub tcp_nodelay: Option<bool>,
    /// When set, the client reconnects and replays inside the stream instead of
    /// surfacing the disconnect. Off by default.
    pub reconnect: Option<ReconnectConfig>,
    pub http2_adaptive_window: Option<bool>,
    pub http2_keep_alive_interval: Option<Duration>,
    pub keep_alive_while_idle: Option<bool>,
}

impl Default for YellowstoneGrpcClientConfig {
    fn default() -> Self {
        Self {
            compression: None,
            connect_timeout: Some(Duration::from_secs(15)),
            timeout: Some(Duration::from_secs(15)),
            max_decoding_message_size: None,
            tls_config: None,
            tcp_nodelay: None,
            reconnect: None,
            http2_adaptive_window: None,
            http2_keep_alive_interval: None,
            keep_alive_while_idle: None,
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct BlockFilters {
    pub filters: HashMap<String, SubscribeRequestFilterBlocks>,
    pub failed_transactions: Option<bool>,
}

impl YellowstoneGrpcGeyserClient {
    /// Creates a new YellowstoneGrpcGeyserClient with optional stream timeout.
    /// If `stream_timeout` is None, defaults to 30 seconds.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint: String,
        x_token: Option<String>,
        commitment: Option<CommitmentLevel>,
        account_filters: HashMap<String, SubscribeRequestFilterAccounts>,
        transaction_filters: HashMap<String, SubscribeRequestFilterTransactions>,
        block_filters: BlockFilters,
        geyser_config: YellowstoneGrpcClientConfig,
        disconnect_notifier: Option<mpsc::Sender<DatasourceDisconnection>>,
        stream_timeout: Option<Duration>,
    ) -> Self {
        YellowstoneGrpcGeyserClient {
            endpoint,
            x_token,
            commitment,
            account_filters,
            transaction_filters,
            block_filters,
            geyser_config,
            disconnect_notifier,
            stream_timeout: stream_timeout
                .unwrap_or(Duration::from_secs(DEFAULT_STREAM_TIMEOUT_SECS)),
            blocks_meta_filters: HashMap::new(),
            tag: None,
            block_time_resolver: None,
            block_meta_observer: None,
            transaction_timing_observer: None,
        }
    }

    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = Some(tag.into());
        self
    }

    pub fn with_blocks_meta_filters(
        mut self,
        blocks_meta_filters: HashMap<String, SubscribeRequestFilterBlocksMeta>,
    ) -> Self {
        self.blocks_meta_filters = blocks_meta_filters;
        self
    }

    /// Fills `TransactionUpdate::block_time` for streamed transactions, which
    /// Yellowstone does not carry.
    pub fn with_block_time_resolver(mut self, resolver: BlockTimeResolver) -> Self {
        self.block_time_resolver = Some(resolver);
        self
    }

    pub fn with_block_meta_observer(mut self, observer: BlockMetaObserver) -> Self {
        self.block_meta_observer = Some(observer);
        self
    }

    pub fn with_transaction_timing_observer(mut self, observer: TransactionTimingObserver) -> Self {
        self.transaction_timing_observer = Some(observer);
        self
    }
}

impl YellowstoneGrpcClientConfig {
    pub const fn new(
        compression: Option<CompressionEncoding>,
        connect_timeout: Option<Duration>,
        timeout: Option<Duration>,
        max_decoding_message_size: Option<usize>,
        tls_config: Option<ClientTlsConfig>,
        tcp_nodelay: Option<bool>,
    ) -> Self {
        YellowstoneGrpcClientConfig {
            compression,
            connect_timeout,
            timeout,
            max_decoding_message_size,
            tls_config,
            tcp_nodelay,
            reconnect: None,
            http2_adaptive_window: None,
            http2_keep_alive_interval: None,
            keep_alive_while_idle: None,
        }
    }

    pub fn with_reconnect(self, reconnect: ReconnectConfig) -> Self {
        YellowstoneGrpcClientConfig {
            reconnect: Some(reconnect),
            ..self
        }
    }

    pub fn geyser_config_builder(
        &self,
        mut builder: GeyserGrpcBuilder,
    ) -> GeyserGrpcBuilderResult<GeyserGrpcBuilder> {
        builder = builder.connect_timeout(self.connect_timeout.unwrap_or(Duration::from_secs(15)));

        builder = builder.timeout(self.timeout.unwrap_or(Duration::from_secs(15)));
        let tls = self
            .tls_config
            .clone()
            .unwrap_or_else(|| ClientTlsConfig::new().with_enabled_roots());
        builder = builder.tls_config(tls)?;

        if let Some(compression) = self.compression {
            builder = builder
                .send_compressed(compression)
                .accept_compressed(compression);
        }
        if let Some(val) = self.max_decoding_message_size {
            builder = builder.max_decoding_message_size(val);
        }

        if let Some(val) = self.tcp_nodelay {
            builder = builder.tcp_nodelay(val);
        }

        if let Some(reconnect) = self.reconnect.clone() {
            builder = builder.set_reconnect_config(reconnect);
        }
        if let Some(val) = self.http2_adaptive_window {
            builder = builder.http2_adaptive_window(val);
        }
        if let Some(val) = self.http2_keep_alive_interval {
            builder = builder.http2_keep_alive_interval(val);
        }
        if let Some(val) = self.keep_alive_while_idle {
            builder = builder.keep_alive_while_idle(val);
        }
        Ok(builder)
    }
}

#[async_trait]
impl Datasource for YellowstoneGrpcGeyserClient {
    async fn consume(
        &self,
        id: DatasourceId,
        sender: Sender<(Update, DatasourceId)>,
        cancellation_token: CancellationToken,
    ) -> CarbonResult<()> {
        register_yellowstone_metrics();
        let endpoint = self.endpoint.clone();
        let x_token = self.x_token.clone();
        let commitment = self.commitment;
        let account_filters = self.account_filters.clone();
        let transaction_filters = self.transaction_filters.clone();
        let blocks_meta_filters = self.blocks_meta_filters.clone();
        let BlockFilters {
            filters,
            failed_transactions: block_failed_transactions,
        } = self.block_filters.clone();
        let retain_block_failed_transactions = block_failed_transactions.unwrap_or(true);
        let tag = self.tag.clone().unwrap_or_else(|| DEFAULT_TAG.to_string());

        let builder = GeyserGrpcClient::build_from_shared(endpoint)
            .map_err(|err| carbon_core::error::Error::FailedToConsumeDatasource(err.to_string()))?
            .x_token(x_token)
            .map_err(|err| carbon_core::error::Error::FailedToConsumeDatasource(err.to_string()))?;

        let mut geyser_client = self
            .geyser_config
            .geyser_config_builder(builder)
            .map_err(|err| carbon_core::error::Error::FailedToConsumeDatasource(err.to_string()))?
            .connect()
            .await
            .map_err(|err| {
                log::error!("[{tag}] gRPC connect() failed: {err}");
                carbon_core::error::Error::FailedToConsumeDatasource(err.to_string())
            })?;

        log::info!("[{tag}] gRPC channel connected");
        tracing::info!(
            component = yellowstone_ready_component(&tag),
            server = tag.as_str(),
            "indexer.connection.ready"
        );

        let disconnect_tx_clone = self.disconnect_notifier.clone();
        let stream_timeout = self.stream_timeout;
        let block_time_resolver = self.block_time_resolver.clone();
        let block_meta_observer = self.block_meta_observer.clone();
        let tx_timing_observer = self.transaction_timing_observer.clone();

        tokio::spawn(async move {
            let subscribe_request = SubscribeRequest {
                slots: HashMap::new(),
                accounts: account_filters,
                transactions: transaction_filters,
                transactions_status: HashMap::new(),
                entry: HashMap::new(),
                blocks: filters,
                blocks_meta: blocks_meta_filters,
                commitment: commitment.map(|x| x as i32),
                accounts_data_slice: vec![],
                ping: None,
                from_slot: None,
            };

            let id_for_loop = id.clone();

            let mut last_disconnect_time: Option<DateTime<Utc>> = None;
            let mut last_slot_before_disconnect: Option<u64> = None;
            let mut last_processed_slot: u64 = 0;
            let mut reconnect_delay = Duration::from_millis(RECONNECT_INITIAL_DELAY_MS);

            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        log::info!("[{tag}] Cancelling Yellowstone gRPC subscription.");
                        break;
                    }
                    result = subscribe_with_deadline(&mut geyser_client, &subscribe_request, &tag) => {
                        match result {
                            Ok((mut subscribe_tx, mut stream)) => {
                                log::info!("[{tag}] Subscribe established");
                                tracing::info!(
                                    component = yellowstone_ready_component(&tag),
                                    server = tag.as_str(),
                                    "indexer.yellowstone.subscribe.ready"
                                );
                                reconnect_delay = Duration::from_millis(RECONNECT_INITIAL_DELAY_MS);
                                let mut first_message_after_reconnect = last_disconnect_time.is_some();

                                loop {
                                    if cancellation_token.is_cancelled() {
                                        break;
                                    }

                                    let message_result = tokio::time::timeout(
                                        stream_timeout,
                                        stream.next()
                                    ).await;

                                    let message = match message_result {
                                        Ok(Some(msg)) => msg,
                                        Ok(None) => {
                                            log::warn!("[{tag}] Stream closed");
                                            if last_disconnect_time.is_none() {
                                                last_disconnect_time = Some(Utc::now());
                                                last_slot_before_disconnect = Some(last_processed_slot);
                                                log::warn!("[{tag}] Disconnected at slot {last_processed_slot}");
                                            }
                                            break;
                                        }
                                        Err(_) => {
                                            log::warn!("[{tag}] Stream timeout - no messages for {stream_timeout:?}");
                                            if last_disconnect_time.is_none() {
                                                last_disconnect_time = Some(Utc::now());
                                                last_slot_before_disconnect = Some(last_processed_slot);
                                                log::warn!("[{tag}] Disconnected at slot {last_processed_slot} (timeout)");
                                            }
                                            break;
                                        }
                                    };

                                    match message {
                                        Ok(msg) => {
                                            if first_message_after_reconnect {
                                                let current_slot = match &msg.update_oneof {
                                                    Some(UpdateOneof::Account(ref update)) => Some(update.slot),
                                                    Some(UpdateOneof::Transaction(ref update)) => Some(update.slot),
                                                    Some(UpdateOneof::Block(ref update)) => Some(update.slot),
                                                    _ => None,
                                                };

                                                if let Some(slot) = current_slot {
                                                    first_message_after_reconnect = false;

                                                    if let (Some(disconnect_time), Some(last_slot)) =
                                                        (last_disconnect_time.take(), last_slot_before_disconnect.take())
                                                    {
                                                        let missed = slot.saturating_sub(last_slot);

                                                        let disconnection = DatasourceDisconnection {
                                                            source: tag.clone(),
                                                            disconnect_time,
                                                            last_slot_before_disconnect: last_slot,
                                                            first_slot_after_reconnect: slot,
                                                            missed_slots: missed,
                                                        };

                                                        if let Some(tx) = &disconnect_tx_clone {
                                                            let _ = tx.try_send(disconnection);
                                                        }

                                                        log::info!("[{tag}] Reconnected. Slots: {last_slot} -> {slot} (missed: {missed})");
                                                    }
                                                }
                                            }

                                            let provider_created_at = msg.created_at.as_ref().map(|ts| ts.seconds);

                                            match msg.update_oneof {
                                            Some(UpdateOneof::Account(account_update)) => {
                                                last_processed_slot = account_update.slot;
                                                send_subscribe_account_update_info(
                                                    account_update.account,
                                                    &sender,
                                                    id_for_loop.clone(),
                                                    account_update.slot,
                                                )
                                                .await
                                            }

                                            Some(UpdateOneof::Transaction(transaction_update)) => {
                                                last_processed_slot = transaction_update.slot;
                                                let block_time = block_time_resolver.as_ref().and_then(|resolve| resolve(transaction_update.slot));
                                                if let Some(observer) = &tx_timing_observer {
                                                    observer(&TransactionTiming {
                                                        slot: transaction_update.slot,
                                                        block_time,
                                                        provider_created_at,
                                                    });
                                                }
                                                send_subscribe_update_transaction_info(
                                                    transaction_update.transaction,
                                                    &sender,
                                                    id_for_loop.clone(),
                                                    transaction_update.slot,
                                                    block_time,
                                                )
                                                .await
                                            }
                                            Some(UpdateOneof::Block(block_update)) => {
                                                last_processed_slot = block_update.slot;
                                                let block_time = block_update.block_time.map(|ts| ts.timestamp);

                                                for transaction_update in block_update.transactions {
                                                    if retain_block_failed_transactions || transaction_update.meta.as_ref().map(|meta| meta.err.is_none()).unwrap_or(false) {
                                                        if let Some(observer) = &tx_timing_observer {
                                                            observer(&TransactionTiming {
                                                                slot: block_update.slot,
                                                                block_time,
                                                                provider_created_at,
                                                            });
                                                        }
                                                        send_subscribe_update_transaction_info(Some(transaction_update), &sender, id_for_loop.clone(), block_update.slot, block_time).await
                                                    }
                                                }

                                                for account_info in block_update.accounts {
                                                    send_subscribe_account_update_info(
                                                        Some(account_info),
                                                        &sender,
                                                        id_for_loop.clone(),
                                                        block_update.slot,
                                                    )
                                                    .await;
                                                }
                                            }

                                            Some(UpdateOneof::BlockMeta(block_meta)) => {
                                                last_processed_slot = block_meta.slot;
                                                BLOCK_META_UPDATES_RECEIVED.inc();
                                                if let Some(observer) = &block_meta_observer {
                                                    observer(&block_meta, msg.created_at.as_ref());
                                                }
                                            }

                                            Some(UpdateOneof::Ping(_)) => {
                                                // Sink replays the last request on reconnect.
                                                match subscribe_tx
                                                    .send(SubscribeRequest {
                                                        ping: Some(SubscribeRequestPing { id: 1 }),
                                                        ..subscribe_request.clone()
                                                    })
                                                    .await {
                                                        Ok(()) => (),
                                                        Err(error) => {
                                                            log::error!("[{tag}] Failed to send ping: {error:?}");
                                                            break;
                                                        },
                                                    }
                                            }

                                            _ => {}
                                        }
                                        }
                                        Err(error) => {
                                            log::error!("[{tag}] Geyser stream error: {error:?}");

                                            if last_disconnect_time.is_none() {
                                                last_disconnect_time = Some(Utc::now());
                                                last_slot_before_disconnect = Some(last_processed_slot);
                                                log::error!("[{tag}] Disconnected at slot {last_processed_slot}");
                                            }

                                            break;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                log::error!("[{tag}] Failed to subscribe: {e:?}");

                                if last_disconnect_time.is_none() {
                                    last_disconnect_time = Some(Utc::now());
                                    last_slot_before_disconnect = Some(last_processed_slot);
                                }

                                tokio::select! {
                                    _ = cancellation_token.cancelled() => {
                                        log::info!("[{tag}] Cancelling Yellowstone gRPC subscription.");
                                        break;
                                    }
                                    _ = tokio::time::sleep(reconnect_delay) => {}
                                }

                                reconnect_delay = (reconnect_delay * 2)
                                    .min(Duration::from_millis(RECONNECT_MAX_DELAY_MS));
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    fn update_types(&self) -> Vec<UpdateType> {
        let has_block_filters = !self.block_filters.filters.is_empty();
        let mut update_types = Vec::new();
        if !self.account_filters.is_empty() || has_block_filters {
            update_types.push(UpdateType::AccountUpdate);
            update_types.push(UpdateType::AccountDeletion);
        }
        if !self.transaction_filters.is_empty() || has_block_filters {
            update_types.push(UpdateType::Transaction);
        }
        update_types
    }
}

async fn subscribe_with_deadline(
    geyser_client: &mut GeyserGrpcClient,
    subscribe_request: &SubscribeRequest,
    tag: &str,
) -> GeyserGrpcClientResult<(SubscribeRequestSink, GeyserStream)> {
    tokio::time::timeout(
        SUBSCRIBE_DEADLINE,
        geyser_client.subscribe_with_request(Some(subscribe_request.clone())),
    )
    .await
    .unwrap_or_else(|_| {
        log::error!("[{tag}] subscribe_with_request hung for {SUBSCRIBE_DEADLINE:?}, retrying");
        Err(GeyserGrpcClientError::TonicStatus(
            Status::deadline_exceeded(format!(
                "subscribe_with_request timed out after {SUBSCRIBE_DEADLINE:?}"
            )),
        ))
    })
}

// Downstream readiness probes key on this component name.
fn yellowstone_ready_component(tag: &str) -> &'static str {
    if tag.contains("processed-blockhash") {
        "yellowstone_processed_blockhash"
    } else {
        "yellowstone_confirmed"
    }
}

async fn send_subscribe_account_update_info(
    account_update_info: Option<SubscribeUpdateAccountInfo>,
    sender: &Sender<(Update, DatasourceId)>,
    id: DatasourceId,
    slot: u64,
) {
    let start_time = std::time::Instant::now();

    if let Some(account_info) = account_update_info {
        let Ok(account_pubkey) = Pubkey::try_from(account_info.pubkey) else {
            return;
        };

        let Ok(account_owner_pubkey) = Pubkey::try_from(account_info.owner) else {
            return;
        };

        let account = Account {
            lamports: account_info.lamports,
            data: account_info.data,
            owner: account_owner_pubkey,
            executable: account_info.executable,
            rent_epoch: account_info.rent_epoch,
        };

        let update = AccountUpdate {
            pubkey: account_pubkey,
            account,
            slot,
            transaction_signature: account_info
                .txn_signature
                .and_then(|sig| Signature::try_from(sig).ok()),
        }
        .into_update();
        let is_deletion = matches!(&update, Update::AccountDeletion(_));

        if let Err(e) = sender.try_send((update, id)) {
            log::error!(
                "Failed to send account event for pubkey {account_pubkey:?} at slot {slot}: {e:?}"
            );
        }

        if is_deletion {
            ACCOUNT_DELETION_PROCESS_TIME_NANOS.record(start_time.elapsed().as_nanos() as f64);
            ACCOUNT_DELETIONS_RECEIVED.inc();
        } else {
            ACCOUNT_PROCESS_TIME_NANOS.record(start_time.elapsed().as_nanos() as f64);
            ACCOUNT_UPDATES_RECEIVED.inc();
        }
    } else {
        log::error!("No account info in UpdateOneof::Account at slot {slot}");
    }
}

async fn send_subscribe_update_transaction_info(
    transaction_info: Option<SubscribeUpdateTransactionInfo>,
    sender: &Sender<(Update, DatasourceId)>,
    id: DatasourceId,
    slot: u64,
    block_time: Option<i64>,
) {
    let start_time = std::time::Instant::now();

    if let Some(transaction_info) = transaction_info {
        let Ok(signature) = Signature::try_from(transaction_info.signature) else {
            return;
        };
        let Some(yellowstone_transaction) = transaction_info.transaction else {
            return;
        };
        let Some(yellowstone_tx_meta) = transaction_info.meta else {
            return;
        };
        let Ok(versioned_transaction) = create_tx_versioned(yellowstone_transaction) else {
            return;
        };
        let meta_original = match create_tx_meta(yellowstone_tx_meta) {
            Ok(meta) => meta,
            Err(err) => {
                log::error!("Failed to create transaction meta: {err:?}");
                return;
            }
        };
        let update = Update::Transaction(Box::new(TransactionUpdate {
            signature,
            transaction: versioned_transaction,
            meta: meta_original,
            is_vote: transaction_info.is_vote,
            slot,
            index: Some(transaction_info.index),
            block_time,
            block_hash: None,
        }));
        if let Err(e) = sender.try_send((update, id)) {
            log::error!(
                "Failed to send transaction update with signature {signature:?} at slot {slot}: {e:?}"
            );
            return;
        }

        TRANSACTION_PROCESS_TIME_NANOS.record(start_time.elapsed().as_nanos() as f64);
        TRANSACTION_UPDATES_RECEIVED.inc();
    } else {
        log::error!("No transaction info in `UpdateOneof::Transaction` at slot {slot}");
    }
}
