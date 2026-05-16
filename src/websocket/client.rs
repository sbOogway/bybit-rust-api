//! WebSocket client implementation.

use disruptor::{MultiProducer, Producer, SingleConsumerBarrier};
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
// use tracing_subscriber::field::debug;
use log::{debug, error, info};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use crate::auth::{generate_ws_signature, get_timestamp};
use crate::config::WsConfig;
use crate::error::{BybitError, Result};
// use crate::utils::BybitResponse;
use crate::websocket::fast_models::BybitResponse;
use crate::websocket::models::*;
use crate::{MAINNET_WS_TRADE, TESTNET_WS_TRADE};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Callback = Arc<dyn Fn(WsMessage) + Send + Sync>;

// use simd_json::prelude::*;

/// WebSocket client for Bybit streaming API.
pub struct BybitWebSocket {
    config: WsConfig,
    subscriptions: Arc<RwLock<Vec<String>>>,
    callbacks: Arc<RwLock<HashMap<String, Callback>>>,
    tx: Option<mpsc::Sender<Message>>,
    is_connected: Arc<RwLock<bool>>,
    is_trade: bool,
    producer: Option<MultiProducer<BybitResponse, SingleConsumerBarrier>>,
}

impl BybitWebSocket {
    /// Create a new public WebSocket client.
    pub fn public(url: &str) -> Self {
        Self {
            config: WsConfig::public(url),
            subscriptions: Arc::new(RwLock::new(Vec::new())),
            callbacks: Arc::new(RwLock::new(HashMap::new())),
            tx: None,
            is_connected: Arc::new(RwLock::new(false)),
            is_trade: false,
            producer: None,
        }
    }

    /// Create a new private WebSocket client.
    pub fn private(api_key: &str, api_secret: &str, url: &str) -> Self {
        Self {
            config: WsConfig::private(api_key, api_secret).with_url(url),
            subscriptions: Arc::new(RwLock::new(Vec::new())),
            callbacks: Arc::new(RwLock::new(HashMap::new())),
            tx: None,
            is_connected: Arc::new(RwLock::new(false)),
            is_trade: url == MAINNET_WS_TRADE || url == TESTNET_WS_TRADE,
            producer: None,
        }
    }

    pub fn set_disruptor_producer(
        &mut self,
        producer: MultiProducer<BybitResponse, SingleConsumerBarrier>,
    ) {
        self.producer = Some(producer);
        debug!("producer set ",);
    }

    /// Connect to the WebSocket server.
    pub async fn connect(&mut self) -> Result<()> {
        let url = &self.config.url;
        info!("Connecting to WebSocket:  {}", url);

        let (ws_stream, _) = connect_async(url)
            .await
            .map_err(|e| BybitError::WebSocket(Box::new(e)))?;

        let (write, read) = ws_stream.split();

        // Create channel for sending messages
        let (tx, mut rx) = mpsc::channel::<Message>(100);
        self.tx = Some(tx.clone());

        // Set connected flag
        *self.is_connected.write().await = true;

        // Spawn write task
        let write = Arc::new(tokio::sync::Mutex::new(write));
        let write_clone = write.clone();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let mut w = write_clone.lock().await;
                if let Err(e) = w.send(msg).await {
                    error!("Failed to send message: {}", e);
                    break;
                }
            }
        });

        // Authenticate if private channel
        if self.config.api_key.is_some() {
            self.authenticate().await?;
        }

        // Spawn ping task
        let tx_ping = tx.clone();
        let ping_interval = self.config.ping_interval;
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(ping_interval));
            loop {
                interval.tick().await;
                let ping = WsPing::new();
                let msg = serde_json::to_string(&ping).unwrap_or_default();
                if tx_ping.send(Message::Text(msg)).await.is_err() {
                    break;
                }
                debug!("Ping sent");
            }
        });

        // let mut read_clone = read.clone();
        // Spawn read task
        // let callbacks = self.callbacks.clone();
        // let is_connected = self.is_connected.clone();
        // let subscriptions = self.subscriptions.clone();
        // let config = self.config.clone();
        // let tx_reconnect = tx.clone();

        // tokio::spawn(async move {
        //     Self::handle_messages(
        //         read,
        //         callbacks,
        //         is_connected,
        //         subscriptions,
        //         config,
        //         tx_reconnect,
        //     )
        //     .await;
        // });
        // let dis = disruptor::build_multi_producer(64, || BybitResponse::Empty(), BusySpinWithSpinLoopHint)
        //     .pin_at_core(1)
        //     .handle_events_with(|_,_,_| {})
        //     .build();

        let producer_clone = self.producer.clone().unwrap();

        tokio::spawn(async move {
            Self::faster_handle(read, producer_clone).await;
        });

        info!("WebSocket connected");
        Ok(())
    }

    async fn faster_handle(
        mut read: futures_util::stream::SplitStream<WsStream>,
        mut producer: MultiProducer<BybitResponse, SingleConsumerBarrier>,
    ) {
        while let Some(msg) = read.next().await {
            let Ok(Message::Text(text)) = msg else {
                continue;
            };

            debug!("gate from socket: {}", &text);

            match serde_json::from_str::<BybitResponse>(&text) {
                Ok(response) => {
                    // debug!("{}", text);
                    producer.publish(|e| {
                        *e = response;
                    });
                }
                Err(error) => {
                    error!("fuck! error parsing message from websocket {}", error)
                }
            }
        }
    }

    /// Handle incoming messages.
    // async fn handle_messages(
    //     mut read: futures_util::stream::SplitStream<WsStream>,
    //     callbacks: Arc<RwLock<HashMap<String, Callback>>>,
    //     is_connected: Arc<RwLock<bool>>,
    //     _subscriptions: Arc<RwLock<Vec<String>>>,
    //     _config: WsConfig,
    //     _tx: mpsc::Sender<Message>,
    // ) {
    //     while let Some(msg_result) = read.next().await {
    //         match msg_result {
    //             Ok(Message::Text(text)) => {
    //                 // Try to parse as JSON
    //                 let json: serde_json::Value = match serde_json::from_str(&text) {
    //                     Ok(v) => v,
    //                     Err(e) => {
    //                         warn!(
    //                             "Failed to parse message: {}, text: {}",
    //                             e,
    //                             &text[..text.len().min(200)]
    //                         );
    //                         continue; // Don't panic, continue processing
    //                     }
    //                 };

    //                 // Handle different message types
    //                 if is_pong(&json) {
    //                     debug!("Pong received");
    //                     continue;
    //                 }

    //                 if is_auth_response(&json) {
    //                     if json
    //                         .get("success")
    //                         .and_then(|v| v.as_bool())
    //                         .unwrap_or(false)
    //                         || json.get("retCode").and_then(|v| v.as_i64()) == Some(0)
    //                     // ^^^ this is for *_WS_TRADE ^^^
    //                     // https://bybit-exchange.github.io/docs/v5/websocket/trade/guideline#response-parameters
    //                     {
    //                         info!("Authentication successful");
    //                     } else {
    //                         error!("Authentication failed: {:?}", json);
    //                     }
    //                     continue;
    //                 }

    //                 if is_subscription_response(&json) {
    //                     if json
    //                         .get("success")
    //                         .and_then(|v| v.as_bool())
    //                         .unwrap_or(false)
    //                     {
    //                         debug!("Subscription successful");
    //                     } else {
    //                         warn!("Subscription failed: {:?}", json);
    //                     }
    //                     continue;
    //                 }

    //                 // Handle data message
    //                 if is_data_message(&json) {
    //                     if let Ok(ws_msg) = serde_json::from_value::<WsMessage>(json) {
    //                         let cbs = callbacks.read().await;
    //                         if let Some(callback) = cbs.get(&ws_msg.topic) {
    //                             callback(ws_msg.clone());
    //                         } else {
    //                             // Try to find matching callback by prefix
    //                             for (topic, callback) in cbs.iter() {
    //                                 if ws_msg
    //                                     .topic
    //                                     .starts_with(topic.split('.').next().unwrap_or(""))
    //                                 {
    //                                     callback(ws_msg.clone());
    //                                     break;
    //                                 }
    //                             }
    //                         }
    //                     }
    //                 }

    //                 debug!("{:#?}", text);
    //             }
    //             Ok(Message::Ping(_)) => {
    //                 debug!("Received ping frame");
    //                 // Tungstenite handles pong automatically
    //             }
    //             Ok(Message::Close(_)) => {
    //                 info!("WebSocket closed");
    //                 *is_connected.write().await = false;
    //                 break;
    //             }
    //             Err(e) => {
    //                 error!("WebSocket error: {}", e);
    //                 *is_connected.write().await = false;
    //                 break;
    //             }
    //             _ => {}
    //         }
    //     }
    // }

    /// Authenticate with the server (for private channels).
    async fn authenticate(&self) -> Result<()> {
        let api_key = self
            .config
            .api_key
            .as_ref()
            .ok_or_else(|| BybitError::Auth("API key not set".into()))?;
        let api_secret = self
            .config
            .api_secret
            .as_ref()
            .ok_or_else(|| BybitError::Auth("API secret not set".into()))?;

        // 10_000 secs are three hours
        let expires = get_timestamp() + 10000;
        let signature = generate_ws_signature(api_secret, expires);

        let auth_msg = if self.is_trade {
            AuthRequest::Trade(WsTradeAuthRequest {
                req_id: uuid::Uuid::new_v4().to_string(),
                op: "auth".to_string(),
                args: vec![
                    serde_json::Value::String(api_key.clone()),
                    serde_json::Value::Number(expires.into()),
                    serde_json::Value::String(signature),
                ],
            })
        } else {
            AuthRequest::Public(WsAuthRequest {
                req_id: uuid::Uuid::new_v4().to_string(),
                op: "auth".to_string(),
                args: vec![
                    serde_json::Value::String(api_key.clone()),
                    serde_json::Value::Number(expires.into()),
                    serde_json::Value::String(signature),
                ],
            })
        };
        let msg = serde_json::to_string(&auth_msg).map_err(|e| BybitError::Parse(e.to_string()))?;

        self.send(msg).await?;
        info!("Authentication request sent");
        Ok(())
    }

    /// Subscribe to topics.
    ///
    /// # Arguments
    /// * `topics` - List of topics to subscribe
    /// * `callback` - Callback function for received messages
    pub async fn subscribe<F>(&mut self, topics: Vec<String>, callback: F) -> Result<()>
    where
        F: Fn(WsMessage) + Send + Sync + 'static,
    {
        let callback = Arc::new(callback) as Callback;

        // Register callbacks
        {
            let mut cbs = self.callbacks.write().await;
            for topic in &topics {
                cbs.insert(topic.clone(), callback.clone());
            }
        }

        // Store subscriptions
        {
            let mut subs = self.subscriptions.write().await;
            subs.extend(topics.clone());
        }

        // Send subscription request
        let sub_msg = WsRequest {
            req_id: uuid::Uuid::new_v4().to_string(),
            op: "subscribe".to_string(),
            args: topics,
        };

        let msg = serde_json::to_string(&sub_msg).map_err(|e| BybitError::Parse(e.to_string()))?;

        self.send(msg).await
    }

    pub async fn subscribe_mut<F>(&mut self, topics: Vec<String>, callback: F) -> Result<()>
    where
        F: FnMut(WsMessage) + Send + Sync + 'static,
    {
        // 1. Wrap the FnMut in a Mutex to "convert" it to an Fn closure
        let callback_mutable = Mutex::new(callback);

        // 2. Create an Fn closure that locks the mutex and calls the inner FnMut
        let wrapped_callback = move |msg: WsMessage| {
            let mut cb = callback_mutable.lock();
            (&mut *cb)(msg);
        };

        // 3. Wrap in Arc and cast to your existing Callback type
        let callback_arc = Arc::new(wrapped_callback) as Callback;

        // --- The rest of the logic remains the same as your original function ---

        // Register callbacks
        {
            let mut cbs = self.callbacks.write().await;
            for topic in &topics {
                cbs.insert(topic.clone(), callback_arc.clone());
            }
        }

        // Store subscriptions
        {
            let mut subs = self.subscriptions.write().await;
            subs.extend(topics.clone());
        }

        // Send subscription request
        let sub_msg = WsRequest {
            req_id: uuid::Uuid::new_v4().to_string(),
            op: "subscribe".to_string(),
            args: topics,
        };

        let msg = serde_json::to_string(&sub_msg).map_err(|e| BybitError::Parse(e.to_string()))?;

        self.send(msg).await
    }

    /// Unsubscribe from topics.
    pub async fn unsubscribe(&mut self, topics: Vec<String>) -> Result<()> {
        // Remove callbacks
        {
            let mut cbs = self.callbacks.write().await;
            for topic in &topics {
                cbs.remove(topic);
            }
        }

        // Remove from subscriptions
        {
            let mut subs = self.subscriptions.write().await;
            subs.retain(|t| !topics.contains(t));
        }

        // Send unsubscribe request
        let unsub_msg = WsRequest {
            req_id: uuid::Uuid::new_v4().to_string(),
            op: "unsubscribe".to_string(),
            args: topics,
        };

        let msg =
            serde_json::to_string(&unsub_msg).map_err(|e| BybitError::Parse(e.to_string()))?;

        self.send(msg).await
    }

    /// Send a message.
    async fn send(&self, msg: String) -> Result<()> {
        if let Some(tx) = &self.tx {
            tx.send(Message::Text(msg)).await.map_err(|_| {
                BybitError::WebSocket(Box::new(
                    tokio_tungstenite::tungstenite::Error::AlreadyClosed,
                ))
            })?;
        }
        Ok(())
    }

    pub fn send_sync(&self, msg: String) {
        if let Some(tx) = &self.tx {
            debug!("about to send message into socket: {}", &msg);
            match tx.try_send(Message::Text(msg)) {
                Ok(mex) => {
                    debug!("sent message into socket {}: {:#?}", self.config.url, &mex)
                }
                Err(err) => error!("{}", err),
            }
        }
    }

    pub async fn send_order(&self, order: WsTradeOrder) -> Result<()> {
        debug!("{:#?}", order);
        if !self.is_trade {
            error!("can t execute a trade on a non trade socket");
            return Err(BybitError::Parse(
                "can t execute a trade on a non trade socket".to_string(),
            ));
        }
        let msg = serde_json::to_string(&order).map_err(|e| BybitError::Parse(e.to_string()))?;
        self.send(msg).await
    }

    /// Check if connected.
    pub async fn is_connected(&self) -> bool {
        *self.is_connected.read().await
    }

    /// Disconnect from the server.
    pub async fn disconnect(&mut self) -> Result<()> {
        *self.is_connected.write().await = false;
        self.tx = None;
        info!("WebSocket disconnected");
        Ok(())
    }
}
