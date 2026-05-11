//! # Bybit API
//!
//! A Rust SDK for the Bybit V5 API.
//!
//! ## Features
//!
//! - Async-first design with tokio
//! - Type-safe request/response models
//! - Zero-panic error handling
//! - Support for REST API and WebSocket
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use bybit_api::{BybitClient, Category};
//!
//! #[tokio::main]
//! async fn main() -> bybit_api::Result<()> {
//!     // Create a client for testnet
//!     let client = BybitClient::testnet("your_api_key", "your_api_secret")?;
//!
//!     // Get tickers
//!     let tickers = client.get_tickers(Category::Linear, Some("BTCUSDT")).await?;
//!     println!("{:?}", tickers);
//!
//!     Ok(())
//! }
//! ```

// Modules
mod auth;
mod client;
mod config;
mod constants;
mod error;
mod models;
pub mod utils;

// API modules
pub mod api;
pub mod websocket;

// Re-exports
pub use client::BybitClient;
pub use config::{ClientConfig, ClientConfigBuilder, WsConfig};
pub use constants::*;
pub use error::{BybitError, Result};
pub use models::*;
