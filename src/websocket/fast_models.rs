use serde::Deserialize;
use std::fmt;

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum BybitResponse {
    Ticker(TickerSnapshot),
    Command(CommandResponse),
    Execution(ExecutionResponse),
    Empty(),
}

#[derive(Deserialize, Debug)]
pub struct TickerSnapshot {
    pub topic: String,
    pub ts: u64,
    #[serde(rename = "type")]
    pub msg_type: String,
    pub data: TickerData,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TickerData {
    pub symbol: String,
    pub bid_price: String, // Spesso restituito come stringa nelle API crypto per precisione
    pub bid_size: String,
    pub bid_iv: String,
    pub ask_price: String,
    pub ask_size: String,
    pub ask_iv: String,
    pub last_price: String,
    pub high_price24h: String,
    pub low_price24h: String,
    pub mark_price: String,
    pub index_price: String,
    pub mark_price_iv: String,
    pub underlying_price: String,
    pub open_interest: String,
    pub turnover24h: String,
    pub volume24h: String,
    pub total_volume: String,
    pub total_turnover: String,
    pub delta: String,
    pub gamma: String,
    pub vega: String,
    pub theta: String,
    pub predicted_delivery_price: String,
    pub change24h: String,
}

#[derive(Deserialize, Debug)]
pub struct CommandResponse {
    pub op: String,
    #[serde(alias = "retCode")]
    pub ret_code: Option<i32>,
    #[serde(alias = "retMsg")]
    pub ret_msg: Option<String>,
    pub success: Option<bool>,
    #[serde(alias = "connId", alias = "conn_id")]
    pub conn_id: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct ExecutionResponse {
    pub success: bool,
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(alias = "connId", alias = "conn_id")]
    pub conn_id: Option<String>,
}

impl fmt::Display for TickerData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Scrivi qui come vuoi che appaia la stringa
        write!(
            f,
            "Symbol: {} | Mark Price: {} | Delta: {}",
            self.symbol, self.mark_price, self.delta
        )
    }
}
