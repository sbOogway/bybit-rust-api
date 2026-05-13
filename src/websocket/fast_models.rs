use serde::{Deserialize, Deserializer};
use std::fmt;
// use serde_with



#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum BybitResponse {
    Ticker(TickerSnapshot),
    Command(CommandResponse),
    Execution(ExecutionResponse),
    Any(String),
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
    #[serde(deserialize_with = "parse_float")]
    pub bid_price: f32, 
    #[serde(deserialize_with = "parse_float")]
    pub bid_size: f32,
    #[serde(deserialize_with = "parse_float")]
    pub bid_iv: f32,
    #[serde(deserialize_with = "parse_float")]
    pub ask_price: f32,
    #[serde(deserialize_with = "parse_float")]
    pub ask_size: f32,
    #[serde(deserialize_with = "parse_float")]
    pub ask_iv: f32,
    #[serde(deserialize_with = "parse_float")]
    pub last_price: f32,
    pub high_price24h: String,
    pub low_price24h: String,
    #[serde(deserialize_with = "parse_float")]
    pub mark_price: f32,
    pub index_price: String,
    #[serde(deserialize_with = "parse_float")]
    pub mark_price_iv: f32,
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

pub fn parse_float<'de, D>(deserializer: D) -> Result<f32, D::Error>
where
    D: Deserializer<'de>,
{
    // Inner enum to accept either a raw number or a string from the input JSON
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrFloat {
        String(String),
        Float(f32),
    }

    match StringOrFloat::deserialize(deserializer)? {
        StringOrFloat::String(s) => s.parse::<f32>().map_err(serde::de::Error::custom),
        StringOrFloat::Float(f) => Ok(f),
    }
}
