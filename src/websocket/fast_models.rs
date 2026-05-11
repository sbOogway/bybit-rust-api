use serde::Deserialize;


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
    pub mark_price: String,
    pub index_price: String,
    pub delta: String,
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
