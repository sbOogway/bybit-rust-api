use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use log::debug;
use std::hash::{Hash, Hasher};

// use regex::Captures;
use regex::Regex;
use std::str::FromStr;
use std::sync::OnceLock;

use crate::websocket::fast_models::parse_float;

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub enum OptionType {
    Put,
    Call,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct BybitInfo {
    pub base: String,
    pub expire: DateTime<Utc>,
    pub strike_price: String,
    // pub mantissa: u8,
    pub opt_type: OptionType,
    pub quote: Option<String>,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct BybitPosition {
    pub bybit_info: BybitInfo,
    /// true -> long, false -> call
    pub side: bool,
}

pub fn parse_expiration_date(date: &str) -> DateTime<Utc> {
    let naive_date = NaiveDate::parse_from_str(date, "%d%b%y")
        .expect("error parsing expire date from bybit symbol");

    return naive_date
        .and_hms_opt(8, 0, 0)
        .expect("error creating utc datetime object from bybit symbol")
        .and_utc();
}

pub fn calculate_years_to_maturity(expire: DateTime<Utc>) -> f32 {
    debug!("expire date time obj: {}", expire);
    let time_to_expiration: TimeDelta = expire - Utc::now();
    debug!("time_to_expiration: {}", time_to_expiration);
    let seconds_to_expiration = time_to_expiration.num_seconds();
    debug!("seconds_to_expiration: {}", seconds_to_expiration);

    let years_to_expiration = (seconds_to_expiration) as f32 / (60 * 60 * 24 * 365) as f32;
    debug!("years_to_expiration: {}", years_to_expiration);
    return years_to_expiration;
}

pub fn extract_bybit_info(symbol: &str) -> Option<BybitInfo> {
    static RE: OnceLock<Regex> = OnceLock::new();

    let re = RE.get_or_init(|| {
        Regex::new(r"(?<base>\w+)-(?<expire>\d+\w+\d+)-(?<strike_price>\d+\.?\d*)-(?<side>C|P)(?:-(?<quote>USDT))?")
            .expect("invalid regex extracting bybit infos from symbol!")
    });

    re.captures(symbol).map(|caps| {
        let strike_price = &caps["strike_price"];
        // let strike_price_split: Vec<&str> = strike_price.split(".").collect();
        // let mantissa = strike_price_split[1].len();
        BybitInfo {
            base: caps["base"].to_string(),
            expire: parse_expiration_date(&caps["expire"]),
            strike_price: strike_price.to_string(),
            // mantissa: mantissa as u8,
            opt_type: match &caps["side"] {
                "C" => OptionType::Call,
                "P" => OptionType::Put,
                _ => unreachable!(),
            },
            quote: caps.name("quote").map(|m| m.as_str().to_string()),
        }
    })
}

use reqwest::{Client, Error};
use serde::{Deserialize, Serialize};

// Definiamo le strutture per mappare il JSON di Bybit
#[derive(Debug, Serialize, Deserialize)]
pub struct HttpBybitResponse {
    #[serde(rename = "retCode")]
    pub ret_code: i32,
    #[serde(rename = "retMsg")]
    pub ret_msg: String,
    pub result: TickersResult,
    pub time: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TickersResult {
    pub category: String,
    pub list: Vec<OptionTicker>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OptionTicker {
    pub symbol: String,
    pub bid1_price: String,
    pub ask1_price: String,
    pub last_price: String,
    pub mark_price: String,
    pub open_interest: String,
    #[serde(deserialize_with = "parse_float")]
    pub delta: f32,
    // Aggiungi altri campi se necessario
}

/// Funzione di libreria che restituisce il JSON già parsato in una Struct
pub async fn get_option_tickers_json(base_coin: &str) -> Result<HttpBybitResponse, Error> {
    let client = Client::new();
    let url = "https://api.bybit.com/v5/market/tickers";

    let response = client
        .get(url)
        .query(&[("category", "option"), ("baseCoin", base_coin)])
        .send()
        .await?;

    // Deserializza automaticamente il corpo della risposta nel tipo BybitResponse
    response.json::<HttpBybitResponse>().await
}
