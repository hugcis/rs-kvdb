extern crate futures;

use actix_web::Error;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::time::{Duration, Instant};

use actix_web::{delete, error, get, patch, post, web, HttpRequest, HttpResponse, Responder};
use futures::StreamExt;
use serde::Deserialize;

use crate::TTL_DEFAULT;
const MAX_SIZE: usize = 262_144; // max payload size is 256k

struct MapValue {
    value: serde_json::Value,
    ttl: u64,
    timestamp: Instant,
}

pub struct Map {
    map: Mutex<HashMap<String, MapValue>>,
}

impl Map {
    pub fn new() -> Map {
        Map {
            map: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for Map {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Deserialize)]
struct SetTx {
    set: String,
    value: serde_json::Value,
    ttl: Option<u64>,
}

#[derive(Deserialize)]
struct DeleteTx {
    delete: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Transaction {
    SetTx(SetTx),
    DeleteTx(DeleteTx),
}

#[derive(Deserialize)]
struct TransactionSet {
    txn: Vec<Transaction>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PostData {
    TransactionSet(TransactionSet),
    Other(serde_json::Value),
}

#[derive(Deserialize)]
struct InsertOpts {
    ttl: Option<u64>,
}

enum FormatEnum {
    Text,
    Json,
}

#[derive(Deserialize)]
struct ListOpts {
    format: Option<String>,
    limit: Option<usize>,
    skip: Option<usize>,
    prefix: Option<String>,
    reverse: Option<bool>,
    values: Option<bool>,
}

/// Format the output (key, value) list from an iterator of key,value pairs.
///
///
/// # Arguments
///
/// * `format` - The output format for the data (text, JSON, etc.).
/// * `values` - A bool saying if we should send the values along with the keys or not.
/// * `st_map` - An Iterator of (String, MapValue) with the available key value pairs.
///
fn format_map<'a, I>(format: FormatEnum, values: bool, st_map: I) -> String
where
    I: Iterator<Item = (&'a String, &'a MapValue)>,
{
    match format {
        FormatEnum::Json => (if values {
            serde_json::json!(st_map
                .map(|(k, v)| (k.clone(), serde_json::json!(&v.value)))
                .collect::<serde_json::map::Map<String, serde_json::Value>>())
        } else {
            serde_json::json!(st_map.map(|(k, _)| k.as_str()).collect::<Vec<&str>>())
        })
        .to_string(),
        FormatEnum::Text => {
            let key_val_list: Vec<String> = if values {
                st_map.map(|(k, v)| format!("{}={}", k, v.value)).collect()
            } else {
                st_map.map(|(k, _)| k.to_string()).collect()
            };
            key_val_list.join("\n")
        }
    }
}

#[get("/")]
async fn list_keys(
    _req: HttpRequest,
    query: web::Query<ListOpts>,
    st_map: web::Data<Map>,
) -> impl Responder {
    let format = match &query.format {
        Some(val) => match val.as_str() {
            "text" => FormatEnum::Text,
            "json" => FormatEnum::Json,
            _ => FormatEnum::Json,
        },
        None => FormatEnum::Json,
    };
    let content_type = match format {
        FormatEnum::Text => "plain/text",
        FormatEnum::Json => "application/json",
    };
    let limit = query
        .limit
        .unwrap_or_else(|| st_map.map.lock().unwrap().len());
    let skip = query.skip.unwrap_or(0);
    let prefix = match &query.prefix {
        Some(pref) => pref,
        None => "",
    };
    let _reverse = query.reverse.unwrap_or(false);
    let values = query.values.unwrap_or(false);
    let key_lists = st_map.map.lock().unwrap();
    HttpResponse::Ok()
        .content_type(content_type)
        .body(format_map(
            format,
            values,
            key_lists
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .filter(|(_, val)| val.timestamp.elapsed() < Duration::from_secs(val.ttl))
                .skip(skip)
                .take(limit),
        ))
}

#[get("/{key}")]
async fn get_val(web::Path(key): web::Path<String>, st_map: web::Data<Map>) -> impl Responder {
    let lock = st_map.map.lock().unwrap();
    match lock.get(&key) {
        Some(val) => {
            if val.timestamp.elapsed() < Duration::from_secs(val.ttl) {
                HttpResponse::Ok().body(serde_json::to_string(&val.value).unwrap())
            } else {
                // Key has expired. Keeping it a different path in case this
                // gets changed into a "there's a value but it has expired"
                // response
                HttpResponse::NotFound().body("Value found but expired".to_string())
            }
        }
        None => HttpResponse::NotFound().body("No value found".to_string()),
    }
}

async fn read_body(mut payload: web::Payload) -> Result<web::BytesMut, error::Error> {
    let mut body = web::BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        // limit max size of in-memory payload
        if (body.len() + chunk.len()) > MAX_SIZE {
            return Err(error::ErrorBadRequest("overflow"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[post("/{key}")]
async fn insert_key(
    payload: web::Payload,
    web::Path(key): web::Path<String>,
    query: web::Query<InsertOpts>,
    st_map: web::Data<Map>,
) -> Result<HttpResponse, Error> {
    let body = read_body(payload).await.unwrap();
    let post_data = serde_json::from_slice::<PostData>(&body)
        .map_err(|e| error::ErrorBadRequest(format!("{}", e)))?;

    let mut lock = st_map.map.lock().unwrap();
    let ttl = query.ttl.unwrap_or(TTL_DEFAULT);

    Ok(match post_data {
        PostData::TransactionSet(_) => {
            HttpResponse::BadRequest().body("Transactions should be used without a key in the path")
        }
        PostData::Other(json) => {
            let timestamp = Instant::now();
            lock.insert(
                key,
                MapValue {
                    value: json,
                    ttl,
                    timestamp,
                },
            );
            HttpResponse::Created().body("Inserted")
        }
    })
}

#[post("/")]
async fn insert_key_txn(
    payload: web::Payload,
    web::Query(query): web::Query<InsertOpts>,
    st_map: web::Data<Map>,
) -> Result<HttpResponse, Error> {
    let body = read_body(payload).await.unwrap();
    let post_data = serde_json::from_slice::<PostData>(&body)
        .map_err(|e| error::ErrorBadRequest(format!("{}", e)))?;

    let mut lock = st_map.map.lock().unwrap();
    let ttl = query.ttl.unwrap_or(TTL_DEFAULT);

    Ok(match post_data {
        PostData::TransactionSet(tx_set) => {
            for item in tx_set.txn {
                apply_action(item, &mut lock, ttl);
            }
            HttpResponse::Created().body("Applied")
        }
        PostData::Other(_) => HttpResponse::BadRequest()
            .body("Invalid payload. Either the transaction is malformed or no key was specified"),
    })
}

fn apply_action(action: Transaction, lock: &mut MutexGuard<HashMap<String, MapValue>>, ttl: u64) {
    let timestamp = Instant::now();
    match action {
        Transaction::SetTx(set_txn) => lock.insert(
            set_txn.set,
            MapValue {
                value: set_txn.value,
                ttl: set_txn.ttl.unwrap_or(ttl),
                timestamp,
            },
        ),
        Transaction::DeleteTx(delete_txn) => lock.remove(&delete_txn.delete),
    };
}

#[patch("/{key}")]
async fn patch_key(
    data: String,
    web::Path(key): web::Path<String>,
    st_map: web::Data<Map>,
) -> impl Responder {
    let mut lock = st_map.map.lock().unwrap();
    if data.starts_with('+') | data.starts_with('-') {
        let increment =
            if data.starts_with('+') { 1 } else { -1 } * data[1..].parse::<i64>().unwrap();
        match lock.get_mut(&key) {
            Some(v) => {
                let found_key = match v.value.as_i64() {
                    Some(mut it) => {
                        it += increment;
                        v.value = serde_json::json!(it);
                        Some(it)
                    }
                    None => None,
                };
                match found_key {
                    Some(val) => HttpResponse::Ok().body(format!("{}", val)),
                    None => HttpResponse::BadRequest().body("Value is not a number"),
                }
            }
            None => {
                lock.insert(
                    key,
                    MapValue {
                        value: serde_json::json!(1),
                        ttl: TTL_DEFAULT,
                        timestamp: Instant::now(),
                    },
                );
                HttpResponse::Ok().body(format!("{}", 1))
            }
        }
    } else {
        HttpResponse::BadRequest().body("Patch requests should be of the form \"+N\"")
    }
}

#[delete("/{key}")]
async fn delete_key(web::Path(key): web::Path<String>, st_map: web::Data<Map>) -> impl Responder {
    let mut lock = st_map.map.lock().unwrap();
    match lock.remove(&key) {
        Some(_) => HttpResponse::Ok().body("Key removed"),
        None => HttpResponse::NotFound().body("Key not found"),
    }
}
