use std::collections::HashMap;
use std::sync::Mutex;

use actix_web::{delete, get, patch, post, web, HttpRequest, HttpResponse, Responder};
use serde::Deserialize;

use crate::TTL_DEFAULT;

struct MapValue {
    value: serde_json::Value,
    ttl: u64,
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

#[derive(Deserialize)]
struct InsertOpts {
    ttl: Option<u64>,
}

enum FormatEnum {
    Text,
    Json,
}

#[derive(Deserialize)]
struct ListOptions {
    format: Option<String>,
    limit: Option<usize>,
    skip: Option<usize>,
    prefix: Option<String>,
    reverse: Option<bool>,
    values: Option<bool>,
}

fn format_map<'a, I>(format: FormatEnum, values: bool, st_map: I) -> String
where
    I: Iterator<Item = (&'a String, &'a MapValue)>,
{
    match format {
        FormatEnum::Json => {
            if values {
                serde_json::json!(st_map
                    .map(|(k, v)| serde_json::json!([
                        serde_json::json!(k.as_str()),
                        serde_json::json!(&v.value)
                    ]))
                    .collect::<serde_json::Value>())
                .to_string()
            } else {
                serde_json::json!(st_map.map(|(k, _)| k.as_str()).collect::<Vec<&str>>())
                    .to_string()
            }
        }
        FormatEnum::Text => {
            let key_val_list: Vec<String> = if values {
                st_map.map(|(k, v)| format!("{}={}", k, v.value)).collect()
            } else {
                st_map.map(|(k, _)| format!("{}", k)).collect()
            };
            key_val_list.join("\n")
        }
    }
}

#[get("/")]
async fn list_keys(
    _req: HttpRequest,
    query: web::Query<ListOptions>,
    st_map: web::Data<Map>,
) -> impl Responder {
    let format = match &query.format {
        Some(val) => match val.as_str() {
            "text" => FormatEnum::Text,
            "json" => FormatEnum::Json,
            _ => FormatEnum::Json,
        },
        None => FormatEnum::Text,
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
    HttpResponse::Ok().body(format_map(
        format,
        values,
        key_lists
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .skip(skip)
            .take(limit),
    ))
}

#[get("/{key}")]
async fn get_val(web::Path(key): web::Path<String>, st_map: web::Data<Map>) -> impl Responder {
    let lock = st_map.map.lock().unwrap();
    HttpResponse::Ok().body(match lock.get(&key) {
        Some(val) => format!(
            "Found value {} {} for key {} with ttl {}",
            val.value.as_u64().unwrap_or(2),
            val.value,
            key,
            val.ttl
        ),
        None => "No value found".to_string(),
    })
}

#[post("/{key}")]
async fn insert_key(
    data: web::Json<serde_json::Value>,
    web::Path(key): web::Path<String>,
    query: web::Query<InsertOpts>,
    st_map: web::Data<Map>,
) -> impl Responder {
    let mut lock = st_map.map.lock().unwrap();
    let ttl = query.ttl.unwrap_or(60);
    lock.insert(
        key,
        MapValue {
            value: data.into_inner(),
            ttl,
        },
    );
    HttpResponse::Created().body("Inserted")
}

#[patch("/{key}")]
async fn patch_key(
    data: String,
    web::Path(key): web::Path<String>,
    st_map: web::Data<Map>,
) -> impl Responder {
    let mut lock = st_map.map.lock().unwrap();
    if data.starts_with("+") | data.starts_with("-") {
        let increment =
            if data.starts_with("+") { 1 } else { -1 } * data[1..].parse::<i64>().unwrap();
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
    HttpResponse::Ok().body(match lock.remove(&key) {
        Some(_) => "Key removed",
        None => "Key not found",
    })
}
