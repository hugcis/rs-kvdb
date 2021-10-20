extern crate actix_web;
extern crate env_logger;

use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};
use rust_keyvaldb::handlers;
use rust_keyvaldb::handlers::Map;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();
    let prefix = "/api";
    let data = web::Data::new(Map::new());

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(data.clone())
            .service(
                web::scope(prefix)
                    .service(handlers::get_val)
                    .service(handlers::insert_key_txn)
                    .service(handlers::insert_key)
                    .service(handlers::patch_key)
                    .service(handlers::list_keys),
            )
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
