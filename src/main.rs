extern crate actix_web;
extern crate env_logger;

mod handlers;

use actix_web::middleware::Logger;
use handlers::Map;
use actix_web::{
    web, App, HttpServer,
};

const TTL_DEFAULT: u64 = 30;


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let data = web::Data::new(Map::new());
    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(data.clone())
            .service(
                web::scope("/api")
                    .service(handlers::get_val)
                    .service(handlers::insert_key)
                    .service(handlers::patch_key)
                    .service(handlers::list_keys),
            )
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
