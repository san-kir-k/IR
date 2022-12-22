#[macro_use]
extern crate lazy_static;
extern crate futures;

lazy_static! {
    static ref ENGINE: engine::Engine = futures::executor::block_on(engine::init_engine()).unwrap();
}

pub mod db;
pub mod engine;
pub mod index;
pub mod inverted_index;

use actix_web::{post, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Request {
    words: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Response {
    doc_ids: Vec<[u8; 12]>,
}

#[post("/search")]
async fn search(req_body: String) -> impl Responder {
    println!("Request: {req_body}");
    let data: Request = serde_json::from_str(&req_body).unwrap();
    let result = ENGINE.search(data.words).await.unwrap();

    print!("Found doc_ids:\n");
    for oid in &result {
        print!("{:?}\n", oid.to_hex());
    }

    let response = Response {
        doc_ids: result.iter().map(|oid| oid.bytes()).collect(),
    };

    return HttpResponse::Ok().body(serde_json::to_string(&response).unwrap());
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| App::new().service(search))
        .bind(("localhost", 8080))?
        .run()
        .await
}
