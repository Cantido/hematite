use actix::prelude::*;
use cloudevents::*;
use hematite::db::{Database, DatabaseActor, ExpectedRevision, Append, Fetch};
use std::{collections::HashMap, path::PathBuf};
use std::sync::RwLock;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};

type DbActorAddress = actix::Addr<DatabaseActor>;

struct AppState {
    streams: RwLock<HashMap<String, DbActorAddress>>
}

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

#[get("/streams/{stream}/events/{rownum}")]
async fn get_event(state: web::Data<AppState>, stream: web::Path<(String, u64)>) -> impl Responder {
    let (stream_id, rownum) = stream.into_inner();

    let init_db = {
        let streams = state.streams.read().unwrap();

        !streams.contains_key(&stream_id)
    };

    if init_db {
        let db = Database::new(&PathBuf::from(format!("{}.db", stream_id))).unwrap();
        let addr = DatabaseActor { database: db }.start();

        let mut streams_mut = state.streams.write().unwrap();
        streams_mut.insert(stream_id.clone(), addr);
    }

    let streams = state.streams.read().unwrap();

    if let Some(addr) = streams.get(&stream_id) {
        match addr.send(Fetch(rownum)).await {
            Ok(Ok(Some(event))) => return HttpResponse::Ok().json(event),
            Ok(Ok(None)) => return HttpResponse::NotFound().json("Not Found"),
            Ok(Err(_)) => return HttpResponse::InternalServerError().json("Internal Server Error"),
            Err(_) => return HttpResponse::InternalServerError().json("Internal Server Error")
        }
    } else {
        return HttpResponse::NotFound().json("Not Found")
    }
}

#[post("/streams/{stream}/events")]
async fn post_event(state: web::Data<AppState>, stream: web::Path<String>, event: web::Json<Event>) -> impl Responder {
    let stream_id = stream.into_inner();

    let init_db = {
        let streams = state.streams.read().unwrap();

        !streams.contains_key(&stream_id)
    };

    if init_db {
        let db = Database::new(&PathBuf::from(format!("{}.db", stream_id))).unwrap();
        let addr = DatabaseActor { database: db }.start();

        let mut streams_mut = state.streams.write().unwrap();
        streams_mut.insert(stream_id.clone(), addr);
    }

    let streams = state.streams.read().unwrap();

    if let Some(addr) = streams.get(&stream_id) {
        match addr.send(Append(event.into_inner(), ExpectedRevision::Any)).await {
            Ok(Ok(())) => return HttpResponse::Created(),
            Ok(Err(_)) => return HttpResponse::InternalServerError(),
            Err(_) => return HttpResponse::InternalServerError()
        }
    } else {
        return HttpResponse::InternalServerError()
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let state = web::Data::new(AppState {
        streams: RwLock::new(HashMap::new())
    });
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .service(hello)
            .service(get_event)
            .service(post_event)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
