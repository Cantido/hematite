use actix::prelude::*;
use cloudevents::*;
use hematite::db::{Database, DatabaseActor, ExpectedRevision, Append, Fetch};
use serde::Deserialize;
use std::{collections::HashMap, path::PathBuf};
use std::sync::RwLock;
use actix_web::{get, web, App, HttpRequest, HttpResponse, HttpServer, Responder, guard};

type DbActorAddress = actix::Addr<DatabaseActor>;

struct AppState {
    streams: RwLock<HashMap<String, DbActorAddress>>
}

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

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

#[derive(Deserialize, Debug)]
struct PostEventParams {
    expected_revision: Option<String>,
}

async fn post_event(req: HttpRequest, state: web::Data<AppState>, stream: web::Path<String>, event: web::Json<Event>, query_params: web::Query<PostEventParams>) -> HttpResponse {
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
        let revision_param = query_params.into_inner().expected_revision.unwrap_or("any".to_string());
        let revision = match revision_param.as_str() {
            "any" => ExpectedRevision::Any,
            "no-stream" => ExpectedRevision::NoStream,
            "stream-exists" => ExpectedRevision::StreamExists,
            exact => {
                if let Ok(exact_revision) = exact.parse() {
                    ExpectedRevision::Exact(exact_revision)
                } else {
                    return HttpResponse::UnprocessableEntity().finish()
                }
            }
        };
        match addr.send(Append(event.into_inner(), revision)).await {
            Ok(Ok(rownum)) => {
                let event_url = req.url_for("stream_events_rownum", [stream_id, rownum.to_string()]).unwrap().to_string();
                return HttpResponse::Created().insert_header(("location", event_url)).finish()
            },
            Ok(Err(err)) => return HttpResponse::Conflict().body(err.to_string()),
            Err(_) => return HttpResponse::InternalServerError().finish()
        }
    } else {
        return HttpResponse::InternalServerError().finish()
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
            .service(
                web::scope("/streams/{stream}")
                    .service(
                        web::resource("/events")
                            .name("stream_events")
                            .guard(guard::Header("content-type", "application/json"))
                            .route(web::post().to(post_event))
                    )
                    .service(
                        web::resource("/events/{rownum}")
                            .name("stream_events_rownum")
                            .route(web::get().to(get_event))
                    )
            )
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
