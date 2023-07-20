use actix::prelude::*;
use actix_web::{get, guard, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use cloudevents::*;
use data_encoding::BASE32_NOPAD;
use hematite::db::{Append, AppendBatch, Database, DatabaseActor, ExpectedRevision, Fetch};
use log::info;
use log4rs;
use serde::Deserialize;
use std::sync::RwLock;
use std::{collections::HashMap, env, path::PathBuf};

type DbActorAddress = actix::Addr<DatabaseActor>;

struct AppState {
    streams_path: PathBuf,
    streams: RwLock<HashMap<String, DbActorAddress>>,
}

impl AppState {
    fn new(streams_path: PathBuf) -> Self {
        AppState {
            streams_path,
            streams: RwLock::new(HashMap::new()),
        }
    }

    fn initialize_database(&self, stream_id: &str) {
        let init_db = {
            let streams = self.streams.read().unwrap();

            !streams.contains_key(stream_id)
        };

        if init_db {
            let stream_file_name: String = BASE32_NOPAD.encode(stream_id.as_bytes());

            let mut path = self.streams_path.clone();
            path.set_file_name(stream_file_name);
            path.set_extension("hemadb");

            let db = Database::new(&path).unwrap();
            let addr = DatabaseActor { database: db }.start();

            let mut streams_mut = self.streams.write().unwrap();
            streams_mut.insert(stream_id.to_owned(), addr);
        }
    }

    fn get_stream_address(&self, stream_id: &str) -> Option<DbActorAddress> {
        let streams = self.streams.read().unwrap();
        let addr = streams.get(stream_id);

        addr.map(|a| a.to_owned())
    }
}

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

async fn get_event(state: web::Data<AppState>, stream: web::Path<(String, u64)>) -> impl Responder {
    let (stream_id, rownum) = stream.into_inner();

    let addr_option = state.get_stream_address(&stream_id);

    if let Some(addr) = addr_option {
        match addr.send(Fetch(rownum)).await {
            Ok(Ok(Some(event))) => return HttpResponse::Ok().json(event),
            Ok(Ok(None)) => return HttpResponse::NotFound().json("Not Found"),
            Ok(Err(_)) => return HttpResponse::InternalServerError().json("Internal Server Error"),
            Err(_) => return HttpResponse::InternalServerError().json("Internal Server Error"),
        }
    } else {
        return HttpResponse::NotFound().json("Not Found");
    }
}

#[derive(Deserialize, Debug)]
struct PostEventParams {
    expected_revision: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum PostEventPayload {
    Single(Event),
    Batch(Vec<Event>),
}

async fn post_event(
    req: HttpRequest,
    state: web::Data<AppState>,
    stream: web::Path<String>,
    payload: web::Json<PostEventPayload>,
    query_params: web::Query<PostEventParams>,
) -> HttpResponse {
    let revision = {
        let revision_param = query_params
            .into_inner()
            .expected_revision
            .unwrap_or("any".to_string());
        let revision_result = parse_expected_revision(revision_param.as_str());

        if revision_result.is_err() {
            return HttpResponse::UnprocessableEntity().finish();
        }

        revision_result.unwrap()
    };

    let stream_id = stream.into_inner();

    state.initialize_database(&stream_id);

    let addr = state.get_stream_address(&stream_id).unwrap();

    let result = match payload.into_inner() {
        PostEventPayload::Single(event) => addr.send(Append(event, revision)).await,
        PostEventPayload::Batch(events) => addr.send(AppendBatch(events, revision)).await,
    };

    match result {
        Ok(Ok(rownum)) => {
            let event_url = req
                .url_for("stream_events_rownum", [stream_id, rownum.to_string()])
                .unwrap()
                .to_string();
            return HttpResponse::Created()
                .insert_header(("location", event_url))
                .finish();
        }
        Ok(Err(err)) => return HttpResponse::Conflict().body(err.to_string()),
        Err(_) => return HttpResponse::InternalServerError().finish(),
    }
}

fn parse_expected_revision(expected_revision: &str) -> Result<ExpectedRevision, String> {
    match expected_revision {
        "any" => Ok(ExpectedRevision::Any),
        "no-stream" => Ok(ExpectedRevision::NoStream),
        "stream-exists" => Ok(ExpectedRevision::StreamExists),
        exact => {
            if let Ok(exact_revision) = exact.parse() {
                Ok(ExpectedRevision::Exact(exact_revision))
            } else {
                Err("String was not a revision number or a recognized token".to_string())
            }
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const STREAMS_DIR: &'static str = env!("HEMATITE_STREAMS_DIR");
    let streams_dir = PathBuf::from(STREAMS_DIR);

    log4rs::init_file("config/log4rs.yml", Default::default()).unwrap();

    info!("Starting Hematite DB version {}", VERSION);
    info!("Stream database directory: {}", streams_dir.display());

    let state = web::Data::new(AppState::new(streams_dir));
    HttpServer::new(move || {
        App::new().app_data(state.clone()).service(hello).service(
            web::scope("/streams/{stream}")
                .service(
                    web::resource("/events")
                        .name("stream_events")
                        .guard(guard::Header("content-type", "application/json"))
                        .route(web::post().to(post_event)),
                )
                .service(
                    web::resource("/events/{rownum}")
                        .name("stream_events_rownum")
                        .route(web::get().to(get_event)),
                ),
        )
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
