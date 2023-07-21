use actix::prelude::*;
use actix_web::{get, guard, web, App, HttpRequest, HttpResponse, HttpServer, Responder, HttpResponseBuilder, error};
use cloudevents::*;
use data_encoding::BASE32_NOPAD;
use hematite::db::{Append, AppendBatch, Database, DatabaseActor, ExpectedRevision, Fetch};
use log::info;
use log4rs;
use serde::Deserialize;
use std::{collections::HashMap, env, fs, path::PathBuf, str, sync::RwLock};

type DbActorAddress = actix::Addr<DatabaseActor>;

struct AppState {
    streams_path: PathBuf,
    streams: RwLock<HashMap<String, DbActorAddress>>,
}

impl AppState {
    fn new(streams_path: PathBuf) -> Self {
        let state = AppState {
            streams_path,
            streams: RwLock::new(HashMap::new()),
        };

        for entry in state
            .streams_path
            .read_dir()
            .expect("Couldn't read stream directory")
        {
            if let Ok(file) = entry {
                let filepath = file.path();
                let encoded_stream_id = filepath.file_stem().unwrap().to_str();
                let stream_id_bytes = BASE32_NOPAD
                    .decode(encoded_stream_id.unwrap().as_bytes())
                    .expect("Expected file in stream dir to have a Base32 no-pad encoded filename");
                let stream_id = str::from_utf8(stream_id_bytes.as_slice()).unwrap();

                info!("Initializing stream {}", &stream_id);

                state.initialize_database(&stream_id);
            }
        }

        state
    }

    fn initialize_database(&self, stream_id: &str) {
        let init_db = {
            let streams = self.streams.read().unwrap();

            !streams.contains_key(stream_id)
        };

        if init_db {
            let stream_file_name: String = BASE32_NOPAD.encode(stream_id.as_bytes());

            let mut path = self.streams_path.clone();
            path.push(stream_file_name);
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
            Ok(Ok(Some(event))) => return apply_secure_headers(&mut HttpResponse::Ok()).json(event),
            Ok(Ok(None)) => return apply_secure_headers(&mut HttpResponse::NotFound()).json("Not Found"),
            Ok(Err(_)) => return apply_secure_headers(&mut HttpResponse::InternalServerError()).json("Internal Server Error"),
            Err(_) => return apply_secure_headers(&mut HttpResponse::InternalServerError()).json("Internal Server Error"),
        }
    } else {
        return apply_secure_headers(&mut HttpResponse::NotFound()).json("Not Found");
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
            return apply_secure_headers(&mut HttpResponse::UnprocessableEntity()).finish();
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
            return apply_secure_headers(&mut HttpResponse::Created())
                .insert_header(("location", event_url))
                .finish();
        }
        Ok(Err(err)) => return apply_secure_headers(&mut HttpResponse::Conflict()).body(err.to_string()),
        Err(_) => return apply_secure_headers(&mut HttpResponse::InternalServerError()).finish(),
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

fn apply_secure_headers(http_builder: &mut HttpResponseBuilder) -> &mut HttpResponseBuilder {
    http_builder
        .insert_header(("Cache-Control", "no-store"))
        .insert_header(("X-Content-Type-Options", "nosniff"))
        .insert_header(("X-Frame-Options", "DENY"))
        .insert_header(("X-XSS-Protection", "1; mode=block"))
        .insert_header(("Content-Security-Policy", "frame-ancestors 'none'"))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    log4rs::init_file("config/log4rs.yml", Default::default()).unwrap();

    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const STREAMS_DIR: &'static str = env!("HEMATITE_STREAMS_DIR");
    let streams_dir = PathBuf::from(STREAMS_DIR);
    fs::create_dir_all(&streams_dir).expect("Could not create stream database directory.");

    info!("Starting Hematite DB version {}", VERSION);
    info!("Stream database directory: {}", streams_dir.display());

    let state = web::Data::new(AppState::new(streams_dir));
    HttpServer::new(move || {
        let json_config = web::JsonConfig::default()
            .error_handler(|err, _req| {
                error::InternalError::from_response(err, HttpResponse::UnprocessableEntity().finish()).into()
            });
        App::new().app_data(state.clone()).app_data(json_config).service(hello).service(
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
