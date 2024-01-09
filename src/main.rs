use actix::Actor;
use axum::{async_trait, Router, routing::get, routing::post, response::{Response, IntoResponse}, http::{StatusCode, request::Parts, header::{AUTHORIZATION, CACHE_CONTROL, X_CONTENT_TYPE_OPTIONS, X_FRAME_OPTIONS, X_XSS_PROTECTION, CONTENT_SECURITY_POLICY, CONTENT_LOCATION}}, extract::{FromRequestParts, State, Path, Json, Request, Query}, middleware::{Next, self}};
use cloudevents::*;
use data_encoding::BASE32_NOPAD;
use hematite::db::{Append, AppendBatch, Database, DatabaseActor, ExpectedRevision, Fetch, FetchMany};
use log::info;
use log4rs;
use serde::Deserialize;
use std::{collections::HashMap, env, fs, path::PathBuf, str, sync::{RwLock, Arc}};
use jsonwebtoken::{decode, DecodingKey, Validation};

type DbActorAddress = actix::Addr<DatabaseActor>;

type UserMap = HashMap<String, StreamMap>;
type StreamMap = HashMap<String, DbActorAddress>;

struct AppState {
    streams_path: PathBuf,
    streams: RwLock<UserMap>,
}

impl AppState {
    fn new(streams_path: PathBuf) -> Self {
        let state = AppState {
            streams_path,
            streams: RwLock::new(HashMap::new()),
        };

        for user_dir_result in state
            .streams_path
            .read_dir()
            .expect("Couldn't read stream directory")
        {
            if let Ok(user_dir) = user_dir_result {
                let user_path = user_dir.path();
                let user_id = user_path.file_stem().unwrap().to_str().unwrap();

                for db_file_result in user_dir
                    .path()
                    .read_dir()
                    .expect("Couldn't read user directory")
                {
                    if let Ok(db_file) = db_file_result {
                        let filepath = db_file.path();
                        let encoded_stream_id = filepath.file_stem().unwrap().to_str();
                        let stream_id_bytes = BASE32_NOPAD
                            .decode(encoded_stream_id.unwrap().as_bytes())
                            .expect("Expected file in stream dir to have a Base32 no-pad encoded filename");
                        let stream_id = str::from_utf8(stream_id_bytes.as_slice()).unwrap();

                        info!("Initializing stream {}", &stream_id);

                        state.initialize_database(&user_id, &stream_id);
                    }
                }
            }
        }

        state
    }

    fn initialize_database(&self, user_id: &str, stream_id: &str) {
        let init_user = {
            let users = self.streams.read().unwrap();

            !users.contains_key(user_id)
        };

        if init_user {
            let mut path = self.streams_path.clone();
            path.push(user_id);
            fs::create_dir_all(path).expect("Could not create user directory");

            let mut users = self.streams.write().unwrap();
            users.insert(user_id.to_string(), HashMap::new());
        }

        let init_db = {
            let users = self.streams.read().unwrap();
            let streams = users.get(user_id).unwrap();

            !streams.contains_key(stream_id)
        };

        if init_db {
            let stream_file_name: String = BASE32_NOPAD.encode(stream_id.as_bytes());

            let mut path = self.streams_path.clone();
            path.push(user_id);
            path.push(stream_file_name);
            path.set_extension("hemadb");

            let db = Database::new(&path).unwrap();
            let addr = DatabaseActor { database: db }.start();

            let mut streams_mut = self.streams.write().unwrap();
            let stream_map = streams_mut.get_mut(user_id).unwrap();
            stream_map.insert(stream_id.to_owned(), addr);
        }
    }

    fn get_stream_address(&self, user_id: &str, stream_id: &str) -> Option<DbActorAddress> {
        let users = self.streams.read().unwrap();
        let user_streams = users.get(user_id)?;
        let addr = user_streams.get(stream_id);

        addr.map(|a| a.to_owned())
    }
}

async fn get_event(state: State<Arc<AppState>>, claims: Claims, stream: Path<(String, u64)>) -> Response {
    let user_id = claims.sub;
    let (stream_id, rownum) = stream.0;

    let addr_option = state.get_stream_address(&user_id, &stream_id);

    if let Some(addr) = addr_option {
        match addr.send(Fetch(rownum)).await {
            Ok(Ok(Some(event))) => return Json(event).into_response(),
            Ok(Ok(None)) => return StatusCode::NOT_FOUND.into_response(),
            Ok(Err(_)) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    } else {
        return StatusCode::NOT_FOUND.into_response();
    }
}

async fn get_event_index(state: State<Arc<AppState>>, claims: Claims, stream:Path<String>, query: Query<HashMap<String, String>>) -> Response {
    let user_id = claims.sub;
    let stream_id = stream.0;

    let start = query.0.get("start").unwrap_or(&"0".to_string()).parse().unwrap_or(0).min(0);
    let limit = query.0.get("start").unwrap_or(&"0".to_string()).parse().unwrap_or(50).max(1000);

    let addr_option = state.get_stream_address(&user_id, &stream_id);

    if let Some(addr) = addr_option {
        match addr.send(FetchMany(start, limit)).await {
            Ok(Ok(events)) => return Json(events).into_response(),
            Ok(Err(_)) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    } else {
        return StatusCode::NOT_FOUND.into_response();
    }
}

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
}

#[async_trait]
impl<S> FromRequestParts<S> for Claims
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        if let Some(auth_header) = parts.headers.get(AUTHORIZATION) {
            let jwt_result = auth_header.to_str().unwrap().strip_prefix("Bearer ");

            if jwt_result.is_some() {
                let jwt = jwt_result.unwrap();
                let secret = std::env::var("HEMATITE_JWT_SECRET").expect("Env var HEMATITE_JWT_SECRET is required.");

                let mut validation = Validation::default();
                validation.set_audience(&["hematite"]);

                let token_result = decode::<Claims>(&jwt, &DecodingKey::from_secret(&secret.into_bytes()), &validation);

                if let Ok(token) = token_result {
                    return Ok(token.claims)
                }
            }
        }
        return Err((StatusCode::UNAUTHORIZED, "Bearer token is missing or invalid"))
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
    state: State<Arc<AppState>>,
    claims: Claims,
    stream: Path<String>,
    query_params: Query<PostEventParams>,
    payload: Json<PostEventPayload>,
) -> Response {
    let revision = {
        let default_revision = "any".to_owned();
        let revision_param = query_params.expected_revision.clone().unwrap_or(default_revision);
        let revision_result = parse_expected_revision(revision_param.as_str());

        if revision_result.is_err() {
            return StatusCode::UNPROCESSABLE_ENTITY.into_response();
        }

        revision_result.unwrap()
    };

    let user_id = claims.sub;
    let stream_id = stream.0;

    state.initialize_database(&user_id, &stream_id);

    let addr = state.get_stream_address(&user_id, &stream_id).unwrap();

    let result = match payload.0 {
        PostEventPayload::Single(event) => addr.send(Append(event, revision)).await,
        PostEventPayload::Batch(events) => addr.send(AppendBatch(events, revision)).await,
    };

    match result {
        Ok(Ok(rownum)) => {
            let event_url = format!("http://localhost:8080/streams/{}/events/{}", stream_id, rownum);
            let mut resp = StatusCode::CREATED.into_response();
            let headers = resp.headers_mut();
            headers.insert(CONTENT_LOCATION, event_url.parse().unwrap());
            return resp;
        }
        Ok(Err(_err)) => return StatusCode::CONFLICT.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
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

async fn apply_secure_headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;

    let headers = response.headers_mut();
    headers.insert(CACHE_CONTROL, "no-store".parse().unwrap());
    headers.insert(X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
    headers.insert(X_FRAME_OPTIONS, "DENY".parse().unwrap());
    headers.insert(X_XSS_PROTECTION, "1; mode=block".parse().unwrap());
    headers.insert(CONTENT_SECURITY_POLICY, "frame-ancestors 'none'".parse().unwrap());

    return response;
}

#[actix::main]
async fn main() {
    log4rs::init_file("config/log4rs.yml", Default::default()).unwrap();

    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const STREAMS_DIR: &'static str = env!("HEMATITE_STREAMS_DIR");
    let streams_dir = PathBuf::from(STREAMS_DIR);
    fs::create_dir_all(&streams_dir).expect("Could not create stream database directory.");

    info!("Starting Hematite DB version {}", VERSION);
    info!("Stream database directory: {}", streams_dir.display());

    let state = Arc::new(AppState::new(streams_dir));

    let app = Router::new()
        .route("/streams/:stream/events/:rownum", get(get_event))
        .route("/streams/:stream/events", post(post_event).get(get_event_index))
        .layer(middleware::from_fn(apply_secure_headers))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();

    axum::serve(listener, app).await.unwrap();
}
