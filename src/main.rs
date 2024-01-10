use axum::{Router, body::Body, routing::{get, delete}, routing::post, response::{Response, IntoResponse}, http::{StatusCode, header}, extract::{Extension, State, Path, Json, Request, Query}, middleware::{Next, self}};
use cloudevents::*;
use data_encoding::BASE32_NOPAD;
use hematite::db::{Database, ExpectedRevision};
use log::info;
use log4rs;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, fs, path::PathBuf, str, sync::{RwLock, Arc, Mutex}};
use jsonwebtoken::{decode, DecodingKey, Validation};

#[derive(Serialize)]
enum HealthStatus {
    Pass,
}

#[derive(Serialize)]
struct ApiHealth {
    status: HealthStatus,
}

type UserId = String;

#[derive(Clone, Debug)]
struct User {
    id: UserId,
}

type UserMap = HashMap<UserId, StreamMap>;
type StreamMap = HashMap<String, Mutex<Database>>;

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
                let user_id: UserId = user_path.file_stem().unwrap().to_str().unwrap().to_string();

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

    fn check_health(&self) -> ApiHealth {
        ApiHealth { status: HealthStatus::Pass }
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

            let db = Database::new(&path).expect("Failed to initialize database");

            let mut streams_mut = self.streams.write().unwrap();
            let stream_map = streams_mut.get_mut(user_id).unwrap();
            stream_map.insert(stream_id.to_owned(), Mutex::new(db));
        }
    }

    fn get_event(&self, user_id: &str, stream_id: &str, rownum: u64) -> anyhow::Result<Option<Event>> {
        let users = self.streams.read().unwrap();
        let user_streams = users.get(user_id).unwrap();
        let db_option = user_streams.get(stream_id);

        if let Some(db) = db_option {
            db.lock().unwrap().query(rownum)
        } else {
            Ok(None)
        }
    }

    fn get_event_many(&self, user_id: &str, stream_id: &str, start: u64, limit: u64) -> anyhow::Result<Vec<Event>> {
        let users = self.streams.read().unwrap();
        let user_streams = users.get(user_id).unwrap();
        let db_option = user_streams.get(stream_id);

        if let Some(db) = db_option {
            db.lock().unwrap().query_many(start, limit)
        } else {
            Ok(vec![])
        }
    }

    fn insert_event(&self, user_id: &str, stream_id: &str, event: Event, revision: ExpectedRevision) -> anyhow::Result<u64> {
        let users = self.streams.read().unwrap();
        let user_streams = users.get(user_id).unwrap();
        let db_option = user_streams.get(stream_id);

        if let Some(db) = db_option {
            db.lock().unwrap().insert(event, revision)
        } else {
            anyhow::bail!("stream not found")
        }
    }

    fn insert_event_many(&self, user_id: &str, stream_id: &str, events: Vec<Event>, revision: ExpectedRevision) -> anyhow::Result<u64> {
        let users = self.streams.read().unwrap();
        let user_streams = users.get(user_id).unwrap();
        let db_option = user_streams.get(stream_id);

        if let Some(db) = db_option {
            db.lock().unwrap().insert_batch(events, revision)
        } else {
            anyhow::bail!("stream not found")
        }
    }

    fn delete_stream(&self, user_id: &str, stream_id: &str) -> anyhow::Result<()> {
        let stream_exists = {
            let users = self.streams.read().unwrap();
            let user_streams = users.get(user_id).unwrap();
            let db_option = user_streams.get(stream_id);

            if let Some(db) = db_option {
                db.lock().unwrap().delete()?;
                true
            } else {
                false
            }
        };

        if stream_exists {
            let mut users = self.streams.write().unwrap();
            let user_streams = users.get_mut(user_id).unwrap();
            user_streams.remove(stream_id);
        }

        // Idempotency!
        Ok(())
    }
}

async fn get_event(state: State<Arc<AppState>>, Extension(user): Extension<User>, stream: Path<(String, u64)>) -> Response {
    let (stream_id, rownum) = stream.0;

    let event_result = state.get_event(&user.id, &stream_id, rownum);

    match event_result {
        Ok(Some(event)) => return Json(event).into_response(),
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn get_event_index(state: State<Arc<AppState>>, Extension(user): Extension<User>, stream:Path<String>, query: Query<HashMap<String, String>>) -> Response {
    let stream_id = stream.0;

    let start = query.0.get("start").unwrap_or(&"0".to_string()).parse().unwrap_or(0).max(0);
    let limit = query.0.get("limit").unwrap_or(&"50".to_string()).parse().unwrap_or(50).min(1000);

    let events_result = state.get_event_many(&user.id, &stream_id, start, limit);

    match events_result {
        Ok(events) => return Json(events).into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn delete_stream(state: State<Arc<AppState>>, Extension(user): Extension<User>, Path(stream_id): Path<String>) -> impl IntoResponse {
    let delete_result = state.delete_stream(&user.id, &stream_id);

    match delete_result {
        Ok(()) => return StatusCode::NO_CONTENT,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[derive(Debug, Default, Serialize)]
struct ApiErrorSource {
    header: Option<String>,
    query: Option<String>,
}

impl ApiErrorSource {
    fn header(name: &str) -> Self {
        Self {
            header: Some(name.to_string()),
            ..Default::default()
        }
    }

    fn query(name: &str) -> Self {
        Self {
            query: Some(name.to_string()),
            ..Default::default()
        }
    }
}

#[derive(Debug, Serialize)]
struct ApiError {
    title: String,
    detail: Option<String>,
    source: ApiErrorSource,
}

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
}

async fn auth(mut req: Request, next: Next) -> Result<Response, impl IntoResponse> {
    let auth_token = req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .and_then(|header| header.strip_prefix("Bearer "));

    let auth_token =
        if let Some(auth_token) = auth_token {
            auth_token
        } else {
            let body = ApiError {
                title: "Not authenticated".to_string(),
                detail: Some("A Bearer token is required to access this API.".to_string()),
                source: ApiErrorSource::header("Authorization"),
            };

            return Err((StatusCode::UNAUTHORIZED, Json::from(body)));
        };

    if let Some(current_user) = authorize_current_user(auth_token) {
        req.extensions_mut().insert(current_user);
        Ok(next.run(req).await)
    } else {
        let body = ApiError {
            title: "Not authenticated".to_string(),
            detail: Some("Bearer token is invalid.".to_string()),
            source: ApiErrorSource::header("Authorization"),
        };

        return Err((StatusCode::UNAUTHORIZED, Json::from(body)));
    }
}

fn authorize_current_user(token: &str) -> Option<User> {
    let secret = std::env::var("HEMATITE_JWT_SECRET").expect("Env var HEMATITE_JWT_SECRET is required.");

    let mut validation = Validation::default();
    validation.set_audience(&["hematite"]);

    let token_result = decode::<Claims>(&token, &DecodingKey::from_secret(&secret.into_bytes()), &validation);

    if let Ok(token) = token_result {
        return Some(User{id: token.claims.sub})
    } else {
        None
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
    Extension(user): Extension<User>,
    stream: Path<String>,
    query_params: Query<PostEventParams>,
    payload: Json<PostEventPayload>,
) -> impl IntoResponse {
    let revision = {
        let default_revision = "any".to_owned();
        let revision_param = query_params.expected_revision.clone().unwrap_or(default_revision);
        let revision_result = parse_expected_revision(revision_param.as_str());

        if revision_result.is_err() {
            let body = ApiError {
                title: "Invalid parameter".to_string(),
                detail: Some("expected_revision is invalid.".to_string()),
                source: ApiErrorSource::query("expected_revision"),
            };
            let mut resp = Json::from(body).into_response();
            *resp.status_mut() = StatusCode::UNAUTHORIZED;
            return resp
        }

        revision_result.unwrap()
    };

    let stream_id = stream.0;

    state.initialize_database(&user.id, &stream_id);

    let result = match payload.0 {
        PostEventPayload::Single(event) => state.insert_event(&user.id, &stream_id, event, revision),
        PostEventPayload::Batch(events) => state.insert_event_many(&user.id, &stream_id, events, revision),
    };

    match result {
        Ok(rownum) => {
            return Response::builder()
                .status(StatusCode::CREATED)
                .header(header::CONTENT_LOCATION, format!("http://localhost:8080/streams/{}/events/{}", stream_id, rownum))
                .body(Body::from(""))
                .unwrap();
        }
        Err(_err) => {
            let body = ApiError {
                title: "Internal server error".to_string(),
                detail: None,
                source: ApiErrorSource::query("expected_revision"),
            };
            let mut resp = Json::from(body).into_response();
            *resp.status_mut() = StatusCode::CONFLICT;
            return resp
        }
    }
}

async fn health(state: State<Arc<AppState>>) -> Response {
    let health = state.check_health();

    let mut resp = Json(health).into_response();
    let headers = resp.headers_mut();
    headers.insert(header::CACHE_CONTROL, "max-age=60".parse().unwrap());

    return resp
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
    headers.insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
    headers.insert(header::X_FRAME_OPTIONS, "DENY".parse().unwrap());
    headers.insert(header::X_XSS_PROTECTION, "1; mode=block".parse().unwrap());
    headers.insert(header::CONTENT_SECURITY_POLICY, "frame-ancestors 'none'".parse().unwrap());

    return response;
}

#[tokio::main]
async fn main() {
    log4rs::init_file("config/log4rs.yml", Default::default()).unwrap();

    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const STREAMS_DIR: &'static str = env!("HEMATITE_STREAMS_DIR");
    let streams_dir = PathBuf::from(STREAMS_DIR);
    fs::create_dir_all(&streams_dir).expect("Could not create stream database directory.");

    info!("Starting Hematite DB version {}", VERSION);
    info!("Stream database directory: {}", streams_dir.display());

    let state = Arc::new(AppState::new(streams_dir));

    let stream_routes = Router::new()
        .route("/:stream/events/:rownum", get(get_event))
        .route("/:stream/events", post(post_event).get(get_event_index))
        .route("/:stream", delete(delete_stream))
        .layer(middleware::from_fn(auth));

    let app = Router::new()
        .nest("/streams", stream_routes)
        .route("/health", get(health))
        .layer(middleware::from_fn(apply_secure_headers))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();

    axum::serve(listener, app).await.unwrap();
}
