use axum::{
    body::Body,
    Extension,
    extract::{
        Json,
        Path,
        Query,
        Request,
        State,
    },
    http::{header, StatusCode},
    middleware::{self, Next},
    Router,
    routing::{get, post},
    response::{
        IntoResponse,
        Response,
    }
};
use cloudevents::Event;
use jsonwebtoken::{
    decode,
    DecodingKey,
    Validation,
};
use log::error;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::Arc,
};
use crate::{
    db::ExpectedRevision,
    server::{
        AppState,
        User, Error,
    }
};

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

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ApiDocument<T> {
    DataDocument {
        data: Option<ApiResource<T>>,
    },
    ErrorDocument {
        errors: Option<Vec<ApiError>>,
    }
}

#[derive(Debug, Serialize)]
struct ApiResource<T> {
    attributes: T,
}

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
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

async fn health(state: State<Arc<AppState>>) -> Response {
    let health = state.check_health();

    let mut resp = Json(health).into_response();
    let headers = resp.headers_mut();
    headers.insert(header::CACHE_CONTROL, "max-age=60".parse().unwrap());

    return resp
}

pub fn health_routes() -> Router<Arc<AppState>> {
    Router::new().route("/", get(health))
}


pub fn stream_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/:stream/events/:rownum", get(get_event))
        .route("/:stream/events", post(post_event).get(get_event_index))
        .route("/:stream", get(get_stream).delete(delete_stream))
        .layer(middleware::from_fn(auth))
}

async fn auth(mut req: Request, next: Next) -> Result<Response, Response> {
    let auth_token = req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .and_then(|header| header.strip_prefix("Bearer "));

    let auth_token =
        if let Some(auth_token) = auth_token {
            auth_token
        } else {
            let body: ApiDocument<()> = ApiDocument::ErrorDocument {
                errors: Some(vec![ApiError {
                    title: "Not authenticated".to_string(),
                    detail: Some("A Bearer token is required to access this API.".to_string()),
                    source: ApiErrorSource::header("Authorization"),
                }])
            };

            return Err((StatusCode::UNAUTHORIZED, Json::from(body)).into_response());
        };

    if let Some(current_user) = authorize_current_user(auth_token) {
        req.extensions_mut().insert(current_user);
        Ok(next.run(req).await)
    } else {
        let body: ApiDocument<()> = ApiDocument::ErrorDocument {
            errors: Some(vec![ApiError {
                title: "Not authenticated".to_string(),
                detail: Some("Bearer token is invalid.".to_string()),
                source: ApiErrorSource::header("Authorization"),
            }])
        };

        return Err((StatusCode::UNAUTHORIZED, Json::from(body)).into_response());
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

async fn get_stream(state: State<Arc<AppState>>, Extension(user): Extension<User>, Path(stream_id): Path<String>) -> Response {
    let get_result = state.get_stream(&user.id, &stream_id);

    match get_result {
        Ok(stream) => {
            let body = ApiDocument::DataDocument {
                data: Some(ApiResource {
                    attributes: Some(stream),
                }),
            };

            let resp = Json::from(body);
            (StatusCode::OK, resp).into_response()
        }
        Err(err) => {
            match err.downcast::<Error>() {
                Ok(Error::UserNotFound) => StatusCode::NOT_FOUND.into_response(),
                Ok(Error::StreamNotFound) => StatusCode::NOT_FOUND.into_response(),
                _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }
    }
}

async fn delete_stream(state: State<Arc<AppState>>, Extension(user): Extension<User>, Path(stream_id): Path<String>) -> impl IntoResponse {
    let delete_result = state.delete_stream(&user.id, &stream_id);

    match delete_result {
        Ok(true) => StatusCode::NO_CONTENT,
        Ok(false) => StatusCode::NOT_FOUND,
        Err(err) => {
            error!("user_id={} stream_id={} Error deleting stream: {}", user.id, stream_id, err);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
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
