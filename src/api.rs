use axum::{
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
use anyhow::{bail, Result};
use cloudevents::Event;
use jsonwebtoken::{
    decode,
    DecodingKey,
    errors::ErrorKind,
    Validation,
};
use tracing::{error, debug};
use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc2822};
use uuid::Uuid;
use std::{
    collections::HashMap,
    sync::Arc,
};
use crate::{
    db::{self, ExpectedRevision},
    server::{
        self,
        AppState,
        User,
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
    id: Uuid,
    title: String,
    detail: Option<String>,
    source: Option<ApiErrorSource>,
}

impl ApiError {
    fn into_document(self) -> ApiErrorDocument {
        ApiErrorDocument::with_error(self)
    }
}

#[derive(Debug, Serialize)]
struct ApiDataDocument<T> {
    data: ApiResource<T>,
}

#[derive(Debug, Serialize)]
struct ApiErrorDocument {
    errors: Option<Vec<ApiError>>,
}

impl ApiErrorDocument {
    fn with_error(error: ApiError) -> Self {
        Self {
            errors: Some(vec![error]),
        }
    }
}

#[derive(Debug, Serialize)]
struct ApiResource<T> {
    attributes: T,
}

impl<T> ApiResource<T> {
    fn into_document(self) -> ApiDataDocument<T> {
        ApiDataDocument {
            data: self,
        }
    }
}

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
}

async fn health(state: State<Arc<AppState>>) -> Response {
    let health = state.check_health();

    return (
        [(header::CACHE_CONTROL, "public, max-age=60")],
        Json::from(health),
    ).into_response();
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
            let error_id = Uuid::now_v7();
            debug!("error_id={} Request is missing Bearer token", error_id);
            let body = ApiError {
                id: error_id,
                title: "Not authenticated".to_string(),
                detail: Some("A Bearer token is required to access this API.".to_string()),
                source: Some(ApiErrorSource::header("Authorization")),
            }.into_document();

            let resp = (
                StatusCode::UNAUTHORIZED,
                [
                    (header::WWW_AUTHENTICATE, "Bearer realm=\"hematite\""),
                    (header::CACHE_CONTROL, "no-cache"),
                ],
                Json::from(body),
            ).into_response();

            return Err(resp);
        };

    match authorize_current_user(auth_token) {
        Ok(current_user) => {
            req.extensions_mut().insert(current_user);
            return Ok(next.run(req).await);
        },
        Err(err) => {
            let error_id = Uuid::now_v7();
            error!("error_id={} Error validating auth token: {:?}", error_id, err);

            let desc =
                if let Ok(jwt_error) = err.downcast::<jsonwebtoken::errors::Error>() {
                    let kind = jwt_error.kind();

                    debug!("Token validation failed with reason: {:?}", kind);
                    match kind {
                        ErrorKind::InvalidAudience => "token has an invalid audience",
                        ErrorKind::ExpiredSignature => "token has expired",
                        ErrorKind::InvalidSignature => "token has an invalid signature",
                        _ => "Bearer token is invalid"
                    }
                } else {
                    "Bearer token is invalid"
                };


            let body = ApiError {
                id: error_id,
                title: "Not authenticated".to_string(),
                detail: Some(desc.to_string()),
                source: Some(ApiErrorSource::header("Authorization")),
            }.into_document();

            let resp = (
                StatusCode::UNAUTHORIZED,
                [
                    (header::WWW_AUTHENTICATE, format!("Bearer realm=\"hematite\" error=\"invalid_token\" error_description=\"{}\"", desc)),
                    (header::CACHE_CONTROL, "no-cache".to_string()),
                ],
                Json::from(body),
            ).into_response();

            return Err(resp);
        }
    }
}

fn authorize_current_user(token: &str) -> Result<User> {
    let secret = std::env::var("HEMATITE_JWT_SECRET").expect("Env var HEMATITE_JWT_SECRET is required.");

    let mut validation = Validation::default();
    validation.set_audience(&["hematite"]);

    let token_result = decode::<Claims>(&token, &DecodingKey::from_secret(&secret.into_bytes()), &validation)?;

    if let Ok(user_id) = token_result.claims.sub.parse() {
        Ok(User{id: user_id})
    } else {
        bail!("User ID is not a valid UUID")
    }
}

async fn get_event(state: State<Arc<AppState>>, Extension(user): Extension<User>, Path((stream_id, rownum)): Path<(String, u64)>) -> Response {
    let event_result = state.get_event(&user.id, &stream_id, rownum);

    match event_result {
        Ok(Some(event)) => return ([(header::CACHE_CONTROL, "max-age=31536000, immutable")], Json(event)).into_response(),
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            match err.downcast::<server::Error>() {
                Ok(server::Error::StreamNotFound) => {
                    return StatusCode::NOT_FOUND.into_response();
                },
                Err(err) => {
                    let error_id = Uuid::now_v7();
                    error!("error_id={} user_id={} stream_id={} Error getting event: {:?}", error_id, user.id, stream_id, err);

                    let body = ApiError {
                        id: error_id,
                        title: "Internal server error".to_string(),
                        detail: None,
                        source: None,
                    }.into_document();

                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        [(header::CACHE_CONTROL, "no-cache")],
                        Json::from(body),
                    ).into_response();
                }
            }
        },
    }
}

async fn get_event_index(state: State<Arc<AppState>>, Extension(user): Extension<User>, Path(stream_id): Path<String>, Query(query): Query<HashMap<String, String>>) -> Response {
    let start = query.get("page[offset]").unwrap_or(&"0".to_string()).parse().unwrap_or(0).max(0);
    let limit = query.get("page[limit]").unwrap_or(&"50".to_string()).parse().unwrap_or(50).min(1000);

    let events_result = state.get_event_many(&user.id, &stream_id, start, limit);

    match events_result {
        Ok(events) => {
            let cache_header =
                if events.len() == limit {
                    (header::CACHE_CONTROL, "max-age=31536000, immutable")
                } else {
                    (header::CACHE_CONTROL, "no-cache")
                };

            return (
                [cache_header],
                Json(events),
            ).into_response();
        },
        Err(err) => {
            let error_id = Uuid::now_v7();
            error!("error_id={} user_id={} stream_id={} Error getting events: {:?}", error_id, user.id, stream_id, err);

            let body = ApiError {
                id: error_id,
                title: "Internal server error".to_string(),
                detail: None,
                source: None,
            }.into_document();

            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CACHE_CONTROL, "no-cache")],
                Json::from(body),
            ).into_response();
        },
    }
}

async fn get_stream(state: State<Arc<AppState>>, Extension(user): Extension<User>, Path(stream_id): Path<String>) -> Response {
    let get_result = state.get_stream(&user.id, &stream_id);

    match get_result {
        Ok(stream) => {
            let last_modified = OffsetDateTime::from_unix_timestamp(stream.last_modified).unwrap().format(&Rfc2822).unwrap();

            let body = ApiResource {
                attributes: Some(stream),
            }.into_document();


            return (
                StatusCode::OK,
                [
                    (header::CACHE_CONTROL, "no-cache"),
                    (header::LAST_MODIFIED, &last_modified),
                ],
                Json::from(body),
            ).into_response();
        }
        Err(err) => {
            match err.downcast::<server::Error>() {
                Ok(server::Error::StreamNotFound) => StatusCode::NOT_FOUND.into_response(),
                Err(err) => {
                    let error_id = Uuid::now_v7();
                    error!("error_id={} user_id={} stream_id={} Error getting stream: {:?}", error_id, user.id, stream_id, err);

                    let body = ApiError {
                        id: error_id,
                        title: "Internal server error".to_string(),
                        detail: None,
                        source: None,
                    }.into_document();

                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        [(header::CACHE_CONTROL, "no-cache")],
                        Json::from(body),
                    ).into_response()
                }
            }
        }
    }
}

async fn delete_stream(state: State<Arc<AppState>>, Extension(user): Extension<User>, Path(stream_id): Path<String>) -> Response {
    let delete_result = state.delete_stream(&user.id, &stream_id);

    match delete_result {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            let error_id = Uuid::now_v7();
            error!("error_id={} user_id={} stream_id={} Error deleting stream: {}", error_id, user.id, stream_id, err);

            let body = ApiError {
                id: error_id,
                title: "Internal server error".to_string(),
                detail: None,
                source: None,
            }.into_document();

            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CACHE_CONTROL, "no-cache")],
                Json::from(body),
            ).into_response();
        }
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
    Path(stream_id): Path<String>,
    Query(query_params): Query<PostEventParams>,
    Json(payload): Json<PostEventPayload>,
) -> Response {
    let revision = {
        let default_revision = "any".to_owned();
        let revision_param = query_params.expected_revision.unwrap_or(default_revision);
        let revision_result = parse_expected_revision(revision_param.as_str());

        if revision_result.is_err() {
            let error_id = Uuid::now_v7();
            debug!("error_id={} Failed to post event: {:?}", error_id, revision_result);
            let body = ApiError {
                id: error_id,
                title: "Invalid parameter".to_string(),
                detail: Some(format!("expected_revision is invalid. Got {:?}", revision_result)),
                source: Some(ApiErrorSource::query("expected_revision")),
            }.into_document();

            return (
                StatusCode::UNAUTHORIZED,
                [(header::CACHE_CONTROL, "no-cache")],
                Json::from(body),
            ).into_response();
        }

        revision_result.unwrap()
    };

    let result = match payload {
        PostEventPayload::Single(event) => state.insert_event(&user.id, &stream_id, event, revision),
        PostEventPayload::Batch(events) => state.insert_event_many(&user.id, &stream_id, events, revision),
    };

    match result {
        Ok(rownum) => {
            return (
                StatusCode::CREATED,
                [
                    (header::CACHE_CONTROL, "no-cache"),
                    (header::CONTENT_LOCATION, &format!("http://localhost:8080/streams/{}/events/{}", stream_id, rownum).to_string()),
                ],
            ).into_response();
        }
        Err(err) => {
            let error_id = Uuid::now_v7();
            debug!("error_id={} Failed to post event: {:?}", error_id, err);

            match err.downcast::<db::Error>() {
                Ok(db::Error::RevisionMismatch) => {
                    let body = ApiError {
                        id: error_id,
                        title: "Revision mismatch".to_string(),
                        detail: Some("expected revision did not match actual revision".to_string()),
                        source: Some(ApiErrorSource::query("expected_revision")),
                    }.into_document();

                    return (
                        StatusCode::CONFLICT,
                        [(header::CACHE_CONTROL, "no-cache")],
                        Json::from(body),
                    ).into_response();
                },
                Ok(db::Error::SourceIdConflict) => {
                    let body = ApiError {
                        id: error_id,
                        title: "Source/ID conflict".to_string(),
                        detail: Some("this stream already contains an event with that source and id field. According to the CloudEvents spec, those fields in combination must be unique".to_string()),
                        source: None,
                    }.into_document();

                    return (
                        StatusCode::CONFLICT,
                        [(header::CACHE_CONTROL, "no-cache")],
                        Json::from(body),
                    ).into_response();
                },
                Ok(db::Error::Stopped) => {
                    let body = ApiError {
                        id: error_id,
                        title: "Stopped stream".to_string(),
                        detail: Some("this stream is stopped and is not accepting requests".to_string()),
                        source: None,
                    }.into_document();

                    return (
                        StatusCode::CONFLICT,
                        [(header::CACHE_CONTROL, "no-cache")],
                        Json::from(body),
                    ).into_response();
                },
                Err(err) => {
                    error!("error_id={} Failed to post event: {:?}", error_id, err);
                    let body = ApiError {
                        id: error_id,
                        title: "Internal server error".to_string(),
                        detail: None,
                        source: None,
                    }.into_document();

                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        [(header::CACHE_CONTROL, "no-cache")],
                        Json::from(body),
                    ).into_response();
                }
            }
        }
    }
}

fn parse_expected_revision(expected_revision: &str) -> Result<ExpectedRevision> {
    match expected_revision {
        "any" => Ok(ExpectedRevision::Any),
        "no-stream" => Ok(ExpectedRevision::NoStream),
        "stream-exists" => Ok(ExpectedRevision::StreamExists),
        exact => {
            if let Ok(exact_revision) = exact.parse() {
                Ok(ExpectedRevision::Exact(exact_revision))
            } else {
                bail!("String was not a revision number or a recognized token")
            }
        }
    }
}
