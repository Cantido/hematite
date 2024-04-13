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
use axum_macros::debug_handler;
use cloudevents::Event;
use jsonwebtoken::errors::ErrorKind;
use tracing::{error, debug};
use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc2822};
use url::Url;
use uuid::Uuid;
use std::{
    collections::HashMap,
    sync::Arc, path::PathBuf,
};
use crate::{
    db::{self, ExpectedRevision},
    server::{
        self,
        AppState,
        User,
    }, openid::OpenIdClient
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
struct ApiDataCollectionDocument<T> {
    data: Vec<ApiResource<T>>,
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
    id: String,
    #[serde(rename = "type")]
    resource_type: String,
    attributes: T,
}

impl<T> ApiResource<T> {
    fn new(id: String, resource_type: String, attributes: T) -> Self {
        Self { id, resource_type, attributes }
    }

    fn into_document(self) -> ApiDataDocument<T> {
        ApiDataDocument {
            data: self,
        }
    }
}

async fn health(state: State<Arc<AppState>>) -> Response {
    let health = state.check_health();

    return (
        [(header::CACHE_CONTROL, "public, max-age=60")],
        Json::from(health),
    ).into_response();
}

#[tracing::instrument]
pub async fn stream_routes(streams_dir: PathBuf, oidc_url: Url) -> Result<Router<()>> {
    let state = Arc::new(AppState::new(streams_dir).await?);

    let oidc_client = Arc::new(OpenIdClient::new(oidc_url));

    oidc_client.refresh().await?;

    let router = Router::new()
        .route("/streams", get(get_streams))
        .route("/streams/:stream/events/:rownum", get(get_event))
        .route("/streams/:stream/events", post(post_event).get(get_event_index))
        .route("/streams/:stream", get(get_stream).delete(delete_stream))
        .route("/health", get(health))
        .layer(middleware::from_fn_with_state(oidc_client, auth))
        .with_state(state);

    Ok(router)
}

#[tracing::instrument]
async fn auth(oidc: State<Arc<OpenIdClient>>, mut req: Request, next: Next) -> Result<Response, Response> {
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

    match oidc.authorize_current_user(auth_token).await {
        Ok(claims) => {
            let current_user = User { id: claims.sub };

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

#[tracing::instrument]
#[debug_handler]
async fn get_event(state: State<Arc<AppState>>, Extension(user): Extension<User>, Path((stream_id, rownum)): Path<(String, u64)>) -> Response {
    let event_result = state.get_event(&user.id, &stream_id, rownum).await;

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

#[tracing::instrument]
#[debug_handler]
async fn get_event_index(state: State<Arc<AppState>>, Extension(user): Extension<User>, Path(stream_id): Path<String>, Query(query): Query<HashMap<String, String>>) -> Response {
    let start = query.get("page[offset]").unwrap_or(&"0".to_string()).parse().unwrap_or(0).max(0);
    let limit = query.get("page[limit]").unwrap_or(&"50".to_string()).parse().unwrap_or(50).min(1000);

    let events_result = state.get_event_many(&user.id, &stream_id, start, limit).await;

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

#[tracing::instrument]
#[debug_handler]
async fn get_streams(
    state: State<Arc<AppState>>,
    Extension(user): Extension<User>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let get_result = state.streams(&user.id).await;

    match get_result {
        Ok(mut streams) => {
            let default_sort_field = "id".to_string();
            let sort_field: &str = query.get("sort").unwrap_or(&default_sort_field);

            match sort_field {
                "id" => streams.sort_by(|a, b| a.id.cmp(&b.id)),
                "usage" => streams.sort_by(|a, b| a.usage.cmp(&b.usage)),
                "-usage" => streams.sort_by(|a, b| b.usage.cmp(&a.usage)),
                "revision" => streams.sort_by(|a, b| a.revision.cmp(&b.revision)),
                "-revision" => streams.sort_by(|a, b| b.revision.cmp(&a.revision)),
                "last_modified" => streams.sort_by(|a, b| a.last_modified.cmp(&b.last_modified)),
                "-last_modified" => streams.sort_by(|a, b| b.last_modified.cmp(&a.last_modified)),
                _ => {
                    return StatusCode::BAD_REQUEST.into_response();
                },
            };

            let mut stream_resources = vec![];
            for stream in streams.into_iter() {
                stream_resources.push(ApiResource::new(stream.id.to_string(), "streams".to_string(), stream));
            }

            let doc = ApiDataCollectionDocument { data: stream_resources };

            return Json::from(doc).into_response();
        }
        Err(err) => {
            let error_id = Uuid::now_v7();
            error!("error_id={} user_id={} Error getting streams: {}", error_id, user.id, err);

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

#[tracing::instrument]
#[debug_handler]
async fn get_stream(state: State<Arc<AppState>>, Extension(user): Extension<User>, Path(stream_id): Path<String>) -> Response {
    let get_result = state.get_stream(&user.id, &stream_id).await;

    match get_result {
        Ok(stream) => {
            let last_modified = OffsetDateTime::from_unix_timestamp(stream.last_modified.try_into().expect("Expected app to be running after epoch")).unwrap().format(&Rfc2822).unwrap();

            let body = ApiResource {
                id: stream_id,
                resource_type: "streams".to_string(),
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

#[tracing::instrument]
#[debug_handler]
async fn delete_stream(state: State<Arc<AppState>>, Extension(user): Extension<User>, Path(stream_id): Path<String>) -> Response {
    let delete_result = state.delete_stream(&user.id, &stream_id).await;

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

#[tracing::instrument]
#[debug_handler]
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
        PostEventPayload::Single(event) => state.insert_event(&user.id, &stream_id, event, revision).await,
        PostEventPayload::Batch(events) => state.insert_event_many(&user.id, &stream_id, events, revision).await,
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
