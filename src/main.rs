use axum::{Router, response::Response, http::{header, StatusCode}, extract::Request, middleware::{Next, self}};
use hematite::{
    api,
    server::AppState,
};
use log::info;
use log4rs;
use std::{env, fs, path::PathBuf, str, sync::Arc};


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

    let app = Router::new()
        .nest("/streams", api::stream_routes())
        .nest("/health", api::health_routes())
        .layer(middleware::from_fn(apply_secure_headers))
        .fallback(fallback)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();

    axum::serve(listener, app).await.unwrap();
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

async fn fallback() -> StatusCode {
    StatusCode::NOT_FOUND
}
