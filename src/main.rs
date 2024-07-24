use anyhow::Context;
use axum::{response::Response, http::{header, StatusCode}, extract::Request, middleware::{Next, self}};
use hematite::api;
use tracing::info;
use tracing_subscriber::{prelude::*, filter::EnvFilter, fmt, Registry};
use url::Url;
use std::{env, fs, path::PathBuf};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter_layer = EnvFilter::from_default_env();

    let subscriber = Registry::default()
        .with(filter_layer)
        .with(fmt::layer());
    tracing::subscriber::set_global_default(subscriber)?;

    let streams_dir = env::var("HEMATITE_STREAMS_DIR").expect("Env var HEMATITE_STREAMS_DIR is required");
    let streams_dir = PathBuf::from(streams_dir);
    fs::create_dir_all(&streams_dir).expect("Could not create stream database directory.");

    let oidc_url: Url =
        env::var("HEMATITE_OIDC_URL")
        .with_context(|| "Env var HEMATITE_OIDC_URL is missing.")?
        .parse()
        .with_context(|| "Failed to parse HEMATITE_OIDC_URL as a URL")?;

    info!("Starting Hematite DB version: {}", hematite::build::VERSION);
    info!("Stream database directory: {}", streams_dir.display());

    let app = api::stream_routes(streams_dir, oidc_url).await?
        .layer(middleware::from_fn(apply_secure_headers))
        .fallback(fallback);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;

    axum::serve(listener, app).await?;

    Ok(())
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
