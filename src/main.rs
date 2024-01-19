use axum::{Router, response::Response, http::{header, StatusCode}, extract::Request, middleware::{Next, self}};
use hematite::{
    api,
    server::AppState,
};
use tracing::info;
use log4rs;
use tracing_subscriber::{prelude::*, filter::EnvFilter, fmt, Registry};
use std::{env, fs, path::PathBuf, sync::Arc};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    log4rs::init_file("config/log4rs.yml", Default::default()).unwrap();

    let otlp_exporter = opentelemetry_otlp::new_exporter().tonic();

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(otlp_exporter)
        .install_simple()?;

    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))?;

    let subscriber =
        Registry::default()
            .with(filter_layer)
            .with(telemetry)
            .with(fmt::layer());

    tracing::subscriber::set_global_default(subscriber)?;

    let version = env::var("CARGO_PKG_VERSION").unwrap();
    let streams_dir = env::var("HEMATITE_STREAMS_DIR").expect("Env var HEMATITE_STREAMS_DIR is required");
    let streams_dir = PathBuf::from(streams_dir);
    fs::create_dir_all(&streams_dir).expect("Could not create stream database directory.");

    info!("Starting Hematite DB version {}", version);
    info!("Stream database directory: {}", streams_dir.display());

    let state = Arc::new(AppState::new(streams_dir).await?);

    let app = Router::new()
        .nest("/streams", api::stream_routes())
        .nest("/health", api::health_routes())
        .layer(middleware::from_fn(apply_secure_headers))
        .fallback(fallback)
        .with_state(state);

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
