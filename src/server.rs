use std::{
    fs,
    path::PathBuf,
    str,
    sync::Arc, fmt,
};
use anyhow::{Context, Result};
use cloudevents::Event;
use dashmap::DashMap;
use data_encoding::BASE32_NOPAD;
use tokio::sync::Mutex;
use tracing::{debug, error, info};
use serde::Serialize;
use crate::db::{
    Database,
    ExpectedRevision,
};


#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("stream not found")]
    StreamNotFound,
}

pub type UserId = String;
pub type StreamId = String;
pub type UserStreamId = (String, String);

fn user_stream_id(user_id: &UserId, stream_id: &StreamId) -> UserStreamId {
    (user_id.to_owned(), stream_id.to_owned())
}

#[derive(Clone, Debug)]
pub struct User {
    pub id: UserId,
}

#[derive(Debug, Serialize)]
pub struct Stream {
    #[serde(skip)]
    pub id: StreamId,
    pub revision: u64,
    pub last_modified: u64,
    pub usage: u64,
}

#[derive(Serialize)]
pub enum HealthStatus {
    Pass,
}

#[derive(Serialize)]
pub struct ApiHealth {
    pub status: HealthStatus,
}

type StreamMap = DashMap<UserStreamId, Arc<Mutex<Database>>>;

pub struct AppState {
    pub streams_path: PathBuf,
    pub streams: StreamMap,
}

impl fmt::Debug for AppState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let stream_count = self.streams.len();
        write!(f, "AppState [{} streams @ {:?}]", stream_count, self.streams_path)
    }
}

impl AppState {
    #[tracing::instrument]
    pub async fn new(streams_path: PathBuf) -> Result<Self> {
        let state = AppState {
            streams_path,
            streams: DashMap::new(),
        };

        info!("Initializing streams...");

        for user_dir_result in state
            .streams_path
            .read_dir()
            .with_context(|| format!("Couldn't read stream directory at {:?}", state.streams_path))?
        {
            if let Ok(user_dir) = user_dir_result {
                let user_path = user_dir.path();
                let user_id: UserId = user_path.file_stem().unwrap().to_str().unwrap().to_string();

                if user_id == "lost+found" {
                    continue;
                }

                for db_dir_result in user_dir
                    .path()
                    .read_dir()
                    .with_context(|| format!("Couldn't read user directory at {:?}", user_dir))?
                {
                    if let Ok(db_dir) = db_dir_result {
                        let db_dir_path = db_dir.path();
                        let encoded_stream_id = db_dir_path.file_name().unwrap().to_str().unwrap();
                        let stream_id_bytes = BASE32_NOPAD
                            .decode(encoded_stream_id.as_bytes())
                            .with_context(|| format!("Expected file in stream dir to have a Base32 no-pad encoded filename, but it was {}", encoded_stream_id))?;
                        let stream_id: StreamId = str::from_utf8(stream_id_bytes.as_slice())
                            .with_context(|| format!("Failed to convert stream file name into into string stream ID"))?
                            .to_string();

                        let user_stream_id = user_stream_id(&user_id, &stream_id);

                        state.initialize_database(&user_stream_id)?;
                    }
                }
            }
        }

        info!("Done initializing streams");

        Ok(state)
    }

    #[tracing::instrument]
    pub fn check_health(&self) -> ApiHealth {
        ApiHealth { status: HealthStatus::Pass }
    }

    fn initialize_database(&self, stream_id: &UserStreamId) -> Result<bool> {
        let init_db = !self.streams.contains_key(stream_id);

        if init_db {
            debug!("user_id={} stream_id={} msg=\"Initializing stream\"", stream_id.0, stream_id.1);

            let user_dir_path =
                self.streams_path
                .join(stream_id.0.to_string());

            fs::create_dir_all(&user_dir_path)
                .with_context(|| format!("Could not create user directory at {:?}", user_dir_path))?;

            let stream_file_name: String = BASE32_NOPAD.encode(stream_id.1.as_bytes());
            let db_path =
                user_dir_path
                .join(stream_file_name);

            let db = Database::new(&db_path);

            self.streams.insert(stream_id.clone(), Arc::new(Mutex::new(db)));
        }

        Ok(init_db)
    }

    #[tracing::instrument]
    pub async fn get_event(&self, user_id: &UserId, stream_id: &StreamId, rownum: u64) -> Result<Option<Event>> {
        let stream_id = user_stream_id(user_id, stream_id);
        let db = self.streams.get(&stream_id).ok_or(Error::StreamNotFound)?;

        let result = db.lock().await.query(rownum, 1).await;

        if let Ok(mut events) = result {
            Ok(events.pop())
        } else {
            Err(result.unwrap_err())
        }
    }

    #[tracing::instrument]
    pub async fn get_event_many(&self, user_id: &UserId, stream_id: &StreamId, start: u64, limit: usize) -> Result<Vec<Event>> {
        let stream_id = user_stream_id(user_id, stream_id);
        let db = self.streams.get(&stream_id).ok_or(Error::StreamNotFound)?;

        let result = db.lock().await.query(start, limit).await;
        result
    }

    #[tracing::instrument]
    pub async fn insert_event(&self, user_id: &UserId, stream_id: &StreamId, event: Event, revision: ExpectedRevision) -> Result<u64> {
        let stream_id = user_stream_id(user_id, stream_id);
        self.initialize_database(&stream_id)?;

        let db = self.streams.get(&stream_id).ok_or(Error::StreamNotFound)?;

        let result = db.lock().await.append(vec![event], revision).await;
        result
    }

    #[tracing::instrument]
    pub async fn insert_event_many(&self, user_id: &UserId, stream_id: &StreamId, events: Vec<Event>, revision: ExpectedRevision) -> Result<u64> {
        let stream_id = user_stream_id(user_id, stream_id);
        self.initialize_database(&stream_id)?;

        let db = self.streams.get(&stream_id).ok_or(Error::StreamNotFound)?;

        let result = db.lock().await.append(events, revision).await;
        result
    }

    pub async fn streams(&self, user_id: &UserId) -> Result<Vec<Stream>> {
        let mut stream_ids = vec![];

        for stream_file_result in self
            .streams_path
            .join(user_id)
            .read_dir()
            .with_context(|| format!("Couldn't read user directory at {:?}", self.streams_path.join(user_id)))?
        {
            if let Ok(stream_file) = stream_file_result {
                let stream_path = stream_file.path();
                let stream_name = stream_path.file_stem().unwrap().to_str().expect("Expected stream filename to be valid unicode");
                let stream_id_bytes = BASE32_NOPAD
                        .decode(stream_name.as_bytes())
                        .with_context(|| format!("Expected file in stream dir to have a Base32 no-pad encoded filename, but it was {}", stream_name))?;
                let stream_id = str::from_utf8(stream_id_bytes.as_slice())?.to_string();

                stream_ids.push(stream_id)
            }

        }

        let mut streams = vec![];

        for stream_id in stream_ids {
            if let Ok(stream) = self.get_stream(user_id, &stream_id).await {
                streams.push(stream);
            }
        }

        return Ok(streams);
    }

    #[tracing::instrument]
    pub async fn get_stream(&self, user_id: &UserId, stream_id: &StreamId) -> Result<Stream> {
        let user_stream_id = user_stream_id(user_id, stream_id);
        let db_lock = self.streams.get(&user_stream_id).ok_or(Error::StreamNotFound)?;

        let db = db_lock.lock().await;
        let revision = db.revision().await?;
        let last_modified = db.last_modified().await?;
        let usage = db.file_len().await?;

        Ok(Stream {
            id: stream_id.to_string(),
            usage,
            revision,
            last_modified,
        })
    }

    #[tracing::instrument]
    pub async fn delete_stream(&self, user_id: &UserId, stream_id: &StreamId) -> Result<bool> {
        let stream_id = user_stream_id(user_id, stream_id);

        if let Some((_, db_mutex)) = self.streams.remove(&stream_id) {
            let mut db = db_mutex.lock().await;
            db.delete().await.with_context(|| format!("user_id={} stream_id={} Failed to delete stream", stream_id.0, stream_id.1))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

