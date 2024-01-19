use std::{
    fs,
    path::PathBuf,
    str,
    sync::{Mutex, Arc}, fmt,
};
use anyhow::{Context, Result};
use cloudevents::Event;
use dashmap::DashMap;
use data_encoding::BASE32_NOPAD;
use tokio::task::JoinSet;
use tracing::{debug, error, info};
use serde::Serialize;
use crate::db::{
    Database,
    ExpectedRevision,
    RunState,
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
    pub revision: u64,
    pub state: RunState,
    pub last_modified: i64,
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
                let user_id: UserId = user_path.file_stem().unwrap().to_str().unwrap().parse()?;

                for db_file_result in user_dir
                    .path()
                    .read_dir()
                    .with_context(|| format!("Couldn't read user directory at {:?}", user_dir))?
                {
                    if let Ok(db_file) = db_file_result {
                        let filepath = db_file.path();
                        let encoded_stream_id = filepath.file_stem().unwrap().to_str().unwrap();
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

            let mut tasks = JoinSet::new();

            for stream_id in state.streams.iter() {
                let db = stream_id.value().clone();

                tasks.spawn(async move {
                    db.lock().unwrap().start()
                });
            }

            while let Some(res) = tasks.join_next().await {
                if let Err(err) = res.unwrap() {
                    error!("Failed to start database: {}", err);
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
                .join(stream_file_name)
                .with_extension("hemadb");

            let db = Database::new(&db_path);

            self.streams.insert(stream_id.clone(), Arc::new(Mutex::new(db)));
        }

        Ok(init_db)
    }

    fn start_database(&self, stream_id: &UserStreamId) -> Result<bool> {
        let db = self.streams.get(stream_id).ok_or(Error::StreamNotFound)?;

        let result = db.lock().unwrap().start();
        result
    }

    #[tracing::instrument]
    pub fn get_event(&self, user_id: &UserId, stream_id: &StreamId, rownum: u64) -> Result<Option<Event>> {
        let stream_id = user_stream_id(user_id, stream_id);
        let db = self.streams.get(&stream_id).ok_or(Error::StreamNotFound)?;

        let result = db.lock().unwrap().query(rownum, 1);

        if let Ok(mut events) = result {
            Ok(events.pop())
        } else {
            Err(result.unwrap_err())
        }
    }

    #[tracing::instrument]
    pub fn get_event_many(&self, user_id: &UserId, stream_id: &StreamId, start: u64, limit: usize) -> Result<Vec<Event>> {
        let stream_id = user_stream_id(user_id, stream_id);
        let db = self.streams.get(&stream_id).ok_or(Error::StreamNotFound)?;

        let result = db.lock().unwrap().query(start, limit);
        result
    }

    #[tracing::instrument]
    pub fn insert_event(&self, user_id: &UserId, stream_id: &StreamId, event: Event, revision: ExpectedRevision) -> Result<u64> {
        let stream_id = user_stream_id(user_id, stream_id);
        if self.initialize_database(&stream_id)? {
            self.start_database(&stream_id)?;
        }

        let db = self.streams.get(&stream_id).ok_or(Error::StreamNotFound)?;

        let result = db.lock().unwrap().append(vec![event], revision);
        result
    }

    #[tracing::instrument]
    pub fn insert_event_many(&self, user_id: &UserId, stream_id: &StreamId, events: Vec<Event>, revision: ExpectedRevision) -> Result<u64> {
        let stream_id = user_stream_id(user_id, stream_id);
        if self.initialize_database(&stream_id)? {
            self.start_database(&stream_id)?;
        }

        let db = self.streams.get(&stream_id).ok_or(Error::StreamNotFound)?;

        let result = db.lock().unwrap().append(events, revision);
        result
    }

    #[tracing::instrument]
    pub fn get_stream(&self, user_id: &UserId, stream_id: &StreamId) -> Result<Stream> {
        let stream_id = user_stream_id(user_id, stream_id);
        let db_lock = self.streams.get(&stream_id).ok_or(Error::StreamNotFound)?;

        let db = db_lock.lock().unwrap();
        let revision = db.revision();
        let state = db.state();
        let last_modified = db.last_modified()?;

        Ok(Stream {
            revision,
            state,
            last_modified,
        })
    }

    #[tracing::instrument]
    pub fn delete_stream(&self, user_id: &UserId, stream_id: &StreamId) -> Result<bool> {
        let stream_id = user_stream_id(user_id, stream_id);

        if let Some((_, db_mutex)) = self.streams.remove(&stream_id) {
            let mut db = db_mutex.lock().unwrap();
            db.delete().with_context(|| format!("user_id={} stream_id={} Failed to delete stream", stream_id.0, stream_id.1))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

