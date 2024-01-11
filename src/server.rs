use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    str,
    sync::{
        Mutex,
        RwLock,
    }
};
use anyhow::{Context, Result};
use cloudevents::Event;
use data_encoding::BASE32_NOPAD;
use log::debug;
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
    #[error("user not found")]
    UserNotFound,
}

pub type UserId = String;

#[derive(Clone, Debug)]
pub struct User {
    pub id: UserId,
}

#[derive(Debug, Serialize)]
pub struct Stream {
    pub revision: u64,
    pub state: RunState,
}

#[derive(Serialize)]
pub enum HealthStatus {
    Pass,
}

#[derive(Serialize)]
pub struct ApiHealth {
    pub status: HealthStatus,
}

type UserMap = HashMap<UserId, StreamMap>;
type StreamMap = HashMap<String, Mutex<Database>>;

pub struct AppState {
    pub streams_path: PathBuf,
    pub streams: RwLock<UserMap>,
}

impl AppState {
    pub fn new(streams_path: PathBuf) -> Result<Self> {
        let state = AppState {
            streams_path,
            streams: RwLock::new(HashMap::new()),
        };

        for user_dir_result in state
            .streams_path
            .read_dir()
            .with_context(|| format!("Couldn't read stream directory at {:?}", state.streams_path))?
        {
            if let Ok(user_dir) = user_dir_result {
                let user_path = user_dir.path();
                let user_id: UserId = user_path.file_stem().unwrap().to_str().unwrap().to_string();

                state.initialize_user(&user_id)?;

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
                        let stream_id = str::from_utf8(stream_id_bytes.as_slice())
                            .with_context(|| format!("Failed to convert stream file name into into string stream ID"))?;

                        state.initialize_database(&user_id, &stream_id)?;
                        state.start_database(&user_id, &stream_id)?;
                    }
                }
            }
        }

        Ok(state)
    }

    pub fn check_health(&self) -> ApiHealth {
        ApiHealth { status: HealthStatus::Pass }
    }

    fn initialize_user(&self, user_id: &str) -> Result<bool> {
        let mut users = self.streams.write().unwrap();

        let init_user = !users.contains_key(user_id);

        if init_user {
            debug!("user_id={} msg=\"Initializing user\"", user_id);

            let path = self.streams_path.join(user_id);
            fs::create_dir_all(&path)
                .with_context(|| format!("Could not create user directory at {:?}", path))?;

            users.insert(user_id.to_string(), HashMap::new());
        }

        Ok(init_user)
    }

    fn initialize_database(&self, user_id: &str, stream_id: &str) -> Result<bool> {
        let mut streams_mut = self.streams.write().unwrap();
        let stream_map = streams_mut.get_mut(user_id).ok_or(Error::UserNotFound)?;

        let init_db = !stream_map.contains_key(stream_id);

        if init_db {
            debug!("user_id={} stream_id={} msg=\"Initializing stream\"", user_id, stream_id);

            let stream_file_name: String = BASE32_NOPAD.encode(stream_id.as_bytes());

            let path =
                self.streams_path
                .join(user_id)
                .join(stream_file_name)
                .with_extension("hemadb");

            let db = Database::new(&path);

            stream_map.insert(stream_id.to_owned(), Mutex::new(db));
        }

        Ok(init_db)
    }

    fn start_database(&self, user_id: &str, stream_id: &str) -> Result<bool> {
        let users = self.streams.read().unwrap();
        let streams = users.get(user_id).ok_or(Error::UserNotFound)?;
        let db = streams.get(stream_id).ok_or(Error::StreamNotFound)?;

        let result = db.lock().unwrap().start();
        result
    }

    pub fn get_event(&self, user_id: &str, stream_id: &str, rownum: u64) -> Result<Option<Event>> {
        let users = self.streams.read().unwrap();
        let user_streams = users.get(user_id).ok_or(Error::UserNotFound)?;
        let db = user_streams.get(stream_id).ok_or(Error::StreamNotFound)?;

        let result = db.lock().unwrap().query(rownum);
        result
    }

    pub fn get_event_many(&self, user_id: &str, stream_id: &str, start: u64, limit: u64) -> Result<Vec<Event>> {
        let users = self.streams.read().unwrap();
        let user_streams = users.get(user_id).ok_or(Error::UserNotFound)?;
        let db = user_streams.get(stream_id).ok_or(Error::StreamNotFound)?;

        let result = db.lock().unwrap().query_many(start, limit);
        result
    }

    pub fn insert_event(&self, user_id: &str, stream_id: &str, event: Event, revision: ExpectedRevision) -> Result<u64> {
        if self.initialize_database(user_id, stream_id)? {
            self.start_database(user_id, stream_id)?;
        }

        let users = self.streams.read().unwrap();
        let user_streams = users.get(user_id).ok_or(Error::UserNotFound)?;
        let db = user_streams.get(stream_id).ok_or(Error::StreamNotFound)?;

        let result = db.lock().unwrap().insert(event, revision);
        result
    }

    pub fn insert_event_many(&self, user_id: &str, stream_id: &str, events: Vec<Event>, revision: ExpectedRevision) -> Result<u64> {
        if self.initialize_database(user_id, stream_id)? {
            self.start_database(user_id, stream_id)?;
        }

        let users = self.streams.read().unwrap();
        let user_streams = users.get(user_id).ok_or(Error::UserNotFound)?;
        let db = user_streams.get(stream_id).ok_or(Error::StreamNotFound)?;

        let result = db.lock().unwrap().insert_batch(events, revision);
        result
    }

    pub fn get_stream(&self, user_id: &str, stream_id: &str) -> Result<Stream> {
        let users = self.streams.read().unwrap();
        let user_streams = users.get(user_id).ok_or(Error::UserNotFound)?;
        let db_lock = user_streams.get(stream_id).ok_or(Error::StreamNotFound)?;

        let db = db_lock.lock().unwrap();
        let revision = db.revision();
        let state = db.state();

        Ok(Stream {
            revision,
            state,
        })
    }

    pub fn delete_stream(&self, user_id: &str, stream_id: &str) -> Result<bool> {
        let stream_exists = {
            self.streams
                .read()
                .unwrap()
                .get(user_id)
                .map(|user_streams| user_streams.get(stream_id))
                .flatten()
                .map(|db| db.lock().unwrap().delete())
                .transpose()
                .with_context(|| format!("user_id={} stream_id={} Failed to delete stream", user_id, stream_id))?
                .is_some()
        };

        if stream_exists {
            let mut users = self.streams.write().unwrap();

            if let Some(user_streams) = users.get_mut(user_id) {
                user_streams.remove(stream_id);
                return Ok(true)
            } else {
                return Ok(false)
            }
        } else {
            Ok(false)
        }
    }
}

