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
use cloudevents::Event;
use data_encoding::BASE32_NOPAD;
use log::info;
use serde::Serialize;
use crate::db::{
    Database,
    ExpectedRevision,
};

pub type UserId = String;

#[derive(Clone, Debug)]
pub struct User {
    pub id: UserId,
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
    pub fn new(streams_path: PathBuf) -> Self {
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

    pub fn check_health(&self) -> ApiHealth {
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

    pub fn get_event(&self, user_id: &str, stream_id: &str, rownum: u64) -> anyhow::Result<Option<Event>> {
        let users = self.streams.read().unwrap();
        let user_streams = users.get(user_id).unwrap();
        let db_option = user_streams.get(stream_id);

        if let Some(db) = db_option {
            db.lock().unwrap().query(rownum)
        } else {
            Ok(None)
        }
    }

    pub fn get_event_many(&self, user_id: &str, stream_id: &str, start: u64, limit: u64) -> anyhow::Result<Vec<Event>> {
        let users = self.streams.read().unwrap();
        let user_streams = users.get(user_id).unwrap();
        let db_option = user_streams.get(stream_id);

        if let Some(db) = db_option {
            db.lock().unwrap().query_many(start, limit)
        } else {
            Ok(vec![])
        }
    }

    pub fn insert_event(&self, user_id: &str, stream_id: &str, event: Event, revision: ExpectedRevision) -> anyhow::Result<u64> {
        self.initialize_database(user_id, stream_id);

        let users = self.streams.read().unwrap();
        let user_streams = users.get(user_id).unwrap();
        let db_option = user_streams.get(stream_id);

        if let Some(db) = db_option {
            db.lock().unwrap().insert(event, revision)
        } else {
            anyhow::bail!("stream not found")
        }
    }

    pub fn insert_event_many(&self, user_id: &str, stream_id: &str, events: Vec<Event>, revision: ExpectedRevision) -> anyhow::Result<u64> {
        self.initialize_database(user_id, stream_id);

        let users = self.streams.read().unwrap();
        let user_streams = users.get(user_id).unwrap();
        let db_option = user_streams.get(stream_id);

        if let Some(db) = db_option {
            db.lock().unwrap().insert_batch(events, revision)
        } else {
            anyhow::bail!("stream not found")
        }
    }

    pub fn delete_stream(&self, user_id: &str, stream_id: &str) -> anyhow::Result<bool> {
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

        Ok(stream_exists)
    }
}

