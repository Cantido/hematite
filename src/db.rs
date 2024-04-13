use anyhow::{ensure, Context, Result};
use cloudevents::*;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt;
use std::io::{SeekFrom, Write};
use std::time::SystemTime;
use tokio::fs::{File, self};
use tokio::io::{BufReader, AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt};
use std::path::Path;
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum RunState {
    Stopped,
    Running
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("revision mismatch")]
    RevisionMismatch,
    #[error("an event with that source and ID value is already present in the stream")]
    SourceIdConflict,
    #[error("the database is currently not accepting requests")]
    Stopped,
}

#[derive(Debug, Default)]
pub enum ExpectedRevision {
    #[default]
    Any,
    NoStream,
    StreamExists,
    Exact(u64),
}

#[derive(Clone)]
pub struct Database {
    state: RunState,
    path: PathBuf,
    primary_index: BTreeMap<u64, u64>,
}

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Database [{:?}]", self.path)
    }
}

impl Database {
    pub fn new(path: &Path) -> Self {
        Self {
            state: RunState::Stopped,
            path: path.to_path_buf(),
            primary_index: BTreeMap::new(),
        }
    }

    async fn load(&mut self) -> Result<()> {
        let events_path = self.events_path();

        let file = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(&events_path).await
            .with_context(|| format!("Could not open file to create DB at {:?}", events_path))?;

        let mut offset = 0u64;
        let mut rowid = 0u64;

        let mut lines = BufReader::new(file).lines();

        while let Some(line) = lines.next_line().await? {
            let rowlen: u64 = line.len() as u64;
            self.primary_index.insert(rowid as u64, offset);

            // offset addend is `rowlen + 1` because `BufReader::lines()` strips newlines for us
            offset += rowlen + 1;
            rowid += 1;
        }

        Ok(())
    }

    #[tracing::instrument]
    pub async fn start(&mut self) -> Result<bool> {
        match self.state {
            RunState::Running => {
                return Ok(false)
            }
            RunState::Stopped => {
                self.load().await?;
                self.state = RunState::Running;
                return Ok(true);
            }
        }
    }

    #[tracing::instrument]
    pub async fn last_modified(&self) -> Result<u64> {
        let events_path = self.events_path();

        fs::metadata(&events_path).await
            .with_context(|| format!("Failed to access metadata of DB path {:?}", &events_path))?
            .modified()
            .with_context(|| format!("Failed to access modified time of DB path {:?}", &events_path))?
            .duration_since(SystemTime::UNIX_EPOCH)
            .with_context(|| format!("Failed to convert mtime to unix time for DB path {:?}", &events_path))
            .map(|d| d.as_secs())
    }

    #[tracing::instrument]
    pub async fn file_len(&self) -> Result<u64> {
        let events_path = self.events_path();

        let size =
            fs::metadata(&events_path).await
                .with_context(|| format!("Failed to access metadata of DB path {:?}", &events_path))?
                .len();

        Ok(size)
    }

    #[tracing::instrument]
    pub fn revision(&self) -> u64 {
        self.primary_index.last_key_value().map_or(0, |(&k, _v)| k)
    }

    #[tracing::instrument]
    pub fn state(&self) -> RunState {
        self.state.clone()
    }

    #[tracing::instrument]
    pub async fn query(&self, start: u64, limit: usize) -> Result<Vec<Event>> {
        ensure!(self.state == RunState::Running, Error::Stopped);

        let events_path = self.events_path();

        let mut file = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(&events_path).await
            .with_context(|| format!("Could not open file to query DB at {:?}", events_path))?;

        let row_offset = match self.primary_index.get(&start) {
            Some(row_offset) => row_offset,
            None => return Ok(vec![]),
        };
        let _position = file
            .seek(SeekFrom::Start(*row_offset)).await
            .with_context(|| format!("Failed to seek to row {} (offset {}) from DB at {:?}", start, row_offset, events_path))?;

        let mut events = vec![];

        let mut lines = BufReader::new(file).lines();

        while let Some(line) = lines.next_line().await? {
            let event = decode_event(line)?;
            events.push(event);

            if events.len() >= limit {
                break
            }
        }

        Ok(events)
    }

    #[tracing::instrument]
    pub async fn append(
        &mut self,
        events: Vec<Event>,
        expected_revision: ExpectedRevision,
    ) -> Result<u64> {
        ensure!(self.state == RunState::Running, Error::Stopped);
        ensure!(!events.is_empty(), "Events list cannot be empty");

        let revision_match: bool = match expected_revision {
            ExpectedRevision::Any => true,
            ExpectedRevision::NoStream => self.primary_index.last_key_value().is_none(),
            ExpectedRevision::StreamExists => self.primary_index.last_key_value().is_some(),
            ExpectedRevision::Exact(revision) => self
                .primary_index
                .last_key_value()
                .map(|t| t.0)
                .map_or(false, |r| r == &revision),
        };

        if revision_match {
            self.write_events(&events).await
        } else {
            Err(Error::RevisionMismatch.into())
        }
    }

    async fn write_events(&mut self, events: &Vec<Event>) -> Result<u64> {
        ensure!(!events.is_empty(), "Events list cannot be empty");

        let events_path = self.events_path();

        let mut event_lengths = Vec::new();
        let mut bytes = Vec::new();

        for event in events.iter() {
            let json = serde_json::to_string(&event).with_context(|| format!("Failed to JSONify event"))?;

            event_lengths.push(bytes.len());
            write!(&mut bytes, "{}\n", json).with_context(|| format!("Failed to write JSON bytes to Vec"))?;
        }

        let mut file = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(&events_path).await
            .with_context(|| format!("Failed to open file for DB at {:?}", events_path))?;

        let position = file.seek(SeekFrom::End(0)).await
            .with_context(|| format!("Failed to seek to end of file for DB at {:?}", events_path))?;

        file.write_all(&bytes).await
            .with_context(|| format!("Failed to write event to file for DB at {:?}", events_path))?;

        let mut last_revision = 0;
        let mut prev_offset = position;

        for event_length in event_lengths.iter() {
            let (next_event_rownum, next_event_offset) = match self.primary_index.last_key_value() {
                None => (0, 0),
                Some((last_rownum, _offset)) => (last_rownum + 1, prev_offset),
            };

            prev_offset += *event_length as u64;
            last_revision = next_event_rownum;

            self.primary_index.insert(next_event_rownum, next_event_offset);
        }

        Ok(last_revision)
    }

    pub async fn delete(&mut self) -> anyhow::Result<()> {
        let events_path = self.events_path();

        fs::remove_file(&events_path).await
            .with_context(|| format!("Failed to delete datbase file at {:?}", events_path))?;
        self.primary_index.clear();

        Ok(())
    }

    fn events_path(&self) -> PathBuf {
        self.path.join("events.ndjson")
    }
}

fn decode_event(row: String) -> Result<Event> {
    let json = row.trim_end();

    serde_json::from_str(&json)
            .with_context(|| format!("Failed to decode event JSON"))
}

#[cfg(test)]
mod tests {
    use cloudevents::event::Event;
    use cloudevents::*;
    use tempfile::tempdir;

    use crate::db::ExpectedRevision;

    use super::Database;

    #[tokio::test]
    async fn can_write_and_read() {
        let test_file = tempdir().unwrap();

        let mut db = Database::new(test_file.path());
        db.start().await.expect("Could not start DB");

        let event = Event::default();

        let rownum = db.append(vec![event.clone()], ExpectedRevision::Any).await
            .expect("Could not write to the DB");

        let result: Event = db
            .query(0, 1).await
            .expect("Row not found")
            .pop()
            .expect("Failed to read row");

        assert_eq!(rownum, 0);
        assert_eq!(result.id(), event.id());
    }

    #[tokio::test]
    async fn read_nonexistent() {
        let test_file = tempdir().unwrap();

        let mut db = Database::new(test_file.path());
        db.start().await.expect("Could not start DB");

        let result = db.query(0, 1).await.expect("Expected success reading empty db");

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn can_write_expecting_no_stream_in_empty_db() {
        let test_file = tempdir().unwrap();

        let mut db = Database::new(test_file.path());
        db.start().await.expect("Could not start DB");

        let event = Event::default();

        db.append(vec![event], ExpectedRevision::NoStream).await
            .expect("Could not write to the DB");
    }

    #[tokio::test]
    async fn cannot_write_expecting_no_stream_in_non_empty_db() {
        let test_file = tempdir().unwrap();

        let mut db = Database::new(test_file.path());
        db.start().await.expect("Could not start DB");

        let event1 = Event::default();
        let event2 = Event::default();
        db.append(vec![event1], ExpectedRevision::NoStream).await
            .expect("Could not write to the DB");
        assert!(db.append(vec![event2], ExpectedRevision::NoStream).await.is_err());
    }

    #[tokio::test]
    async fn cannot_write_to_empty_db_expecting_stream_exists() {
        let test_file = tempdir().unwrap();

        let mut db = Database::new(test_file.path());
        db.start().await.expect("Could not start DB");

        let event = Event::default();

        assert!(db.append(vec![event], ExpectedRevision::StreamExists).await.is_err());
    }

    #[tokio::test]
    async fn cannot_write_expecting_revision_zero_with_empty_db() {
        let test_file = tempdir().unwrap();

        let mut db = Database::new(test_file.path());
        db.start().await.expect("Could not start DB");

        let event = Event::default();

        assert!(db.append(vec![event], ExpectedRevision::Exact(0)).await.is_err());
    }

    #[tokio::test]
    async fn can_write_expecting_revision_zero_with_present_row() {
        let test_file = tempdir().unwrap();

        let mut db = Database::new(test_file.path());
        db.start().await.expect("Could not start DB");

        let event1 = Event::default();
        let event2 = Event::default();
        db.append(vec![event1], ExpectedRevision::NoStream).await
            .expect("Could not write to the DB");
        db.append(vec![event2], ExpectedRevision::Exact(0)).await
            .expect("Could not write to the DB");
    }

    #[tokio::test]
    async fn can_write_and_read_many() {
        let test_file = tempdir().unwrap();

        let mut db = Database::new(test_file.path());
        db.start().await.expect("Could not start DB");

        let event = Event::default();

        for n in 0..100 {
            let rownum =
                db.append(vec![Event::default()], ExpectedRevision::Any).await
                .expect("Could not write to the DB");

            assert_eq!(rownum, n);
        }

        db.append(vec![event.clone()], ExpectedRevision::Any).await
            .expect("Could not write to the DB");

        for n in 0..100 {
            let rownum =
                db.append(vec![Event::default()], ExpectedRevision::Any).await
                .expect("Could not write to the DB");

            assert_eq!(rownum, n + 101);
        }

        let result: Event = db
            .query(100, 1).await
            .expect("Row not found")
            .pop()
            .expect("Failed to read row");

        assert_eq!(result.id(), event.id());
    }
}
