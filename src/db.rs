use anyhow::{ensure, Context, Result};
use cloudevents::*;
use serde::Serialize;
use std::fmt;
use std::io::{SeekFrom, Write};
use std::time::SystemTime;
use tokio::fs::{File, self};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
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
    path: PathBuf,
}

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Database [{:?}]", self.path)
    }
}

impl Database {
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
        }
    }

    #[tracing::instrument]
    pub async fn rebuild_index(&mut self) -> Result<()> {
        let events_path = self.events_path();
        let file = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(&events_path).await
            .with_context(|| format!("Could not open file to create DB at {:?}", events_path))?;

        let index_path = self.index_path();
        let mut index_file = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(&index_path).await
            .with_context(|| format!("Failed to open file for index at {:?}", index_path))?;

        let mut offset = 0u64;
        let mut lines = BufReader::new(file).lines();

        index_file.write_u64(offset).await?;

        while let Some(line) = lines.next_line().await? {
            // offset addend is `rowlen + 1` because `BufReader::lines()` strips newlines for us
            offset += line.len() as u64 + 1;

            index_file.write_u64(offset).await?;
        }

        Ok(())
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
    pub async fn revision(&self) -> Result<u64> {
        let index_path = self.index_path();

        if index_path.try_exists()? {
            fs::metadata(&index_path).await
                .with_context(|| format!("Failed to metadata of index at {:?}", index_path))
                .map(|m| m.len() / 8)
        } else {
            Ok(0)
        }
    }

    pub async fn last_offset(&self) -> Result<u64> {
        let index_path = self.index_path();

        if index_path.try_exists()? {
        let mut index_file = File::options()
            .read(true)
            .open(&index_path).await
            .with_context(|| format!("Failed to open file for index at {:?}", index_path))?;

        index_file.seek(SeekFrom::End(8)).await?;
        index_file.read_u64().await
            .with_context(|| format!("Failed to read u64 from index at {:?}", index_path))
        } else {
            Ok(0)
        }
    }

    #[tracing::instrument]
    pub async fn query(&self, start: u64, limit: usize) -> Result<Vec<Event>> {
        let index_path = self.index_path();

        if !index_path.try_exists()? {
            return Ok(vec![]);
        }

        let mut index_file = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(&index_path).await
            .with_context(|| format!("Could not open index file at {:?}", index_path))?;

        index_file.seek(SeekFrom::Start(start * 8)).await?;

        let start_offset = index_file.read_u64().await?;
        let events_path = self.events_path();

        let mut file = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(&events_path).await
            .with_context(|| format!("Could not open file to query DB at {:?}", events_path))?;

        let _position = file
            .seek(SeekFrom::Start(start_offset)).await
            .with_context(|| format!("Failed to seek to row {} (offset {}) from DB at {:?}", start, start_offset, events_path))?;

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
        ensure!(!events.is_empty(), "Events list cannot be empty");

        let current_revision = self.revision().await?;

        let revision_match: bool = match expected_revision {
            ExpectedRevision::Any => true,
            ExpectedRevision::NoStream => current_revision == 0,
            ExpectedRevision::StreamExists => current_revision > 0,
            ExpectedRevision::Exact(revision) => current_revision == revision,
        };

        if !revision_match {
            return Err(Error::RevisionMismatch.into());
        }

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

        let mut event_offset = file.seek(SeekFrom::End(0)).await
            .with_context(|| format!("Failed to seek to end of file for DB at {:?}", events_path))?;

        file.write_all(&bytes).await
            .with_context(|| format!("Failed to write event to file for DB at {:?}", events_path))?;

        let index_path = self.index_path();
        let mut index_file = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(&index_path).await
            .with_context(|| format!("Failed to open file for index at {:?}", index_path))?;

        for event_length in event_lengths.iter() {
            index_file.write_u64(event_offset).await?;

            event_offset += *event_length as u64 + 1;
        }

        Ok(current_revision + events.len() as u64)
    }

    pub async fn delete(&mut self) -> anyhow::Result<()> {
        let events_path = self.events_path();
        fs::remove_file(&events_path).await
            .with_context(|| format!("Failed to delete database file at {:?}", events_path))?;

        let index_path = self.index_path();
        fs::remove_file(&index_path).await
            .with_context(|| format!("Failed to delete index file at {:?}", events_path))?;

        Ok(())
    }

    fn events_path(&self) -> PathBuf {
        self.path.join("events.ndjson")
    }
    fn index_path(&self) -> PathBuf {
        self.path.join("index.dat")
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

        let event = Event::default();

        let rownum = db.append(vec![event.clone()], ExpectedRevision::Any).await
            .expect("Could not write to the DB");

        let result: Event = db
            .query(0, 1).await
            .expect("Row not found")
            .pop()
            .expect("Failed to read row");

        assert_eq!(rownum, 1);
        assert_eq!(result.id(), event.id());
    }

    #[tokio::test]
    async fn read_nonexistent() {
        let test_file = tempdir().unwrap();

        let db = Database::new(test_file.path());

        let result = db.query(0, 1).await.expect("Expected success reading empty db");

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn can_write_expecting_no_stream_in_empty_db() {
        let test_file = tempdir().unwrap();

        let mut db = Database::new(test_file.path());

        let event = Event::default();

        db.append(vec![event], ExpectedRevision::NoStream).await
            .expect("Could not write to the DB");
    }

    #[tokio::test]
    async fn cannot_write_expecting_no_stream_in_non_empty_db() {
        let test_file = tempdir().unwrap();

        let mut db = Database::new(test_file.path());

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

        let event = Event::default();

        assert!(db.append(vec![event], ExpectedRevision::StreamExists).await.is_err());
    }

    #[tokio::test]
    async fn can_write_expecting_revision_zero_with_present_row() {
        let test_file = tempdir().unwrap();

        let mut db = Database::new(test_file.path());

        let event1 = Event::default();
        let event2 = Event::default();
        db.append(vec![event1], ExpectedRevision::NoStream).await
            .expect("Could not write to the DB");
        db.append(vec![event2], ExpectedRevision::Exact(1)).await
            .expect("Could not write to the DB");
    }

    #[tokio::test]
    async fn can_write_and_read_many() {
        let test_file = tempdir().unwrap();

        let mut db = Database::new(test_file.path());

        let event = Event::default();

        for n in 1..100 {
            let rownum =
                db.append(vec![Event::default()], ExpectedRevision::Any).await
                .expect("Could not write to the DB");

            assert_eq!(rownum, n);
        }

        db.append(vec![event.clone()], ExpectedRevision::Any).await
            .expect("Could not write to the DB");

        for n in 1..100 {
            let rownum =
                db.append(vec![Event::default()], ExpectedRevision::Any).await
                .expect("Could not write to the DB");

            assert_eq!(rownum, n + 100);
        }

        let result: Event = db
            .query(99, 1).await
            .expect("Row not found")
            .pop()
            .expect("Failed to read row");

        assert_eq!(result.id(), event.id());
    }
}
