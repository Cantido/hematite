use anyhow::{Context, Result};
use cloudevents::event::Event;
use cloudevents::*;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs::{File, self};
use std::io::prelude::*;
use std::io::{self, BufRead};
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

pub struct Database {
    state: RunState,
    path: PathBuf,
    primary_index: BTreeMap<u64, u64>,
    source_id_index: BTreeMap<(String, String), u64>,
}

impl Database {
    pub fn new(path: &Path) -> Self {
        Self {
            state: RunState::Stopped,
            path: path.to_path_buf(),
            primary_index: BTreeMap::new(),
            source_id_index: BTreeMap::new(),
        }
    }

    fn load(&mut self) -> Result<()> {
        let file = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(&self.path)
            .with_context(|| format!("Could not open file to create DB at {:?}", self.path))?;

        let mut offset = 0u64;

        for (rowid, line_result) in io::BufReader::new(&file).lines().enumerate() {
            let line = line_result.with_context(|| format!("Failed to read line {} of DB at {:?}", rowid + 1, self.path))?;
            let rowlen: u64 = line.len() as u64;

            let event = decode_event(line).with_context(|| format!("Failed to decode line {} of DB at {:?}", rowid + 1, self.path))?;
            self.primary_index.insert(rowid as u64, offset);
            self.source_id_index.insert((event.source().to_string(), event.id().to_string()), rowid as u64);

            // offset addend is `rowlen + 1` because `BufReader::lines()` strips newlines for us
            offset = offset + rowlen + 1;
        }

        Ok(())
    }

    pub fn start(&mut self) -> Result<bool> {
        match self.state {
            RunState::Running => {
                return Ok(false)
            }
            RunState::Stopped => {
                self.load()?;
                self.state = RunState::Running;
                return Ok(true);
            }
        }
    }

    pub fn revision(&self) -> u64 {
        self.primary_index.last_key_value().map_or(0, |(&k, _v)| k)
    }

    pub fn state(&self) -> RunState {
        self.state.clone()
    }

    pub fn query(&mut self, rownum: u64) -> Result<Option<Event>> {
        if self.state != RunState::Running {
            return Err(Error::Stopped.into());
        }

        let mut file = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(&self.path)
            .with_context(|| format!("Could not open file to query DB at {:?}", self.path))?;

        let row_offset = match self.primary_index.get(&rownum) {
            Some(row_offset) => row_offset,
            None => return Ok(None),
        };

        let _position = file
            .seek(io::SeekFrom::Start(*row_offset))
            .with_context(|| format!("Failed to seek to row {} (offset {}) from DB at {:?}", rownum, row_offset, self.path))?;

        let event =
            if let Some(line_result) = io::BufReader::new(&file).lines().next() {
                let line = line_result.with_context(|| format!("Failed to read rowid {} (offset {}) from DB at {:?}", rownum, row_offset, self.path))?;
                let event = decode_event(line).with_context(|| format!("Failed to decode row {} (offset {}) from DB at {:?}", rownum, row_offset, self.path))?;

                Some(event)
            } else {
                None
            };

        Ok(event)
    }

    pub fn query_many(&mut self, start: u64, limit: u64) -> Result<Vec<Event>> {
        if self.state != RunState::Running {
            return Err(Error::Stopped.into());
        }

        let mut file = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(&self.path)
            .with_context(|| format!("Could not open file to query DB at {:?}", self.path))?;

        let row_offset = match self.primary_index.get(&start) {
            Some(row_offset) => row_offset,
            None => return Ok(vec![]),
        };
        let _position = file
            .seek(io::SeekFrom::Start(*row_offset))
            .with_context(|| format!("Failed to seek to row {} (offset {}) from DB at {:?}", start, row_offset, self.path))?;

        let mut events = vec![];

        for (rowid, line_result) in io::BufReader::new(&file).lines().take(limit.try_into().unwrap()).enumerate() {
            let line = line_result.with_context(|| format!("Failed to read rowid {} from DB at {:?}", rowid, self.path))?;
            let event = decode_event(line)?;
            events.push(event);
        }

        Ok(events)
    }

    pub fn insert(&mut self, event: Event, expected_revision: ExpectedRevision) -> Result<u64> {
        if self.state != RunState::Running {
            return Err(Error::Stopped.into());
        }

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
            self.write_event(event)
                .with_context(|| format!("Failed to write event to DB at {:?}", self.path))
        } else {
            Err(Error::RevisionMismatch.into())
        }
    }

    pub fn insert_batch(
        &mut self,
        events: Vec<Event>,
        expected_revision: ExpectedRevision,
    ) -> Result<u64> {
        if self.state != RunState::Running {
            return Err(Error::Stopped.into());
        }

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
            for event in events.iter() {
                self.write_event(event.clone())?;
            }
            Ok(self.primary_index.last_key_value().unwrap().0.clone())
        } else {
            Err(Error::RevisionMismatch.into())
        }
    }

    fn write_event(&mut self, event: Event) -> Result<u64> {
        if self
            .source_id_index
            .contains_key(&(event.source().to_string(), event.id().to_string()))
        {
            return Err(Error::SourceIdConflict.into());
        }

        let mut file = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(&self.path)
            .with_context(|| format!("Failed to open file for DB at {:?}", self.path))?;

        let position = file.seek(io::SeekFrom::End(0))
            .with_context(|| format!("Failed to seek to end of file for DB at {:?}", self.path))?;

        let encoded = encode_event(&event)
            .with_context(|| format!("Failed to encode event"))?;

        file.write_all(&encoded)
            .with_context(|| format!("Failed to write event to file for DB at {:?}", self.path))?;

        let (event_rownum, event_offset) = match self.primary_index.last_key_value() {
            None => (0, 0),
            Some((last_rownum, _offset)) => (last_rownum + 1, position),
        };

        self.primary_index.insert(event_rownum, event_offset);
        self.source_id_index.insert(
            (event.source().to_string(), event.id().to_string()),
            event_rownum,
        );

        Ok(event_rownum)
    }

    pub fn delete(&mut self) -> anyhow::Result<()> {
        fs::remove_file(&self.path)
            .with_context(|| format!("Failed to delete datbase file at {:?}", self.path))?;
        self.primary_index.clear();
        self.source_id_index.clear();

        Ok(())
    }
}

fn encode_event(event: &Event) -> Result<Vec<u8>> {
    let json = serde_json::to_string(&event).with_context(|| format!("Failed to JSONify event"))?;

    let mut row = Vec::new();
    write!(&mut row, "{}\n", json).with_context(|| format!("Failed to write JSON bytes to Vec"))?;
    Ok(row)
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
    use std::path::PathBuf;

    use crate::db::ExpectedRevision;

    use super::Database;

    struct TestFile(PathBuf);

    impl TestFile {
        pub fn new(filename: &str) -> Self {
            let mut tmp = std::env::temp_dir();
            tmp.push(filename);
            Self(tmp)
        }

        pub fn path(&self) -> &PathBuf {
            &self.0
        }

        pub fn delete(&self) -> std::io::Result<()> {
            std::fs::remove_file(&self.0)
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            let _ = self.delete();
        }
    }

    #[test]
    fn can_write_and_read() {
        let test_file = TestFile::new("writereadtest.db");
        let _ = test_file.delete();

        let mut db = Database::new(test_file.path());
        db.start().expect("Could not start DB");

        let event = Event::default();

        let rownum = db.insert(event.clone(), ExpectedRevision::Any)
            .expect("Could not write to the DB");

        let result: Event = db
            .query(0)
            .expect("Row not found")
            .expect("Failed to read row");

        assert_eq!(rownum, 0);
        assert_eq!(result.id(), event.id());
    }

    #[test]
    fn cannot_write_duplicate_event() {
        let test_file = TestFile::new("writereadtest.db");
        let _ = test_file.delete();

        let mut db = Database::new(test_file.path());
        db.start().expect("Could not start DB");

        let event = Event::default();

        let rownum =
            db.insert(event.clone(), ExpectedRevision::Any)
            .expect("Could not write to the DB");

        assert_eq!(rownum, 0);
        assert!(db.insert(event.clone(), ExpectedRevision::Any).is_err());
    }

    #[test]
    fn read_nonexistent() {
        let test_file = TestFile::new("readnonexistent.db");
        let _ = test_file.delete();

        let mut db = Database::new(test_file.path());
        db.start().expect("Could not start DB");

        let result = db.query(0).expect("Expected success reading empty db");

        assert!(result.is_none());
    }

    #[test]
    fn can_write_expecting_no_stream_in_empty_db() {
        let test_file = TestFile::new("nostreamemptydb.db");
        let _ = test_file.delete();

        let mut db = Database::new(test_file.path());
        db.start().expect("Could not start DB");

        let event = Event::default();

        db.insert(event, ExpectedRevision::NoStream)
            .expect("Could not write to the DB");
    }

    #[test]
    fn cannot_write_expecting_no_stream_in_non_empty_db() {
        let test_file = TestFile::new("nonemptydbexpectingnostream.db");
        let _ = test_file.delete();

        let mut db = Database::new(test_file.path());
        db.start().expect("Could not start DB");

        let event1 = Event::default();
        let event2 = Event::default();
        db.insert(event1, ExpectedRevision::NoStream)
            .expect("Could not write to the DB");
        assert!(db.insert(event2, ExpectedRevision::NoStream).is_err());
    }

    #[test]
    fn cannot_write_to_empty_db_expecting_stream_exists() {
        let test_file = TestFile::new("emptyexpectexists.db");
        let _ = test_file.delete();

        let mut db = Database::new(test_file.path());
        db.start().expect("Could not start DB");

        let event = Event::default();

        assert!(db.insert(event, ExpectedRevision::StreamExists).is_err());
    }

    #[test]
    fn cannot_write_expecting_revision_zero_with_empty_db() {
        let test_file = TestFile::new("expectrevisionzerofailure.db");
        let _ = test_file.delete();

        let mut db = Database::new(test_file.path());
        db.start().expect("Could not start DB");

        let event = Event::default();

        assert!(db.insert(event, ExpectedRevision::Exact(0)).is_err());
    }

    #[test]
    fn can_write_expecting_revision_zero_with_present_row() {
        let test_file = TestFile::new("expectrevisionzerofailure.db");
        let _ = test_file.delete();

        let mut db = Database::new(test_file.path());
        db.start().expect("Could not start DB");

        let event1 = Event::default();
        let event2 = Event::default();
        db.insert(event1, ExpectedRevision::NoStream)
            .expect("Could not write to the DB");
        db.insert(event2, ExpectedRevision::Exact(0))
            .expect("Could not write to the DB");
    }

    #[test]
    fn can_write_and_read_many() {
        let test_file = TestFile::new("writereadmanytest.db");
        let _ = test_file.delete();

        let mut db = Database::new(test_file.path());
        db.start().expect("Could not start DB");

        let event = Event::default();

        for n in 0..100 {
            let rownum =
                db.insert(Event::default(), ExpectedRevision::Any)
                .expect("Could not write to the DB");

            assert_eq!(rownum, n);
        }

        db.insert(event.clone(), ExpectedRevision::Any)
            .expect("Could not write to the DB");

        for n in 0..100 {
            let rownum =
                db.insert(Event::default(), ExpectedRevision::Any)
                .expect("Could not write to the DB");

            assert_eq!(rownum, n + 101);
        }

        let result: Event = db
            .query(100)
            .expect("Row not found")
            .expect("Failed to read row");

        assert_eq!(result.id(), event.id());
    }
}
