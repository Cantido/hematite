pub mod db {
    use base64::{engine::general_purpose, Engine as _};
    use cloudevents::*;
    use cloudevents::event::Event;
    use std::collections::BTreeMap;
    use std::fs::File;
    use std::io::prelude::*;
    use std::io::{self, BufRead};
    use std::path::PathBuf;
    use anyhow::{Result, bail};

    pub enum ExpectedRevision {
        Any,
        NoStream,
        StreamExists,
        Exact(u64),
    }

    pub struct Database {
        file: File,
        primary_index: BTreeMap<u64, u64>,
        source_id_index: BTreeMap<(String, String), u64>
    }

    impl Database {
        pub fn new(path: &PathBuf) -> Result<Self> {
            let file = File::options()
                .read(true)
                .append(true)
                .create(true)
                .open(path)?;

            Ok(Database {
                file,
                primary_index: BTreeMap::new(),
                source_id_index: BTreeMap::new()
            })
        }

        pub fn query(&mut self, rownum: u64) -> Result<Option<Event>> {
            let row_offset = match self.primary_index.get(&rownum) {
              Some(row_offset) => row_offset,
              None => return Ok(None)
            };
            let _position = self
                .file
                .seek(io::SeekFrom::Start(*row_offset))
                .expect("Cannot seek to rownum's row");

              io::BufReader::new(&self.file)
                  .lines()
                  .next()
                  .transpose()
                  .map(|r| r.map(decode_event))
                  .map_err(|e| e.into())
        }

        pub fn insert(
            &mut self,
            event: &mut Event,
            expected_revision: ExpectedRevision,
        ) -> Result<()> {
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
            } else {
                bail!("revision mismatch");
            }
        }

        fn write_event(&mut self, event: &mut Event) -> Result<()> {
            if self.source_id_index.contains_key(&(event.source().to_string(), event.id().to_string())) {
              bail!("Event with that source and ID value already exists in this stream");
            }
            let position = self.file.seek(io::SeekFrom::End(0))?;

            let (event_rownum, event_offset) =
              match self.primary_index.last_key_value() {
                  None => {
                      (0, 0)
                  }
                  Some((last_rownum, _offset)) => {
                      (last_rownum + 1, position)
                  }
              };

            event.set_extension("sequence", event_rownum.to_string());

            let encoded = encode_event(&event)?;
            self.file.write_all(&encoded)?;

            self.primary_index.insert(event_rownum, event_offset);
            self.source_id_index.insert((event.source().to_string(), event.id().to_string()), event_rownum);

            Ok(())
        }
    }

    fn encode_event(event: &Event) -> Result<Vec<u8>> {
        let json = serde_json::to_string(&event)?;
        let encoded: String = general_purpose::STANDARD_NO_PAD.encode(json);

        let mut row = Vec::new();
        write!(&mut row, "{}\n", encoded)?;
        Ok(row)
    }

    fn decode_event(row: String) -> Event {
        let trimmed_b64 = row.trim_end();
        let json_bytes = general_purpose::STANDARD_NO_PAD
            .decode(trimmed_b64)
            .expect("Expected row to be decodable from base64");
        let json = String::from_utf8(json_bytes).expect("Expected row to be valid UTF8");

        let event: Event =
            serde_json::from_str(&json).expect("Expected row to be deserializable to json");
        event
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

            let mut db = Database::new(test_file.path()).expect("Could not initialize DB");

            let mut event = Event::default();

            db.insert(&mut event, ExpectedRevision::Any).expect("Could not write to the DB");

            let result: Event = db
                .query(0)
                .expect("Row not found")
                .expect("Failed to read row");

            assert_eq!(result.id(), event.id());
        }

        #[test]
        fn cannot_write_duplicate_event() {
            let test_file = TestFile::new("writereadtest.db");
            let _ = test_file.delete();

            let mut db = Database::new(test_file.path()).expect("Could not initialize DB");

            let mut event = Event::default();

            db.insert(&mut event, ExpectedRevision::Any).expect("Could not write to the DB");
            assert!(db.insert(&mut event, ExpectedRevision::Any).is_err());
        }

        #[test]
        fn read_nonexistent() {
            let test_file = TestFile::new("readnonexistent.db");
            let _ = test_file.delete();

            let mut db = Database::new(test_file.path()).expect("Could not initialize DB");

            let result = db.query(0).expect("Expected success reading empty db");

            assert!(result.is_none());
        }

        #[test]
        fn can_write_expecting_no_stream_in_empty_db() {
            let test_file = TestFile::new("nostreamemptydb.db");
            let _ = test_file.delete();

            let mut db = Database::new(test_file.path()).expect("Could not initialize DB");

            let mut event = Event::default();

            db.insert(&mut event, ExpectedRevision::NoStream).expect("Could not write to the DB");
        }

        #[test]
        fn cannot_write_expecting_no_stream_in_non_empty_db() {
            let test_file = TestFile::new("nonemptydbexpectingnostream.db");
            let _ = test_file.delete();

            let mut db = Database::new(test_file.path()).expect("Could not initialize DB");

            let mut event1 = Event::default();
            let mut event2 = Event::default();
            db.insert(&mut event1, ExpectedRevision::NoStream).expect("Could not write to the DB");
            assert!(db.insert(&mut event2, ExpectedRevision::NoStream).is_err());
        }

        #[test]
        fn cannot_write_to_empty_db_expecting_stream_exists() {
            let test_file = TestFile::new("emptyexpectexists.db");
            let _ = test_file.delete();

            let mut db = Database::new(test_file.path()).expect("Could not initialize DB");

            let mut event = Event::default();

            assert!(db.insert(&mut event, ExpectedRevision::StreamExists).is_err());
        }

        #[test]
        fn cannot_write_expecting_revision_zero_with_empty_db() {
            let test_file = TestFile::new("expectrevisionzerofailure.db");
            let _ = test_file.delete();

            let mut db = Database::new(test_file.path()).expect("Could not initialize DB");

            let mut event = Event::default();

            assert!(db.insert(&mut event, ExpectedRevision::Exact(0)).is_err());
        }

        #[test]
        fn can_write_expecting_revision_zero_with_present_row() {
            let test_file = TestFile::new("expectrevisionzerofailure.db");
            let _ = test_file.delete();

            let mut db = Database::new(test_file.path()).expect("Could not initialize DB");

            let mut event1 = Event::default();
            let mut event2 = Event::default();
            db.insert(&mut event1, ExpectedRevision::NoStream).expect("Could not write to the DB");
            db.insert(&mut event2, ExpectedRevision::Exact(0)).expect("Could not write to the DB");
        }

        #[test]
        fn can_write_and_read_many() {
            let test_file = TestFile::new("writereadmanytest.db");
            let _ = test_file.delete();

            let mut db = Database::new(test_file.path()).expect("Could not initialize DB");

            let mut event = Event::default();

            for _n in 0..100 {
                db.insert(&mut Event::default(), ExpectedRevision::Any)
                    .expect("Could not write to the DB");
            }

            db.insert(&mut event, ExpectedRevision::Any).expect("Could not write to the DB");

            for _n in 0..100 {
                db.insert(&mut Event::default(), ExpectedRevision::Any)
                    .expect("Could not write to the DB");
            }

            let result: Event = db
                .query(100)
                .expect("Row not found")
                .expect("Failed to read row");

            assert_eq!(result.id(), event.id());
        }
    }
}
