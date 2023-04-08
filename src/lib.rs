pub mod db {
    use base64::{engine::general_purpose, Engine as _};
    use std::collections::BTreeMap;
    use std::fs::File;
    use std::io::prelude::*;
    use std::io::{self, BufRead};
    use std::path::PathBuf;
    use cloudevents::event::Event;

    pub struct Database {
        file: File,
        primary_index: BTreeMap<u64, u64>,
    }

    impl Database {
        pub fn new(path: &PathBuf) -> std::io::Result<Self> {
            let file = File::options().read(true).append(true).create(true).open(path)?;

            Ok(Database {
                file: file,
                primary_index: BTreeMap::new(),
            })
        }

        pub fn query(
            &mut self,
            rownum: u64,
        ) -> Option<std::io::Result<Event>> {
            let row_offset = self.primary_index.get(&rownum)?;
            let _position = self.file
                .seek(io::SeekFrom::Start(*row_offset))
                .expect("Cannot seek to rownum's row");
            io::BufReader::new(&self.file).lines().next().map(|r| r.map(decode_event))
        }

        pub fn insert(
            &mut self,
            event: &Event,
        ) -> std::io::Result<()> {
            let position = self.file.seek(io::SeekFrom::End(0))?;
            let encoded = encode_event(&event)?;
            self.file.write_all(&encoded)?;

            match self.primary_index.last_key_value() {
              None => {
                self.primary_index.insert(0, 0);
                Ok(())
              }

              Some((last_rownum, _offset)) => {
                self.primary_index.insert(last_rownum + 1, position);
                Ok(())
              }
            }

        }

    }

    fn encode_event(event: &Event) -> std::io::Result<Vec<u8>> {
        let json = serde_json::to_string(&event)?;
        let encoded: String = general_purpose::STANDARD_NO_PAD.encode(json);

        let mut row = Vec::new();
        write!(&mut row, "{}\n", encoded)?;
        Ok(row)
    }

    fn decode_event(row: String) -> Event {
      let trimmed_b64 = row.trim_end();
      let json_bytes = general_purpose::STANDARD_NO_PAD.decode(trimmed_b64).expect("Expected row to be decodable from base64");
      let json = String::from_utf8(json_bytes).expect("Expected row to be valid UTF8");

      let event: Event = serde_json::from_str(&json).expect("Expected row to be deserializable to json");
      event
    }

    #[cfg(test)]
    mod tests {
        use std::path::PathBuf;
        use cloudevents::*;
        use cloudevents::event::Event;

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

          let event = Event::default();

          db.insert(&event).expect("Could not write to the DB");

          let result: Event = db.query(0).expect("Row not found").expect("Failed to read row");

          assert_eq!(result.id(), event.id());
        }

        #[test]
        fn read_nonexistent() {
          let test_file = TestFile::new("readnonexistent.db");
          let _ = test_file.delete();

          let mut db = Database::new(test_file.path()).expect("Could not initialize DB");

          let result = db.query(0);

          assert!(result.is_none());
        }

        #[test]
        fn can_write_and_read_many() {
          let test_file = TestFile::new("writereadmanytest.db");
          let _ = test_file.delete();

          let mut db = Database::new(test_file.path()).expect("Could not initialize DB");

          let event = Event::default();

          for _n in 0..100 {
            db.insert(&Event::default()).expect("Could not write to the DB");
          }

          db.insert(&event).expect("Could not write to the DB");

          for _n in 0..100 {
            db.insert(&Event::default()).expect("Could not write to the DB");
          }

          let result: Event = db.query(100).expect("Row not found").expect("Failed to read row");

          assert_eq!(result.id(), event.id());
        }
    }
}

