pub mod db {
    use base64::{engine::general_purpose, Engine as _};
    use std::collections::BTreeMap;
    use std::fs::File;
    use std::io::prelude::*;
    use std::io::{self, BufRead};
    use std::path::PathBuf;

    pub struct Database {
        file: File,
        rownum: u64,
        primary_index: BTreeMap<u64, u64>,
    }

    impl Database {
        pub fn new(path: &PathBuf) -> std::io::Result<Self> {
            let file = File::options().append(true).create(true).open(path)?;

            Ok(Database {
                file: file,
                rownum: 0,
                primary_index: BTreeMap::new(),
            })
        }

        pub fn query(
            &mut self,
            rownum: u64,
        ) -> Option<std::io::Result<String>> {
            let row_offset = self.primary_index.get(&rownum)?;
            let _position = self.file
                .seek(io::SeekFrom::Start(*row_offset))
                .expect("Cannot seek to rownum's row");
            io::BufReader::new(&self.file).lines().next()
        }

        pub fn insert(
            &mut self,
            event: &[u8],
        ) -> std::io::Result<()> {
            let position = self.file.seek(io::SeekFrom::End(0))?;
            let encoded = encode_row(&event)?;
            self.file.write_all(&encoded)?;

            self.primary_index.insert(self.rownum, position);

            self.rownum += 1;

            Ok(())
        }

    }

    fn encode_row(row: &[u8]) -> std::io::Result<Vec<u8>> {
        let encoded: String = general_purpose::STANDARD_NO_PAD.encode(row);

        let mut row = Vec::new();
        write!(&mut row, "{}\n", encoded)?;
        Ok(row)
    }
}
