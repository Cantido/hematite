pub mod db {
    use base64::{engine::general_purpose, Engine as _};
    use std::fs::File;
    use std::io::prelude::*;
    use std::io::{self, BufRead};
    use std::collections::BTreeMap;

    fn encode_row(row: &[u8]) -> std::io::Result<Vec<u8>> {
        let encoded: String = general_purpose::STANDARD_NO_PAD.encode(row);

        let mut row = Vec::new();
        write!(&mut row, "{}\n", encoded)?;
        Ok(row)
    }

    pub fn insert(file: &mut File, rownum: &mut u64, primary_index: &mut BTreeMap<u64, u64>, event: &[u8]) -> std::io::Result<()> {
        let position = file.seek(io::SeekFrom::End(0))?;
        let encoded = encode_row(&event)?;
        file.write_all(&encoded)?;

        primary_index.insert(*rownum, position);

        *rownum += 1;

        Ok(())
    }

    pub fn query(file: &mut File, primary_index: &BTreeMap<u64, u64>, rownum: u64) -> Option<std::io::Result<String>>{
      let row_offset = primary_index.get(&rownum)?;
      let _position = file.seek(io::SeekFrom::Start(*row_offset)).expect("Cannot seek to rownum's row");
      io::BufReader::new(file).lines().next()
    }
}
