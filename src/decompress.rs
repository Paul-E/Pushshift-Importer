use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::Path;

use flate2::read::GzDecoder;

pub fn gzip_file(file_name: &Path) -> impl BufRead {
    let file = File::open(file_name).unwrap();
    BufReader::new(GzDecoder::new(file))
}
