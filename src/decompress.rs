use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::Path;

use anyhow::{anyhow, Result};
use bzip2::read::BzDecoder;
use fallible_streaming_iterator::FallibleStreamingIterator;
use flate2::read::GzDecoder;
use xz2::read::XzDecoder;

// represents the maximum distance as calculated by 2^log_distance for a decode window in zstd.
const ZSTD_DECODE_WINDOW_LOG_MAX: u32 = 31;

pub fn stream_lines(
    filename: &Path,
) -> Result<impl FallibleStreamingIterator<Item = str, Error = std::io::Error>> {
    let extension = filename
        .extension()
        .and_then(|extension| extension.to_str())
        .ok_or_else(|| anyhow!("cannot the file extension for {}", filename.display()))?;
    if extension == "gz" {
        let file = File::open(filename)?;
        let gzip_file = BufReader::new(GzDecoder::new(file));
        return Ok(StreamingReader::new(gzip_file));
    } else if extension == "bz2" {
        let reader = fs::File::open(filename)?;
        let decoder = BufReader::new(BzDecoder::new(reader));
        return Ok(StreamingReader::new(decoder));
    } else if extension == "xz" {
        let reader = fs::File::open(filename)?;
        let decoder = BufReader::new(XzDecoder::new_multi_decoder(reader));
        return Ok(StreamingReader::new(decoder));
    } else if extension == "zst" {
        let reader = fs::File::open(filename)?;
        let mut stream_decoder = zstd::stream::read::Decoder::new(reader)?;
        stream_decoder.window_log_max(ZSTD_DECODE_WINDOW_LOG_MAX)?;
        let decoder = BufReader::new(stream_decoder);
        return Ok(StreamingReader::new(decoder));
    }
    Err(anyhow!(
        "unknown file extension for file {}",
        filename.display()
    ))
}

enum ReadResult {
    Eof,
    Ok,
}

struct StreamingReader {
    buffer: String,
    result: ReadResult,
    reader: Box<dyn std::io::BufRead>,
}

impl StreamingReader {
    fn new<T: BufRead + 'static>(reader: T) -> StreamingReader {
        Self {
            buffer: String::new(),
            result: ReadResult::Ok,
            reader: Box::new(reader),
        }
    }
}

impl FallibleStreamingIterator for StreamingReader {
    type Item = str;
    type Error = std::io::Error;

    fn advance(&mut self) -> Result<(), Self::Error> {
        self.buffer.clear();
        self.result = if self.reader.read_line(&mut self.buffer)? == 0 {
            ReadResult::Eof
        } else {
            ReadResult::Ok
        };
        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        match &self.result {
            ReadResult::Eof => None,
            ReadResult::Ok => Some(self.buffer.as_str()),
        }
    }
}
