use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use xz2::read::XzDecoder;

// represents the maximum distance as calculated by 2^log_distance for a decode window in zstd.
const ZSTD_DECODE_WINDOW_LOG_MAX: u32 = 31;

pub fn iter_lines(filename: &Path) -> Result<Box<dyn Iterator<Item = String>>> {
    let extension = filename
        .extension()
        .and_then(|extension| extension.to_str())
        .ok_or_else(|| anyhow!("cannot the file extension for {}", filename.display()))?;
    let filename_owned = filename.to_path_buf();
    if extension == "gz" {
        let file = File::open(filename)?;
        let gzip_file = BufReader::new(GzDecoder::new(file));
        let iter = gzip_file.lines().map(move |line| {
            line.with_context(|| format!("failed to read file {}", filename_owned.display()))
                .unwrap()
        });
        return Ok(Box::new(iter));
    } else if extension == "bz2" {
        let reader = fs::File::open(filename)?;
        let decoder = BufReader::new(BzDecoder::new(reader));
        let iter = decoder.lines().map(move |line| {
            line.with_context(|| format!("failed to read file {}", filename_owned.display()))
                .unwrap()
        });
        return Ok(Box::new(iter));
    } else if extension == "xz" {
        let reader = fs::File::open(filename)?;
        let decoder = BufReader::new(XzDecoder::new_multi_decoder(reader));
        let iter = decoder.lines().map(move |line| {
            line.with_context(|| format!("failed to read file {}", filename_owned.display()))
                .unwrap()
        });
        return Ok(Box::new(iter));
    } else if extension == "zst" {
        let reader = fs::File::open(filename)?;
        let mut stream_decoder = zstd::stream::read::Decoder::new(reader)?;
        stream_decoder.window_log_max(ZSTD_DECODE_WINDOW_LOG_MAX)?;
        let decoder = BufReader::new(stream_decoder);
        let iter = decoder.lines().map(move |line| {
            line.with_context(|| format!("failed to read file {}", filename_owned.display()))
                .unwrap()
        });
        return Ok(Box::new(iter));
    }
    Err(anyhow!(
        "unknown file extension for file {}",
        filename.display()
    ))
}
