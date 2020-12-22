extern crate clap;
extern crate flate2;
extern crate hashbrown;
extern crate serde;
extern crate serde_json;

mod comment;
mod decompress;
mod sqlite;

use std::{
    fs,
    io::{self, BufRead, BufReader},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, RwLock,
    },
    thread, time,
};

use crate::hashbrown::HashSet;
use crate::sqlite::Sqlite;
use bzip2::read::BzDecoder;
use clap::{App, Arg};
use xz2::read::XzDecoder;

use comment::Comment;

fn main() {
    let matches = App::new("pushshift-importer")
        .version("0.1")
        .author("Paul Ellenbogen")
        .arg(
            Arg::with_name("input-dir")
                .required(true)
                .help("Directory where compressed json files containing comments are located")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("sqlite-outfile")
                .required(true)
                .help("Path for for output Sqlite database.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("username")
                .long("username")
                .required(false)
                .multiple(true)
                .takes_value(true)
                .help("Add a username to the username filter"),
        )
        .arg(
            Arg::with_name("subreddit")
                .long("subreddit")
                .required(false)
                .multiple(true)
                .takes_value(true)
                .help("Add a subreddit to the subreddit filter"),
        )
        .about("Import data from pushshift dump into a Sqlite database. Currently limited to comment data only.\
        Multiple filters can be applied, and if any of the filter criteria match, the comment is included. If no filters are supplied, all comments match; ie the whole dataset will be added to the sqlite file.")
        .get_matches();
    let users: HashSet<String> = matches
        .values_of("username")
        .map(|users| users.map(|user| user.to_string()).collect())
        .unwrap_or_else(HashSet::new);
    let subreddits: HashSet<String> = matches
        .values_of("subreddit")
        .map(|users| users.map(|user| user.to_string()).collect())
        .unwrap_or_else(HashSet::new);
    let sqlite_filename = Path::new(matches.value_of("sqlite-outfile").unwrap());
    let sqlite = Sqlite::new(sqlite_filename).expect("Error setting up sqlite DB");
    let filter: CommentFilter = CommentFilter { users, subreddits };
    let input_dir = Path::new(matches.value_of("input-dir").unwrap());
    let file_list = get_file_list(input_dir);
    process(file_list, filter, sqlite);
}

fn process(file_list: Vec<PathBuf>, filter: CommentFilter, db: Sqlite) {
    let shared_file_list = Arc::new(RwLock::new(file_list));
    let shared_filter = Arc::new(filter);
    let completed = Arc::new(AtomicUsize::new(0));
    let mut threads = Vec::new();
    let (tx, rx) = mpsc::channel();
    let num_cpus = num_cpus::get_physical();
    for _i in 0..(num_cpus - 1) {
        let filter_context = FilterContext::new(
            shared_filter.clone(),
            shared_file_list.clone(),
            completed.clone(),
            tx.clone(),
        );
        let thread = thread::spawn(move || {
            filter_context.process_queue();
        });
        threads.push(thread);
    }

    loop {
        let maybe_comment = rx.try_recv();
        match maybe_comment {
            Ok(comment) => {
                db.insert_comment(&comment)
                    .expect("Error inserting comment");
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                maybe_comment.unwrap();
            }
            Err(mpsc::TryRecvError::Empty) => {
                if completed.load(Ordering::Relaxed) < (num_cpus - 1) {
                    thread::sleep(time::Duration::from_secs(1));
                } else {
                    break;
                }
            }
        }
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

fn get_file_list(dir: &Path) -> Vec<PathBuf> {
    fs::read_dir(dir)
        .unwrap()
        .into_iter()
        .filter_map(|dir_entry| dir_entry.ok().map(|ent| ent.path()))
        .collect()
}

struct FilterContext {
    filter: Arc<CommentFilter>,
    queue: Arc<RwLock<Vec<PathBuf>>>,
    completed: Arc<AtomicUsize>,
    send_channel: mpsc::Sender<comment::Comment>,
}

impl FilterContext {
    fn new(
        filter: Arc<CommentFilter>,
        queue: Arc<RwLock<Vec<PathBuf>>>,
        completed: Arc<AtomicUsize>,
        send_channel: mpsc::Sender<comment::Comment>,
    ) -> Self {
        FilterContext {
            filter,
            queue,
            completed,
            send_channel,
        }
    }

    fn get_next_file(&self) -> Option<PathBuf> {
        let mut queue = self.queue.write().unwrap();
        queue.pop()
    }

    fn process_queue(&self) {
        while let Some(filename) = self.get_next_file() {
            for comment in
                iter_comments(filename.as_path()).filter(|comment| self.filter.filter(comment))
            {
                self.send_channel.send(comment).unwrap();
            }
        }
        self.completed.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Default)]
struct CommentFilter {
    users: HashSet<String>,
    subreddits: HashSet<String>,
}

impl CommentFilter {
    fn filter(&self, comment: &comment::Comment) -> bool {
        if self.users.is_empty() && self.subreddits.is_empty() {
            return true;
        }
        if self.users.contains(comment.author.as_str()) {
            return true;
        }
        if self.subreddits.contains(comment.subreddit.as_str()) {
            return true;
        }
        false
    }
}

fn deserialize_lines(line: io::Result<String>) -> comment::Comment {
    let line = line.unwrap();
    Comment::from_json_str(line.as_str())
}

fn iter_comments(filename: &Path) -> Box<dyn Iterator<Item = comment::Comment>> {
    let extension = filename.extension().unwrap().to_str().unwrap();
    if extension == "gz" {
        let gzip_file = decompress::gzip_file(filename);
        let iter = gzip_file.lines().into_iter().map(deserialize_lines);
        return Box::new(iter);
    } else if extension == "bz2" {
        let reader = fs::File::open(filename).unwrap();
        let decoder = BufReader::new(BzDecoder::new(reader));
        let iter = decoder.lines().into_iter().map(deserialize_lines);
        return Box::new(iter);
    } else if extension == "xz" {
        let reader = fs::File::open(filename).unwrap();
        let decoder = BufReader::new(XzDecoder::new_multi_decoder(reader));
        let iter = decoder.lines().into_iter().map(deserialize_lines);
        return Box::new(iter);
    } else if extension == "zst" {
        let reader = fs::File::open(filename).unwrap();
        let decoder = BufReader::new(zstd::stream::read::Decoder::new(reader).unwrap());
        let iter = decoder.lines().into_iter().map(deserialize_lines);
        return Box::new(iter);
    }
    panic!("Unknown file extension for file {}", filename.display());
}
