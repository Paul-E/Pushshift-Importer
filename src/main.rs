extern crate clap;
extern crate flate2;
extern crate hashbrown;
extern crate serde;
extern crate serde_json;

mod comment;
mod decompress;
mod sqlite;
mod storage;
mod submission;

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
use crate::{
    comment::Comment,
    sqlite::Sqlite,
    storage::{Storable, Storage},
    submission::Submission,
};
use bzip2::read::BzDecoder;
use clap::{App, Arg};
use xz2::read::XzDecoder;

fn main() {
    // let test = include_str!("example.json");
    // Submission::from_json_str(test);
    // println!("Test string passed");
    let matches = App::new("pushshift-importer")
        .version("0.1")
        .author("Paul Ellenbogen")
        .arg(
            Arg::with_name("sqlite-outfile")
                .required(true)
                .help("Path for for output Sqlite database.")
                .takes_value(true),
        )
        .arg(Arg::with_name("comments")
            .long("comments")
            .help("Directory where compressed json files containing comments are located")
            .takes_value(true))
        .arg(Arg::with_name("submissions")
            .long("submissions")
            .help("Directory where compressed json files containing submissions are located")
            .takes_value(true))
        .arg(Arg::with_name("filter-config")
            .long("filter-config")
            .help("File containing filter configuration")
            .takes_value(true))
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
    let filter: Arc<Filter> = Arc::new(Filter { users, subreddits });
    if let Some(comments_dir) = matches.value_of("comments") {
        let file_list = get_file_list(Path::new(comments_dir));
        process::<_, Comment>(file_list, filter.clone(), &sqlite);
    }
    if let Some(submissions_dir) = matches.value_of("submissions") {
        let file_list = get_file_list(Path::new(submissions_dir));
        process::<_, Submission>(file_list, filter, &sqlite);
    }
}

fn process<T, U>(file_list: Vec<PathBuf>, filter: Arc<Filter>, db: &T)
where
    T: Storage,
    U: Storable + FromJsonString + Filterable + Send + 'static,
{
    let shared_file_list = Arc::new(RwLock::new(file_list));
    let completed = Arc::new(AtomicUsize::new(0));
    let mut threads = Vec::new();
    let (tx, rx) = mpsc::channel();
    let num_cpus = num_cpus::get_physical();
    for _i in 0..(num_cpus - 1) {
        let filter_context = ThreadContext::new(
            filter.clone(),
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
        let maybe_content: Result<U, _> = rx.try_recv();
        match maybe_content {
            Ok(content) => {
                content.store(db).expect("Error inserting content");
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                maybe_content.unwrap();
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

struct ThreadContext<T> {
    filter: Arc<Filter>,
    queue: Arc<RwLock<Vec<PathBuf>>>,
    completed: Arc<AtomicUsize>,
    send_channel: mpsc::Sender<T>,
}

impl<T: FromJsonString + Filterable> ThreadContext<T> {
    fn new(
        filter: Arc<Filter>,
        queue: Arc<RwLock<Vec<PathBuf>>>,
        completed: Arc<AtomicUsize>,
        send_channel: mpsc::Sender<T>,
    ) -> Self {
        ThreadContext {
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
            for content in iter_lines(filename.as_path())
                .map(|line| T::from_json_str(line.as_str()))
                .filter(|content| self.filter.filter(content))
            {
                self.send_channel.send(content).unwrap();
            }
        }
        self.completed.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Default)]
struct Filter {
    users: HashSet<String>,
    subreddits: HashSet<String>,
}

impl Filter {
    fn filter<T: Filterable>(&self, content: &T) -> bool {
        if self.users.is_empty() && self.subreddits.is_empty() {
            return true;
        }
        if content
            .author()
            .map(|author| self.users.contains(author))
            .unwrap_or_default()
        {
            return true;
        }
        if self.subreddits.contains(content.subreddit()) {
            return true;
        }
        false
    }
}

fn iter_lines(filename: &Path) -> Box<dyn Iterator<Item = String>> {
    let extension = filename.extension().unwrap().to_str().unwrap();
    if extension == "gz" {
        let gzip_file = decompress::gzip_file(filename);
        let iter = gzip_file.lines().into_iter().map(io::Result::unwrap);
        return Box::new(iter);
    } else if extension == "bz2" {
        let reader = fs::File::open(filename).unwrap();
        let decoder = BufReader::new(BzDecoder::new(reader));
        let iter = decoder.lines().into_iter().map(io::Result::unwrap);
        return Box::new(iter);
    } else if extension == "xz" {
        let reader = fs::File::open(filename).unwrap();
        let decoder = BufReader::new(XzDecoder::new_multi_decoder(reader));
        let iter = decoder.lines().into_iter().map(io::Result::unwrap);
        return Box::new(iter);
    } else if extension == "zst" {
        let reader = fs::File::open(filename).unwrap();
        let decoder = BufReader::new(zstd::stream::read::Decoder::new(reader).unwrap());
        let iter = decoder.lines().into_iter().map(io::Result::unwrap);
        return Box::new(iter);
    }
    panic!("Unknown file extension for file {}", filename.display());
}

// TODO: Use a standard deserialize trait
trait FromJsonString {
    fn from_json_str(line: &str) -> Self;
}

trait Filterable {
    fn score(&self) -> i32;
    fn author(&self) -> Option<&str>;
    fn subreddit(&self) -> &str;
    fn created(&self) -> i64;
}
