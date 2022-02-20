extern crate chrono;
extern crate clap;
extern crate flate2;
extern crate hashbrown;
extern crate serde;
extern crate serde_json;

use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, RwLock,
    },
    thread, time,
};

use anyhow::Result;
use clap::{App, Arg};
use log::{error, info, warn};
use simple_logger::SimpleLogger;

use crate::{
    comment::Comment,
    filter::{date_format_validator, Filter, Filterable},
    sqlite::Sqlite,
    storage::{Storable, Storage},
    submission::Submission,
};

mod comment;
mod decompress;
mod filter;
mod sqlite;
mod storage;
mod submission;

fn main() {
    SimpleLogger::new().init().unwrap();
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
        .arg(
            Arg::with_name("min-score")
                .long("min-score")
                .required(false)
                .takes_value(true)
                .help("Only include content with this score or higher"),
        )
        .arg(
            Arg::with_name("max-score")
                .long("max-score")
                .required(false)
                .takes_value(true)
                .help("Only include content with this score or lower"),
        )
        .arg(
            Arg::with_name("min-datetime")
                .long("min-datetime")
                .required(false)
                .takes_value(true)
                .validator(date_format_validator)
                .help("Only include content created at or after this date"),
        )
        .arg(
            Arg::with_name("max-datetime")
                .long("max-datetime")
                .required(false)
                .takes_value(true)
                .validator(date_format_validator)
                .help("Only include content created at or before this date"),
        )
        .arg(Arg::with_name("unsafe-mode")
            .long("unsafe-mode")
            .required(false)
            .takes_value(false)
            .help("Store some database structures in memory, improving performance at the const of durability. Errors will cause database corruption. This flag is used for testing."))
        .arg(Arg::with_name("enable-fts")
            .long("enable-fts")
            .required(false)
            .takes_value(false)
            .help("Enable full text search features. Creates a larger database and takes longer to run."))
        .about("Import data from pushshift dump into a Sqlite database. Currently limited to comment data only.\
        Multiple filters can be applied, and if any of the filter criteria match, the comment is included. If no filters are supplied, all comments match; ie the whole dataset will be added to the sqlite file.")
        .get_matches();
    let sqlite_filename = Path::new(matches.value_of("sqlite-outfile").unwrap());
    let mut sqlite = Sqlite::new(
        sqlite_filename,
        matches.is_present("unsafe-mode"),
        matches.is_present("enable-fts"),
    )
    .expect("Error setting up sqlite DB");
    let filter: Arc<Filter> = Arc::new(Filter::from_args(&matches));
    if let Some(comments_dir) = matches.value_of("comments") {
        let file_list = get_file_list(Path::new(comments_dir));
        info!("Processing comments");
        process::<_, Comment>(file_list, filter.clone(), &mut sqlite);
    }
    if let Some(submissions_dir) = matches.value_of("submissions") {
        let file_list = get_file_list(Path::new(submissions_dir));
        info!("Processing submissions");
        process::<_, Submission>(file_list, filter, &mut sqlite);
    }
}

fn process<T, U>(file_list: Vec<PathBuf>, filter: Arc<Filter>, db: &mut T)
where
    T: Storage,
    U: Storable + FromJsonString + Filterable + Send + 'static,
{
    let shared_file_list = Arc::new(RwLock::new(file_list));
    let completed = Arc::new(AtomicUsize::new(0));
    let mut threads = Vec::new();
    let (tx, rx) = mpsc::sync_channel(10000);
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

    let mut count: usize = 0;

    loop {
        let maybe_content: Result<U, _> = rx.try_recv();
        match maybe_content {
            Ok(content) => {
                content.store(db).expect("Error inserting content");
                count += 1;
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

    info!("Processed {} items", count);

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
    send_channel: mpsc::SyncSender<T>,
}

impl<T: FromJsonString + Filterable> ThreadContext<T> {
    fn new(
        filter: Arc<Filter>,
        queue: Arc<RwLock<Vec<PathBuf>>>,
        completed: Arc<AtomicUsize>,
        send_channel: mpsc::SyncSender<T>,
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
            let lines = match decompress::iter_lines(filename.as_path()) {
                Ok(l) => l,
                Err(e) => {
                    warn!("Error encountered in input file: {:#}. Skipping file", e);
                    continue;
                }
            };

            let item_iterator = lines
                .map(|line| T::from_json_str(line.as_str()))
                .filter_map(|maybe_content| {
                    maybe_content
                        .map_err(|err| {
                            error!("Error parsing content: {:#?}", err);
                            err
                        })
                        .ok()
                })
                .filter(|content| self.filter.filter(content));
            for content in item_iterator {
                self.send_channel.send(content).unwrap();
            }
        }
        self.completed.fetch_add(1, Ordering::Relaxed);
    }
}

// TODO: Use a standard deserialize trait
trait FromJsonString {
    fn from_json_str(line: &str) -> Result<Self>
    where
        Self: Sized;
}
