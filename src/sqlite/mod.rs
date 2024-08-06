use std::path::Path;

use anyhow::{Context, Result};
use log::info;
use rusqlite::{params, Connection, OpenFlags};

use crate::{comment::Comment, storage::Storage, submission::Submission};

const SETUP_COMMENTS: &str = include_str!("comment.sql");
const COMMENTS_FTS: &str = include_str!("comment_fts.sql");
const SETUP_SUBMISSIONS: &str = include_str!("submission.sql");
const SUBMISSIONS_FTS: &str = include_str!("submission_fts.sql");
const PRAGMA: &str = "PRAGMA journal_mode=WAL;
                      PRAGMA recursive_triggers = ON;
                      PRAGMA synchronous = NORMAL;
                      PRAGMA max_page_count = 4294967292;";

const UNSAFE_PRAGMA: &str = "PRAGMA journal_mode=MEMORY;
                             PRAGMA cache_size=-40000;
                             PRAGMA temp_store = memory;
                             PRAGMA recursive_triggers = ON;
                             PRAGMA synchronous = OFF;
                             PRAGMA max_page_count = 4294967292;
                             PRAGMA locking_mode=EXCLUSIVE;";
const TRANSACTION_SIZE: usize = 10000;

pub struct Sqlite {
    connection: Connection,
    in_transaction: usize,
}

impl Sqlite {
    pub fn new(filename: &Path, unsafe_pragma: bool, fts: bool) -> Result<Self> {
        let connection = Connection::open_with_flags(
            filename,
            OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        if unsafe_pragma {
            info!(
                "Executing in unsafe-mode. Do not interrupt as crashes will corrupt the database."
            );
            connection.execute_batch(UNSAFE_PRAGMA)?;
        } else {
            connection.execute_batch(PRAGMA)?;
        }
        connection.execute_batch(SETUP_COMMENTS)?;
        connection.execute_batch(SETUP_SUBMISSIONS)?;
        if fts {
            connection.execute_batch(COMMENTS_FTS)?;
            connection.execute_batch(SUBMISSIONS_FTS)?;
        }
        connection.execute_batch("BEGIN DEFERRED")?;
        Ok(Sqlite {
            connection,
            in_transaction: 0,
        })
    }

    fn check_transaction(&mut self) -> Result<()> {
        if self.in_transaction >= TRANSACTION_SIZE {
            self.connection.execute_batch("COMMIT")?;
            self.connection.execute_batch("BEGIN DEFERRED")?;
            self.in_transaction = 0;
        }
        Ok(())
    }
}

impl Storage for Sqlite {
    fn insert_comment(&mut self, comment: &Comment) -> Result<usize> {
        {
            let mut statement = self.connection.prepare_cached("INSERT INTO comment (reddit_id, permalink, author, subreddit, body, score, ups, downs, created_utc, retrieved_on, parent_type, parent_id, stickied, distinguished) \
                                                                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\
                                                                                 ON CONFLICT DO NOTHING").expect("cached comment statement");
            statement.execute(params![
                comment.id.as_str(),
                comment.permalink.as_deref(),
                comment.author.as_str(),
                comment.subreddit.as_str(),
                comment.body.as_str(),
                comment.score,
                comment.ups,
                comment.downs,
                comment.created_utc,
                comment.retrieved_on,
                comment.parent_id.parent_type,
                comment.parent_id.parent_id.as_str(),
                comment.stickied,
                comment.distinguished.as_deref()
            ])?;
        }
        self.in_transaction += 1;
        self.check_transaction()?;
        Ok(0)
    }

    fn insert_submission(&mut self, submission: &Submission) -> Result<usize> {
        {
            let mut statement = self.connection.prepare_cached("INSERT INTO submission (reddit_id, author, title, subreddit, selftext, permalink,\
                                                                                domain, url, score, ups, downs, created_utc, retrieved_on, is_self, over_18,\
                                                                                spoiler, pinned, stickied, num_crossposts, author_flair_text, link_flair_text) \
                                                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                                                                                ON CONFLICT DO NOTHING").expect("insert submission cached statement");
            let params = params![
                submission.id.as_str(),
                submission.author.as_deref(),
                submission.title.as_str(),
                submission.subreddit.as_deref(),
                submission.selftext.as_str(),
                submission.permalink.as_str(),
                submission.domain.as_deref(),
                submission.url.as_deref(),
                submission.score,
                submission.ups,
                submission.downs,
                submission.created_utc,
                submission.retrieved_on,
                submission.is_self,
                submission.over_18,
                submission.spoiler,
                submission.pinned,
                submission.stickied,
                submission.num_crossposts,
                submission.author_flair_text,
                submission.link_flair_text
            ];
            statement
                .execute(params)
                .with_context(|| format!("Failed to insert: {submission:#?}"))?;
        }
        self.in_transaction += 1;

        self.check_transaction()?;

        Ok(0)
    }
}

impl Drop for Sqlite {
    fn drop(&mut self) {
        if self.in_transaction > 0 {
            self.connection
                .execute_batch("COMMIT")
                .with_context(|| "Failed when dropping Sqlite struct")
                .unwrap()
        }
    }
}
