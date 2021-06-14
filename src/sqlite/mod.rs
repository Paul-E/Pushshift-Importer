use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{params, Connection};

use crate::{comment::Comment, storage::Storage, submission::Submission};

const SETUP_COMMENTS: &str = include_str!("comment.sql");
const SETUP_SUBMISSIONS: &str = include_str!("submission.sql");

pub struct Sqlite {
    connection: Connection,
    in_transaction: usize,
}

impl Sqlite {
    pub fn new(filename: &Path) -> Result<Self> {
        let connection = Connection::open(filename).unwrap();
        connection.execute_batch(SETUP_COMMENTS)?;
        connection.execute_batch(SETUP_SUBMISSIONS)?;
        connection.execute_batch("BEGIN DEFERRED")?;
        Ok(Sqlite {
            connection,
            in_transaction: 0,
        })
    }

    fn check_transaction(&mut self) -> Result<()> {
        if self.in_transaction >= 500 {
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
            let mut statement = self.connection.prepare_cached("INSERT OR IGNORE INTO comment (reddit_id, author, subreddit, body, score, created_utc, retrieved_on, parent_id, parent_is_post) \
                                                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)").unwrap();
            statement.execute(params![
                comment.id.as_str(),
                comment.author.as_str(),
                comment.subreddit.as_str(),
                comment.body.as_str(),
                comment.score,
                comment.created_utc,
                comment.retrieved_on,
                comment.parent_id.as_str(),
                comment.parent_is_post
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
                                                                                spoiler, stickied, num_crossposts) \
                                                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                                                                                ON CONFLICT DO NOTHING").unwrap();
            let params = params![
                submission.subreddit_id.as_str(),
                submission.author.as_deref(),
                submission.title.as_str(),
                submission.subreddit.as_str(),
                submission.selftext.as_str(),
                submission.permalink.as_str(),
                submission.domain.as_str(),
                submission.url.as_str(),
                submission.score,
                submission.ups,
                submission.downs,
                submission.created_utc,
                submission.retrieved_on,
                submission.is_self,
                submission.over_18,
                submission.spoiler,
                submission.stickied,
                submission.num_crossposts
            ];
            statement
                .execute(params)
                .with_context(|| format!("Failed to insert: {:#?}", submission))?;
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
