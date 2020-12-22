use std::path::Path;

use anyhow::Result;
use rusqlite::{params, Connection};

use crate::comment::Comment;

const SETUP: &str = include_str!("comment.sql");

pub struct Sqlite {
    connection: Connection,
}

impl Sqlite {
    pub fn new(filename: &Path) -> Result<Self> {
        let connection = Connection::open(filename).unwrap();
        connection.execute_batch(SETUP)?;
        Ok(Sqlite { connection })
    }

    pub fn insert_comment(&self, comment: &Comment) -> Result<usize> {
        let mut statement = self.connection.prepare_cached("INSERT OR IGNORE INTO comment (reddit_id, author, subreddit, body, score, created_utc, retrieved_on, parent_id, parent_is_post) \
                                                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)").unwrap();
        let res = statement.execute(params![
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
        Ok(res)
    }
}
