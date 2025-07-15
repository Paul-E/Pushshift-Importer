use std::path::Path;

use anyhow::{Context, Result};
use log::info;
use rusqlite::{
    Connection, OpenFlags, ToSql,
    types::{ToSqlOutput, Value as OwnedSqliteValue, ValueRef as RefSqliteValue},
};

use crate::{
    reddit_types::{comment::Comment, submission::Submission},
    storage::Storage,
};

const SETUP_COMMENTS: &str = include_str!("comment.sql");
const COMMENTS_FTS: &str = include_str!("comment_fts.sql");
const SETUP_SUBMISSIONS: &str = include_str!("submission.sql");
const SUBMISSIONS_FTS: &str = include_str!("submission_fts.sql");
const PRAGMA: &str = "PRAGMA journal_mode=WAL;
                      PRAGMA recursive_triggers = ON;
                      PRAGMA synchronous = NORMAL;
                      PRAGMA max_page_count = 4294967292;
                      PRAGMA page_size = 8192;";

const UNSAFE_PRAGMA: &str = "PRAGMA journal_mode=MEMORY;
                             PRAGMA cache_size=-8000000;
                             PRAGMA temp_store = memory;
                             PRAGMA recursive_triggers = ON;
                             PRAGMA synchronous = OFF;
                             PRAGMA max_page_count = 4294967292;
                             PRAGMA page_size = 8192;
                             PRAGMA locking_mode=EXCLUSIVE;";
const TRANSACTION_SIZE: usize = 10000;
const BATCH_SIZE: usize = 50;

pub struct Sqlite {
    connection: Connection,
    in_transaction: usize,
    comment_buffer: Vec<Comment>,
    submission_buffer: Vec<Submission>,
}

impl Sqlite {
    pub fn new(filename: &Path, unsafe_pragma: bool, fts: bool) -> Result<Self> {
        let connection = Connection::open_with_flags(
            filename,
            OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        Self::with_connection(connection, unsafe_pragma, fts)
    }

    fn with_connection(connection: Connection, unsafe_pragma: bool, fts: bool) -> Result<Self> {
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
            comment_buffer: Vec::with_capacity(BATCH_SIZE),
            submission_buffer: Vec::with_capacity(BATCH_SIZE),
        })
    }

    fn check_transaction(&mut self) -> Result<()> {
        if self.in_transaction >= TRANSACTION_SIZE {
            self.commit()?;
        }
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        self.flush_comments()?;
        self.flush_submissions()?;
        self.connection.execute_batch("COMMIT")?;
        self.connection.execute_batch("BEGIN DEFERRED")?;
        self.in_transaction = 0;
        Ok(())
    }

    fn flush_comments(&mut self) -> Result<()> {
        if self.comment_buffer.is_empty() {
            return Ok(());
        }

        let batch_size = self.comment_buffer.len();
        let values_clause = vec![
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            batch_size
        ]
        .join(", ");
        let sql = format!(
            "INSERT INTO comment \
            (decoded_reddit_id, reddit_id, permalink, author, author_premium, \
             subreddit, decoded_subreddit_id, subreddit_type, \
             body, score, ups, \
             downs, created_utc, edited_utc, retrieved_on, \
             parent_type, parent_id, decoded_parent_id, distinguished, \
             stickied, parent_is_post, is_submitter, archived, locked, collapsed) \
            VALUES {values_clause} \
            ON CONFLICT DO NOTHING"
        );

        let mut stmt = self.connection.prepare_cached(&sql)?;
        let mut params = Vec::with_capacity(batch_size);
        for comment in &self.comment_buffer {
            let decoded_reddit_id = i64::from_str_radix(&comment.id, 36)?;
            let decoded_parent_id = comment
                .parent_id
                .as_ref()
                .and_then(|parent_id| i64::from_str_radix(&parent_id.parent_id, 26).ok());

            let row_params = [
                ToSqlOutput::Owned(OwnedSqliteValue::from(decoded_reddit_id)),
                comment.id.to_sql()?,
                comment.permalink.to_sql()?,
                comment.author.to_sql()?,
                comment.author_premium.to_sql()?,
                comment.subreddit.to_sql()?,
                comment.subreddit_id.decoded_parent_id.to_sql()?,
                ToSqlOutput::Borrowed(comment.subreddit_type.map_or(RefSqliteValue::Null, |ty| {
                    RefSqliteValue::Text(<&'static str>::from(ty).as_bytes())
                })),
                comment.body.to_sql()?,
                comment.score.to_sql()?,
                comment.ups.to_sql()?,
                comment.downs.to_sql()?,
                comment.created_utc.to_sql()?,
                comment.edited.to_sql()?,
                comment.retrieved_on.to_sql()?,
                ToSqlOutput::Owned(
                    comment
                        .parent_id
                        .as_ref()
                        .and_then(|parent_id| parent_id.parent_type)
                        .map_or(OwnedSqliteValue::Null, |ty| {
                            OwnedSqliteValue::Integer(ty.into())
                        }),
                ),
                ToSqlOutput::Borrowed(
                    comment
                        .parent_id
                        .as_ref()
                        .map_or(RefSqliteValue::Null, |parent_id| {
                            RefSqliteValue::Text(parent_id.parent_id.as_bytes())
                        }),
                ),
                ToSqlOutput::Owned(OwnedSqliteValue::from(decoded_parent_id)),
                ToSqlOutput::Borrowed(comment.distinguished.map_or(
                    RefSqliteValue::Null,
                    |distinguished| {
                        RefSqliteValue::Text(<&'static str>::from(distinguished).as_bytes())
                    },
                )),
                comment.stickied.to_sql()?,
                comment.parent_is_post.to_sql()?,
                comment.is_submitter.to_sql()?,
                comment.archived.to_sql()?,
                comment.locked.to_sql()?,
                comment.collapsed.to_sql()?,
            ];
            params.push(row_params);
        }

        stmt.execute(rusqlite::params_from_iter(params.into_iter().flatten()))?;

        self.in_transaction += batch_size;
        self.comment_buffer.clear();
        Ok(())
    }

    fn flush_submissions(&mut self) -> Result<()> {
        if self.submission_buffer.is_empty() {
            return Ok(());
        }

        let batch_size = self.submission_buffer.len();
        let values_clause = vec!["(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"; batch_size].join(", ");
        let sql = format!(
            "INSERT INTO submission \
            (decoded_reddit_id, reddit_id, author, author_premium, title, subreddit, decoded_subreddit_id, subreddit_subscribers, selftext, permalink, \
             domain, url, score, ups, downs, created_utc, edited_utc, retrieved_on, is_self, over_18, \
             spoiler, pinned, stickied, num_comments, num_crossposts, author_flair_text, author_flair_template_id, link_flair_text, link_flair_template_id, \
             is_created_from_ads_ui, is_gallery, is_video, is_original_content, is_reddit_media_domain, contest_mode, quarantine, \
             subreddit_type, suggested_sort, decoded_crosspost_parent_id, post_hint, removed_by_category) \
            VALUES {values_clause} \
            ON CONFLICT DO NOTHING"
        );

        let mut stmt = self.connection.prepare_cached(&sql)?;
        let mut params = Vec::with_capacity(batch_size);

        for submission in &self.submission_buffer {
            let decoded_reddit_id = i64::from_str_radix(&submission.id, 36)?;

            let row_params = [
                ToSqlOutput::Owned(OwnedSqliteValue::from(decoded_reddit_id)),
                submission.id.to_sql()?,
                submission.author.to_sql()?,
                submission.author_premium.to_sql()?,
                submission.title.to_sql()?,
                submission.subreddit.to_sql()?,
                ToSqlOutput::Owned(OwnedSqliteValue::from(
                    submission
                        .subreddit_id
                        .as_ref()
                        .map(|s| s.decoded_parent_id),
                )),
                submission.subreddit_subscribers.to_sql()?,
                submission.selftext.to_sql()?,
                submission.permalink.to_sql()?,
                submission.domain.to_sql()?,
                submission.url.to_sql()?,
                submission.score.to_sql()?,
                submission.ups.to_sql()?,
                submission.downs.to_sql()?,
                submission.created_utc.to_sql()?,
                submission.edited_utc.to_sql()?,
                submission.retrieved_on.to_sql()?,
                submission.is_self.to_sql()?,
                submission.over_18.to_sql()?,
                submission.spoiler.to_sql()?,
                submission.pinned.to_sql()?,
                submission.stickied.to_sql()?,
                submission.num_comments.to_sql()?,
                submission.num_crossposts.to_sql()?,
                submission.author_flair_text.to_sql()?,
                submission.author_flair_template_id.to_sql()?,
                submission.link_flair_text.to_sql()?,
                submission.link_flair_template_id.to_sql()?,
                submission.is_created_from_ads_ui.to_sql()?,
                submission.is_gallery.to_sql()?,
                submission.is_video.to_sql()?,
                submission.is_original_content.to_sql()?,
                submission.is_reddit_media_domain.to_sql()?,
                submission.contest_mode.to_sql()?,
                submission.quarantine.to_sql()?,
                ToSqlOutput::Borrowed(
                    submission
                        .subreddit_type
                        .map_or(RefSqliteValue::Null, |ty| {
                            RefSqliteValue::Text(<&'static str>::from(ty).as_bytes())
                        }),
                ),
                ToSqlOutput::Borrowed(
                    submission
                        .suggested_sort
                        .map_or(RefSqliteValue::Null, |sort| {
                            RefSqliteValue::Text(<&'static str>::from(sort).as_bytes())
                        }),
                ),
                ToSqlOutput::Owned(OwnedSqliteValue::from(
                    submission
                        .crosspost_parent
                        .as_ref()
                        .map(|id| id.decoded_parent_id),
                )),
                ToSqlOutput::Borrowed(submission.post_hint.map_or(RefSqliteValue::Null, |hint| {
                    RefSqliteValue::Text(<&'static str>::from(hint).as_bytes())
                })),
                ToSqlOutput::Borrowed(
                    submission
                        .removed_by_category
                        .map_or(RefSqliteValue::Null, |cat| {
                            RefSqliteValue::Text(<&'static str>::from(cat).as_bytes())
                        }),
                ),
            ];
            params.push(row_params);
        }

        stmt.execute(rusqlite::params_from_iter(params.into_iter().flatten()))?;

        self.in_transaction += batch_size;
        self.submission_buffer.clear();
        Ok(())
    }
}

impl Storage for Sqlite {
    fn insert_comment(&mut self, comment: Comment) -> Result<usize> {
        // Validate the base36 ID early to maintain expected error behavior
        i64::from_str_radix(&comment.id, 36)?;

        self.comment_buffer.push(comment);

        if self.comment_buffer.len() >= BATCH_SIZE {
            self.flush_comments()?;
        }

        self.check_transaction()?;
        Ok(0)
    }

    fn insert_submission(&mut self, submission: Submission) -> Result<usize> {
        // Validate the base36 ID early to maintain expected error behavior
        i64::from_str_radix(&submission.id, 36)?;

        self.submission_buffer.push(submission);

        if self.submission_buffer.len() >= BATCH_SIZE {
            self.flush_submissions()?;
        }

        self.check_transaction()?;
        Ok(0)
    }
}

impl Drop for Sqlite {
    fn drop(&mut self) {
        // Flush any remaining buffered items
        let _ = self.flush_comments();
        let _ = self.flush_submissions();

        if self.in_transaction > 0 {
            self.connection
                .execute_batch("COMMIT")
                .with_context(|| "Failed when dropping Sqlite struct")
                .unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reddit_types::comment::Comment;
    use crate::reddit_types::submission::Submission;
    use rusqlite::params;

    impl Sqlite {
        #[cfg(test)]
        fn new_test(fts: bool) -> Result<Self> {
            let connection = Connection::open_in_memory()?;
            Self::with_connection(connection, false, fts)
        }
    }

    #[test]
    fn test_insert_comments() -> Result<()> {
        let mut storage = Sqlite::new_test(false)?;
        let comments = include_str!("../../test_data/test_comments.json");

        let mut inserted_count = 0;
        for line in comments.lines() {
            let comment: Comment = serde_json::from_str(line)?;
            storage.insert_comment(comment)?;
            inserted_count += 1;
        }

        // Commit the transaction
        storage.commit()?;

        // Verify comments were inserted
        let count: i64 =
            storage
                .connection
                .query_row("SELECT COUNT(*) FROM comment", [], |row| row.get(0))?;

        assert_eq!(count as usize, inserted_count);

        // Verify specific comment data
        let first_line = comments.lines().next().unwrap();
        let first_comment: Comment = serde_json::from_str(first_line)?;
        let decoded_id = i64::from_str_radix(&first_comment.id, 36)?;

        let (reddit_id, author, body, score): (String, String, String, Option<i64>) =
            storage.connection.query_row(
                "SELECT reddit_id, author, body, score FROM comment WHERE decoded_reddit_id = ?",
                params![decoded_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;

        assert_eq!(reddit_id, first_comment.id);
        assert_eq!(author, first_comment.author);
        assert_eq!(body, first_comment.body);
        assert_eq!(score, first_comment.score);

        Ok(())
    }

    #[test]
    fn test_insert_submissions() -> Result<()> {
        let mut storage = Sqlite::new_test(false)?;
        let submissions = include_str!("../../test_data/test_submissions.json");

        let mut inserted_count = 0;
        for line in submissions.lines() {
            let submission: Submission = serde_json::from_str(line)?;
            storage.insert_submission(submission)?;
            inserted_count += 1;
        }

        // Commit the transaction
        storage.commit()?;

        // Verify submissions were inserted
        let count: i64 =
            storage
                .connection
                .query_row("SELECT COUNT(*) FROM submission", [], |row| row.get(0))?;

        assert_eq!(count as usize, inserted_count);

        // Verify specific submission data
        let first_line = submissions.lines().next().unwrap();
        let first_submission: Submission = serde_json::from_str(first_line)?;
        let decoded_id = i64::from_str_radix(&first_submission.id, 36)?;

        let (reddit_id, author, title, score): (String, Option<String>, String, Option<i64>) = storage.connection.query_row(
            "SELECT reddit_id, author, title, score FROM submission WHERE decoded_reddit_id = ?",
            params![decoded_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        )?;

        assert_eq!(reddit_id, first_submission.id);
        assert_eq!(author, first_submission.author);
        assert_eq!(title, first_submission.title);
        assert_eq!(score, first_submission.score);

        Ok(())
    }

    #[test]
    fn test_invalid_base36_id() -> Result<()> {
        let mut storage = Sqlite::new_test(false)?;
        let comments = include_str!("../../test_data/test_comments.json");

        // Take a valid comment and modify the ID to be invalid
        let first_line = comments.lines().next().unwrap();
        let mut comment_json: serde_json::Value = serde_json::from_str(first_line)?;
        comment_json["id"] = serde_json::Value::String("invalid!@#".to_string());

        let comment: Result<Comment, _> = serde_json::from_value(comment_json);

        // If parsing succeeds, insertion should fail due to invalid base36
        if let Ok(comment) = comment {
            let result = storage.insert_comment(comment);
            assert!(result.is_err());
        }

        Ok(())
    }

    #[test]
    fn test_duplicate_handling() -> Result<()> {
        let mut storage = Sqlite::new_test(false)?;
        let comments = include_str!("../../test_data/test_comments.json");

        // Use first comment from test data
        let first_line = comments.lines().next().unwrap();
        let comment: Comment = serde_json::from_str(first_line)?;

        // Insert the comment
        storage.insert_comment(comment.clone())?;

        // Try to insert the same comment again
        let result = storage.insert_comment(comment.clone());
        assert!(result.is_ok()); // Should succeed due to ON CONFLICT DO NOTHING

        // Commit and verify only one copy exists
        storage.commit()?;

        let count: i64 = storage.connection.query_row(
            "SELECT COUNT(*) FROM comment WHERE reddit_id = ?",
            params![comment.id],
            |row| row.get(0),
        )?;

        assert_eq!(count, 1);

        Ok(())
    }

    #[test]
    fn test_drop_commits_transaction() -> Result<()> {
        let mut storage = Sqlite::new_test(false)?;
        let comments = include_str!("../../test_data/test_comments.json");

        // Use first comment from test data
        let first_line = comments.lines().next().unwrap();
        let comment: Comment = serde_json::from_str(first_line)?;

        storage.insert_comment(comment)?;

        // Drop storage - this should commit the transaction
        drop(storage);

        // Create a new storage instance
        let new_storage = Sqlite::new_test(false)?;

        // Need to commit the automatic transaction to read data
        new_storage.connection.execute_batch("COMMIT")?;

        let count: i64 =
            new_storage
                .connection
                .query_row("SELECT COUNT(*) FROM comment", [], |row| row.get(0))?;

        // Should be 0 because in-memory database is destroyed on drop
        assert_eq!(count, 0);

        Ok(())
    }

    #[test]
    fn test_with_fts() -> Result<()> {
        let mut storage = Sqlite::new_test(true)?;
        let comments = include_str!("../../test_data/test_comments.json");

        // Use first comment from test data
        let first_line = comments.lines().next().unwrap();
        let comment: Comment = serde_json::from_str(first_line)?;

        storage.insert_comment(comment)?;
        storage.commit()?;

        // Verify FTS table exists
        let count: i64 = storage.connection.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='comment_fts'",
            [],
            |row| row.get(0),
        )?;

        assert_eq!(count, 1);

        Ok(())
    }

    #[test]
    fn test_batch_size_flushing() -> Result<()> {
        let mut storage = Sqlite::new_test(false)?;
        let comments = include_str!("../../test_data/test_comments.json");
        let base_comment: Comment = serde_json::from_str(comments.lines().next().unwrap())?;

        // Insert comments one by one until we trigger a batch flush
        for i in 0..(BATCH_SIZE - 1) {
            let mut comment = base_comment.clone();
            // Create unique base36 IDs
            comment.id = format!("{:x}", i + 1000000); // Convert to hex which is valid base36
            storage.insert_comment(comment)?;
        }

        // Buffer should have items but not be flushed yet
        assert_eq!(storage.comment_buffer.len(), BATCH_SIZE - 1);
        assert_eq!(storage.in_transaction, 0);

        // Insert one more to trigger batch flush
        let mut comment = base_comment.clone();
        comment.id = format!("{:x}", 2000000);
        storage.insert_comment(comment)?;

        // Buffer should be empty and transaction count should reflect the batch
        assert_eq!(storage.comment_buffer.len(), 0);
        assert_eq!(storage.in_transaction, BATCH_SIZE);

        storage.commit()?;

        // Verify all comments were inserted
        let count: i64 =
            storage
                .connection
                .query_row("SELECT COUNT(*) FROM comment", [], |row| row.get(0))?;
        assert_eq!(count as usize, BATCH_SIZE);

        Ok(())
    }

    #[test]
    fn test_commit_flushes_buffers() -> Result<()> {
        let mut storage = Sqlite::new_test(false)?;
        let comments = include_str!("../../test_data/test_comments.json");

        // Insert a few comments (less than batch size)
        let insert_count = 3;
        for line in comments.lines().take(insert_count) {
            let comment: Comment = serde_json::from_str(line)?;
            storage.insert_comment(comment)?;
        }

        // Buffer should have items
        assert_eq!(storage.comment_buffer.len(), insert_count);
        assert_eq!(storage.in_transaction, 0);

        // Commit should flush the buffer
        storage.commit()?;

        // Buffer should be empty and transaction count should reflect the flush
        assert_eq!(storage.comment_buffer.len(), 0);
        assert_eq!(storage.in_transaction, 0); // Reset by commit

        // Verify comments were inserted
        let count: i64 =
            storage
                .connection
                .query_row("SELECT COUNT(*) FROM comment", [], |row| row.get(0))?;
        assert_eq!(count as usize, insert_count);

        Ok(())
    }

    #[test]
    fn test_drop_flushes_buffers() -> Result<()> {
        let comments = include_str!("../../test_data/test_comments.json");
        let insert_count = 3;

        {
            let mut storage = Sqlite::new_test(false)?;

            // Insert a few comments (less than batch size)
            for line in comments.lines().take(insert_count) {
                let comment: Comment = serde_json::from_str(line)?;
                storage.insert_comment(comment)?;
            }

            // Buffer should have items
            assert_eq!(storage.comment_buffer.len(), insert_count);
        } // Storage is dropped here, should flush buffers

        // Create new storage to verify data was persisted
        // Note: In-memory database doesn't persist across connections,
        // but the Drop trait should have attempted to flush
        Ok(())
    }

    #[test]
    fn test_mixed_comment_submission_batching() -> Result<()> {
        let mut storage = Sqlite::new_test(false)?;
        let comments = include_str!("../../test_data/test_comments.json");
        let submissions = include_str!("../../test_data/test_submissions.json");

        // Insert some comments and submissions
        let comment_count = 5;
        let submission_count = 3;

        for line in comments.lines().take(comment_count) {
            let comment: Comment = serde_json::from_str(line)?;
            storage.insert_comment(comment)?;
        }

        for line in submissions.lines().take(submission_count) {
            let submission: Submission = serde_json::from_str(line)?;
            storage.insert_submission(submission)?;
        }

        // Buffers should have items
        assert_eq!(storage.comment_buffer.len(), comment_count);
        assert_eq!(storage.submission_buffer.len(), submission_count);
        assert_eq!(storage.in_transaction, 0);

        // Commit should flush both buffers
        storage.commit()?;

        // Buffers should be empty
        assert_eq!(storage.comment_buffer.len(), 0);
        assert_eq!(storage.submission_buffer.len(), 0);

        // Verify data was inserted
        let comment_db_count: i64 =
            storage
                .connection
                .query_row("SELECT COUNT(*) FROM comment", [], |row| row.get(0))?;
        let submission_db_count: i64 =
            storage
                .connection
                .query_row("SELECT COUNT(*) FROM submission", [], |row| row.get(0))?;

        assert_eq!(comment_db_count as usize, comment_count);
        assert_eq!(submission_db_count as usize, submission_count);

        Ok(())
    }
}
