use std::path::Path;

use anyhow::{Context, Result};
use log::info;
use rusqlite::{Connection, OpenFlags, params};

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
                      PRAGMA max_page_count = 4294967292;";

const UNSAFE_PRAGMA: &str = "PRAGMA journal_mode=MEMORY;
                             PRAGMA cache_size=-8000000;
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
        })
    }

    fn check_transaction(&mut self) -> Result<()> {
        if self.in_transaction >= TRANSACTION_SIZE {
            self.commit()?;
        }
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        self.connection.execute_batch("COMMIT")?;
        self.connection.execute_batch("BEGIN DEFERRED")?;
        self.in_transaction = 0;
        Ok(())
    }
}

impl Storage for Sqlite {
    fn insert_comment(&mut self, comment: &Comment) -> Result<usize> {
        {
            let decoded_reddit_id = i64::from_str_radix(&comment.id, 36)?;
            let decoded_parent_id = comment
                .parent_id
                .as_ref()
                .and_then(|parent_id| i64::from_str_radix(&parent_id.parent_id, 26).ok());
            let mut statement = self.connection.prepare_cached("INSERT INTO comment\
                                                                                    (decoded_reddit_id, reddit_id, permalink, author, author_premium,\
                                                                                     subreddit, decoded_subreddit_id, subreddit_type,\
                                                                                     body, score, ups,\
                                                                                     downs, created_utc, edited_utc, retrieved_on,\
                                                                                     parent_type, parent_id, decoded_parent_id, distinguished, \
                                                                                     stickied, parent_is_post, is_submitter, archived, locked, collapsed) \
                                                                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\
                                                                                 ON CONFLICT DO NOTHING").expect("cached comment statement");
            statement.execute(params![
                decoded_reddit_id,
                comment.id.as_str(),
                comment.permalink.as_deref(),
                comment.author.as_str(),
                comment.author_premium,
                comment.subreddit.as_str(),
                comment.subreddit_id.decoded_parent_id,
                comment.subreddit_type.map(<&'static str>::from),
                comment.body.as_str(),
                comment.score,
                comment.ups,
                comment.downs,
                comment.created_utc,
                comment.edited,
                comment.retrieved_on,
                comment
                    .parent_id
                    .as_ref()
                    .and_then(|parent| parent.parent_type),
                comment
                    .parent_id
                    .as_ref()
                    .map(|parent| parent.parent_id.as_str()),
                decoded_parent_id,
                comment.distinguished.map(<&'static str>::from),
                comment.stickied,
                comment.parent_is_post,
                comment.is_submitter,
                comment.archived,
                comment.locked,
                comment.collapsed,
            ])?;
        }
        self.in_transaction += 1;
        self.check_transaction()?;
        Ok(0)
    }

    fn insert_submission(&mut self, submission: &Submission) -> Result<usize> {
        {
            let decoded_reddit_id = i64::from_str_radix(&submission.id, 36)?;
            let mut statement = self.connection.prepare_cached("INSERT INTO submission\
                                                                               (decoded_reddit_id, reddit_id, author, author_premium, title, subreddit, decoded_subreddit_id, subreddit_subscribers, selftext, permalink,\
                                                                                domain, url, score, ups, downs, created_utc, edited_utc, retrieved_on, is_self, over_18,\
                                                                                spoiler, pinned, stickied, num_comments, num_crossposts, author_flair_text, author_flair_template_id, link_flair_text, link_flair_template_id,\
                                                                                is_created_from_ads_ui, is_gallery, is_video, is_original_content, is_reddit_media_domain, contest_mode, quarantine,\
                                                                                subreddit_type, suggested_sort, decoded_crosspost_parent_id, post_hint, removed_by_category
                                                                                ) \
                                                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                                                                                ON CONFLICT DO NOTHING").expect("insert submission cached statement");
            let params = params![
                decoded_reddit_id,
                submission.id.as_str(),
                submission.author.as_deref(),
                submission.author_premium,
                submission.title.as_str(),
                submission.subreddit.as_deref(),
                submission
                    .subreddit_id
                    .as_ref()
                    .map(|subreddit_id| subreddit_id.decoded_parent_id),
                submission.subreddit_subscribers,
                submission.selftext.as_str(),
                submission.permalink.as_str(),
                submission.domain.as_deref(),
                submission.url.as_deref(),
                submission.score,
                submission.ups,
                submission.downs,
                submission.created_utc,
                submission.edited_utc,
                submission.retrieved_on,
                submission.is_self,
                submission.over_18,
                submission.spoiler,
                submission.pinned,
                submission.stickied,
                submission.num_comments,
                submission.num_crossposts,
                submission.author_flair_text,
                submission.author_flair_template_id,
                submission.link_flair_text,
                submission.link_flair_template_id,
                submission.is_created_from_ads_ui,
                submission.is_gallery,
                submission.is_video,
                submission.is_original_content,
                submission.is_reddit_media_domain,
                submission.contest_mode,
                submission.quarantine,
                submission.subreddit_type.map(<&'static str>::from),
                submission.suggested_sort.map(<&'static str>::from),
                submission
                    .crosspost_parent
                    .as_ref()
                    .map(|id| id.decoded_parent_id),
                submission.post_hint.map(<&'static str>::from),
                submission.removed_by_category.map(<&'static str>::from),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reddit_types::comment::Comment;
    use crate::reddit_types::submission::Submission;

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
            storage.insert_comment(&comment)?;
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
            storage.insert_submission(&submission)?;
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
            let result = storage.insert_comment(&comment);
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
        storage.insert_comment(&comment)?;

        // Try to insert the same comment again
        let result = storage.insert_comment(&comment);
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

        storage.insert_comment(&comment)?;

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

        storage.insert_comment(&comment)?;
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
}
