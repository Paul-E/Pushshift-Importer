use crate::{
    storage::{Storable, Storage},
    Filterable, FromJsonString,
};
use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct Submission {
    pub author: Option<String>,
    pub url: String,
    pub permalink: String,
    pub score: i32,
    pub title: String,
    pub selftext: String,
    pub domain: String,
    pub author_flair_text: Option<String>,
    pub subreddit: String,
    pub subreddit_id: String,
    pub id: String,
    pub num_comments: i32,
    pub over_18: bool,
    pub is_self: bool,
    pub link_flair_text: Option<String>,
    pub spoiler: Option<bool>,
    pub pinned: Option<bool>,
    pub stickied: Option<bool>,
    pub num_crossposts: Option<u32>,
    pub ups: Option<i32>,
    pub downs: Option<i32>,
    created: Option<i64>,
    pub created_utc: i64,
    pub retrieved_on: Option<i64>,
}

impl FromJsonString for Submission {
    fn from_json_str(line: &str) -> Self {
        serde_json::from_str(line.trim_matches(char::from(0)))
            .with_context(|| format!("Failed to deserialize line: {}", line))
            .unwrap()
    }
}

impl Filterable for Submission {
    fn score(&self) -> i32 {
        self.score
    }
    fn author(&self) -> Option<&str> {
        self.author.as_deref()
    }
    fn subreddit(&self) -> &str {
        self.subreddit.as_str()
    }
    fn created(&self) -> i64 {
        self.created_utc
    }
}

impl Storable for Submission {
    fn store<T: Storage>(&self, storage: &T) -> Result<usize> {
        storage.insert_submission(&self)
    }
}
