use crate::{
    Filterable,
    deser::deserialize_time,
    storage::{Storable, Storage},
};
use anyhow::Result;
use serde::Deserialize;

#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
pub struct Submission {
    pub author: Option<String>,
    pub url: Option<String>,
    pub permalink: String,
    pub score: Option<i64>,
    pub title: String,
    pub selftext: String,
    pub domain: Option<String>,
    pub author_flair_text: Option<String>,
    pub subreddit: Option<String>,
    pub subreddit_id: Option<String>,
    pub id: String,
    pub num_comments: i32,
    pub over_18: bool,
    pub is_self: bool,
    pub link_flair_text: Option<String>,
    pub spoiler: Option<bool>,
    pub pinned: Option<bool>,
    #[serde(default)]
    pub stickied: bool,
    pub num_crossposts: Option<u32>,
    pub ups: Option<i32>,
    pub downs: Option<i32>,
    #[serde(deserialize_with = "deserialize_time")]
    pub created_utc: i64,
    pub retrieved_on: Option<i64>,
}

impl Filterable for Submission {
    fn score(&self) -> Option<i64> {
        self.score
    }
    fn author(&self) -> Option<&str> {
        self.author.as_deref()
    }
    fn subreddit(&self) -> Option<&str> {
        self.subreddit.as_deref()
    }
    fn created(&self) -> i64 {
        self.created_utc
    }
}

impl Storable for Submission {
    fn store<T: Storage>(&self, storage: &mut T) -> Result<usize> {
        storage.insert_submission(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_deserialize() {
        let submissions = include_str!("../test_data/test_submissions.json");
        for line in submissions.lines() {
            let _comment: Submission = serde_json::from_str(line).expect("deserialization");
        }
    }
}
