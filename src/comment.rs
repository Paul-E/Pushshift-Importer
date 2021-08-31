use crate::{
    storage::{Storable, Storage},
    Filterable, FromJsonString,
};
use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct Comment {
    pub author: String,
    pub body: String,
    pub subreddit: String,
    #[serde(default)]
    pub author_flair_text: Option<String>,
    #[serde(default)]
    author_flair_css_class: Option<String>,
    pub score: Option<i32>,
    ups: Option<i32>,
    downs: Option<i32>,
    pub created_utc: i64,
    #[serde(default)]
    pub retrieved_on: Option<i64>,
    pub link_id: String,
    pub id: String,
    pub parent_id: String,
    #[serde(default)]
    pub parent_is_post: bool,
    #[serde(default)]
    stickied: bool,
    #[serde(default)]
    distinguished: Option<String>,
    //    edited: Option<Edited>,
    #[serde(default)]
    archived: bool,
    controversiality: Option<i32>,
}

impl FromJsonString for Comment {
    fn from_json_str(line: &str) -> Result<Self> {
        let mut json: serde_json::Value = serde_json::from_str(line)
            .with_context(|| format!("Failed to read json for line: {}", line))?;
        if let Some(created) = json.get_mut("created_utc") {
            if let serde_json::Value::String(utc_string) = created {
                let utc: u64 = utc_string.parse()?;
                *created = utc.into();
            }
        }
        if let Some(score) = json.get_mut("score") {
            if matches!(score, serde_json::Value::Null) {
                *score = 0.into()
            }
        }
        let mut comment = Comment::deserialize(json)
            .with_context(|| format!("Failed to deserialize line: {}", line))?;

        if comment.parent_id.starts_with("t3_") {
            comment.parent_is_post = true;
        }
        comment.parent_id = comment.parent_id.split_off(2);
        Ok(comment)
    }
}

impl Filterable for Comment {
    fn score(&self) -> Option<i32> {
        self.score
    }
    fn author(&self) -> Option<&str> {
        Some(self.author.as_str())
    }
    fn subreddit(&self) -> Option<&str> {
        Some(self.subreddit.as_str())
    }
    fn created(&self) -> i64 {
        self.created_utc
    }
}

impl Storable for Comment {
    fn store<T: Storage>(&self, storage: &mut T) -> Result<usize> {
        storage.insert_comment(self)
    }
}
