use crate::{
    deser::{deserialize_score, deserialize_time},
    storage::{Storable, Storage},
    Filterable,
};
use anyhow::Result;
use serde::{de, Deserialize, Deserializer};

#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
pub struct Comment {
    pub author: String,
    pub body: String,
    pub subreddit: String,
    #[serde(default)]
    pub author_flair_text: Option<String>,
    #[serde(default)]
    author_flair_css_class: Option<String>,
    #[serde(deserialize_with = "deserialize_score")]
    pub score: Option<i64>,
    pub ups: Option<i32>,
    pub downs: Option<i32>,
    #[serde(deserialize_with = "deserialize_time")]
    pub created_utc: i64,
    #[serde(default)]
    pub retrieved_on: Option<i64>,
    pub link_id: String,
    pub id: String,
    pub permalink: Option<String>,
    pub parent_id: ParentId,
    #[serde(default)]
    pub parent_is_post: bool,
    #[serde(default)]
    pub stickied: bool,
    #[serde(default)]
    is_submitter: bool,
    #[serde(default)]
    pub distinguished: Option<String>,
    //    edited: Option<Edited>,
    #[serde(default)]
    archived: bool,
    controversiality: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct ParentId {
    pub parent_type: u8,
    pub parent_id: String,
}

impl<'de> Deserialize<'de> for ParentId {
    fn deserialize<D>(deserializer: D) -> Result<ParentId, D::Error>
    where
        D: Deserializer<'de>,
    {
        let parent: String = Deserialize::deserialize(deserializer)?;
        let make_err =
            || de::Error::invalid_value(de::Unexpected::Str(&parent), &"a valid parent id");
        let (ty, parent_id) = parent.split_once('_').ok_or_else(make_err)?;
        let type_char = ty.chars().nth(1).ok_or_else(make_err)?;
        let parent_type: u8 = type_char
            .to_digit(10)
            .ok_or_else(make_err)?
            .try_into()
            .map_err(|_| make_err())?;
        Ok(ParentId {
            parent_type,
            parent_id: parent_id.into(),
        })
    }
}

impl Filterable for Comment {
    fn score(&self) -> Option<i64> {
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_deserialize() {
        let comments = include_str!("../test_data/test_comments.json");
        for line in comments.lines() {
            let _comment: Comment = serde_json::from_str(line).expect("deserialization");
        }
    }
}
