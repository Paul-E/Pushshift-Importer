use crate::{
    Filterable,
    deser::{deserialize_optional_time, deserialize_time},
    reddit_types::{ParentId, SubredditType},
    storage::{Storable, Storage},
};
use anyhow::Result;
use serde::Deserialize;
use serde_with::{NoneAsEmptyString, serde_as};
use strum::IntoStaticStr;
use uuid::Uuid;

#[serde_as]
#[derive(Deserialize, Debug, Clone)]
pub struct Submission {
    pub author: Option<String>,
    pub author_premium: Option<bool>,
    pub url: Option<String>,
    pub permalink: String,
    pub score: Option<i64>,
    pub title: String,
    pub selftext: String,
    pub domain: Option<String>,
    pub author_flair_text: Option<String>,
    #[serde(default)]
    #[serde_as(as = "NoneAsEmptyString")]
    pub author_flair_template_id: Option<Uuid>,
    pub subreddit: Option<String>,
    pub subreddit_id: Option<ParentId>,
    pub id: String,
    pub num_comments: i32,
    pub over_18: bool,
    pub is_self: bool,
    #[serde(default)]
    #[serde_as(as = "NoneAsEmptyString")]
    pub link_flair_template_id: Option<Uuid>,
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
    #[serde(default, deserialize_with = "deserialize_optional_time")]
    pub edited_utc: Option<i64>,
    pub retrieved_on: Option<i64>,
    #[serde(default)]
    pub is_created_from_ads_ui: bool,
    #[serde(default)]
    pub is_gallery: bool,
    #[serde(default)]
    pub is_video: bool,
    pub is_original_content: Option<bool>,
    #[serde(default)]
    pub is_reddit_media_domain: bool,
    #[expect(dead_code)]
    pub gallery_data: Option<serde_json::Value>,
    #[expect(dead_code)]
    pub media_metadata: Option<serde_json::Value>,
    #[expect(dead_code)]
    pub url_overridden_by_dest: Option<String>,
    pub quarantine: Option<bool>,
    pub subreddit_subscribers: Option<i64>,
    #[serde(default)]
    pub contest_mode: bool,
    pub suggested_sort: Option<SuggestedSort>,
    pub crosspost_parent: Option<ParentId>,
    pub post_hint: Option<PostHint>,
    pub removed_by_category: Option<RemovedByCategory>,
    pub subreddit_type: Option<SubredditType>,

    // Whitelist status stuff
    #[expect(dead_code)]
    pub wls: Option<i8>,
    #[expect(dead_code)]
    pub whitelist_status: Option<String>,
}

#[derive(Deserialize, Debug, Clone, Copy, IntoStaticStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub(crate) enum SuggestedSort {
    Confidence,
    Controversial,
    Live,
    New,
    Old,
    Qa,
    Random,
    Top,
}

#[derive(Deserialize, Debug, Clone, Copy, IntoStaticStr)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "snake_case")]
pub(crate) enum PostHint {
    Gallery,
    #[serde(rename = "hosted:video")]
    HostedVideo,
    Image,
    Link,
    #[serde(rename = "rich:video")]
    RichVideo,
    #[serde(rename = "self")]
    Slf,
    Video,
}

#[derive(Deserialize, Debug, Clone, Copy, IntoStaticStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub(crate) enum RemovedByCategory {
    Author,
    AutomodFiltered,
    ContentTakedown,
    Deleted,
    Moderator,
    Reddit,
    AntiEvilOps,
    CommunityOps,
    CopyrightTakedown,
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
        let submissions = include_str!("../../test_data/test_submissions.json");
        for line in submissions.lines() {
            let _comment: Submission = serde_json::from_str(line).expect("deserialization");
        }
    }
}
