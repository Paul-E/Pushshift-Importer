use crate::{
    Filterable,
    deser::{deserialize_optional_time, deserialize_score, deserialize_time},
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
pub struct Comment {
    pub author: String,
    pub body: String,
    pub subreddit: String,
    pub subreddit_id: ParentId,
    pub subreddit_type: Option<SubredditType>,
    #[serde(default)]
    #[expect(dead_code)]
    pub author_flair_text: Option<String>,
    #[expect(dead_code)]
    #[serde(default)]
    #[serde_as(as = "NoneAsEmptyString")]
    pub author_flair_template_id: Option<Uuid>,
    #[serde(default)]
    pub author_premium: Option<bool>,
    #[serde(default)]
    #[expect(dead_code)]
    pub author_flair_css_class: Option<String>,
    #[serde(deserialize_with = "deserialize_score")]
    pub score: Option<i64>,
    pub ups: Option<i32>,
    pub downs: Option<i32>,
    #[serde(deserialize_with = "deserialize_time")]
    pub created_utc: i64,
    #[serde(default)]
    pub retrieved_on: Option<i64>,
    #[expect(dead_code)]
    pub link_id: ParentId,
    pub id: String,
    pub permalink: Option<String>,
    #[serde(default)]
    pub parent_id: Option<ParentId>,
    #[serde(default)]
    pub parent_is_post: bool,
    #[serde(default)]
    pub stickied: bool,
    #[serde(default)]
    pub is_submitter: bool,
    #[serde(default)]
    pub distinguished: Option<Distinguished>,
    #[serde(deserialize_with = "deserialize_optional_time")]
    pub edited: Option<i64>,
    #[serde(default)]
    pub archived: bool,
    #[expect(dead_code)]
    pub controversiality: Option<i32>,
    #[serde(default)]
    pub locked: bool,
    #[serde(default)]
    pub collapsed: bool,
}

#[derive(Deserialize, Debug, Clone, Copy, IntoStaticStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub(crate) enum Distinguished {
    Admin,
    Moderator,
    Special,
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
        let comments = include_str!("../../test_data/test_comments.json");
        for line in comments.lines() {
            let _comment: Comment = serde_json::from_str(line).expect("deserialization");
        }
    }

    #[test]
    fn test_parent_id() {
        let comment_str = r#"{"all_awardings": [], "archived": false, "associated_award": null, "author": "StrongRespect6974", "author_flair_background_color": null, "author_flair_css_class": null, "author_flair_richtext": [], "author_flair_template_id": null, "author_flair_text": null, "author_flair_text_color": null, "author_flair_type": "text", "author_fullname": "t2_jnfunod9c", "author_is_blocked": false, "author_patreon_flair": false, "author_premium": false, "body": "\\ud83d\\ude06\\ud83d\\ude02", "body_sha1": "d6ad3ad001eb24bacb4c9f93e8c46e92f5182f74", "can_gild": false, "collapsed": false, "collapsed_because_crowd_control": null, "collapsed_reason": null, "collapsed_reason_code": null, "comment_type": null, "controversiality": 0, "created_utc": 1712016498, "distinguished": null, "edited": false, "gilded": 0, "gildings": {}, "id": "kxmi7jz", "is_submitter": true, "link_id": "t3_1btio9w", "locked": false, "no_follow": true, "parent_id": 45568804275, "permalink": "/r/SammysMakeup/comments/1btio9w/can_you_say_idiot/kxmi7jz/", "retrieved_on": 1712016632, "score": 1, "score_hidden": false, "send_replies": true, "stickied": false, "subreddit": "SammysMakeup", "subreddit_id": "t5_ar9yc0", "subreddit_name_prefixed": "r/SammysMakeup", "subreddit_type": "restricted", "top_awarded_type": null, "total_awards_received": 0, "treatment_tags": [], "unrepliable_reason": null, "updated_on": 1712016633}"#;
        let _comment: Comment = serde_json::from_str(comment_str).expect("deserialization");
    }
}
