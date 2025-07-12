pub(crate) mod comment;
pub(crate) mod submission;

use serde::{Deserialize, Deserializer, de};
use strum::IntoStaticStr;

#[derive(Deserialize, Debug, Clone, Copy, IntoStaticStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub(crate) enum SubredditType {
    Public,
    Restricted,
    User,
    Archived,
    GoldRestricted,
    GoldOnly,
    Private,
}

#[derive(Debug, Clone)]
pub struct ParentId {
    pub parent_type: Option<u8>,
    pub parent_id: String,
    pub decoded_parent_id: i64,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum IdStringOrInt {
    String(String),
    Int(i64),
}

impl<'de> Deserialize<'de> for ParentId {
    fn deserialize<D>(deserializer: D) -> anyhow::Result<ParentId, D::Error>
    where
        D: Deserializer<'de>,
    {
        let parent: IdStringOrInt = Deserialize::deserialize(deserializer)?;
        Ok(match parent {
            IdStringOrInt::Int(value) => ParentId {
                parent_type: None,
                parent_id: radix_fmt::radix_36(value).to_string(),
                decoded_parent_id: value,
            },
            IdStringOrInt::String(parent) => {
                let make_err =
                    || de::Error::invalid_value(de::Unexpected::Str(&parent), &"a valid parent id");
                let (ty, parent_id) = parent.split_once('_').ok_or_else(make_err)?;
                let type_char = ty.chars().nth(1).ok_or_else(make_err)?;
                let parent_type: u8 = type_char
                    .to_digit(10)
                    .ok_or_else(make_err)?
                    .try_into()
                    .map_err(|_| make_err())?;
                let decoded_parent_id: i64 = i64::from_str_radix(parent_id, 36).map_err(|_| {
                    de::Error::invalid_value(
                        de::Unexpected::Str(parent_id),
                        &"a valid base 36 number",
                    )
                })?;
                ParentId {
                    parent_type: Some(parent_type),
                    parent_id: parent_id.into(),
                    decoded_parent_id,
                }
            }
        })
    }
}
