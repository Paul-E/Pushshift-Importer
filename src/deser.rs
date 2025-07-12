use serde::{Deserialize, Deserializer};
use serde_json::Value;

pub(crate) fn deserialize_time<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let json = Value::deserialize(deserializer)?;
    match json {
        Value::Number(val) => {
            if let Some(time) = val.as_i64() {
                Ok(time)
            } else if let Some(time) = val.as_u64() {
                time.try_into().map_err(serde::de::Error::custom)
            } else if let Some(time) = val.as_f64() {
                Ok(time.round() as i64)
            } else {
                Err(serde::de::Error::custom(format!(
                    "invalid timestamp value {val}"
                )))
            }
        }
        Value::String(val) => {
            let ret: i64 = val.parse().map_err(serde::de::Error::custom)?;
            Ok(ret)
        }
        _ => Err(serde::de::Error::custom("invalid timestamp value")),
    }
}

pub(crate) fn deserialize_optional_time<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let json = Value::deserialize(deserializer)?;
    match json {
        Value::Number(val) => {
            if let Some(time) = val.as_i64() {
                Ok(Some(time))
            } else if let Some(time) = val.as_u64() {
                Ok(Some(time.try_into().map_err(serde::de::Error::custom)?))
            } else if let Some(time) = val.as_f64() {
                Ok(Some(time.round() as i64))
            } else {
                Err(serde::de::Error::custom(format!(
                    "invalid timestamp value {val}"
                )))
            }
        }
        Value::String(val) => {
            let ret: i64 = val.parse().map_err(serde::de::Error::custom)?;
            Ok(Some(ret))
        }
        Value::Null => Ok(None),
        Value::Bool(_) => Ok(None),
        _ => Err(serde::de::Error::custom("invalid timestamp value")),
    }
}

pub(crate) fn deserialize_score<'de, D>(deserializer: D) -> anyhow::Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let json = serde_json::Value::deserialize(deserializer)?;
    match json {
        Value::Number(val) => {
            let score = val
                .as_i64()
                .ok_or_else(|| serde::de::Error::custom(format!("invalid score value {val}")))?;
            Ok(Some(score))
        }
        Value::String(val) => {
            let ret: i64 = val
                .parse()
                .map_err(|_| serde::de::Error::custom(format!("unable to parse score: {val}")))?;
            Ok(Some(ret))
        }
        Value::Null => Ok(None),
        _ => Err(serde::de::Error::custom("invalid timestamp value")),
    }
}
