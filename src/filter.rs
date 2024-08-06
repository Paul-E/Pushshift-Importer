use crate::hashbrown::HashSet;
use chrono::NaiveDateTime;
use clap::ArgMatches;
use log::warn;

const DATE_FORMAT: &str = "%Y-%m-%d-%H:%M:%S";

pub trait Filterable {
    fn score(&self) -> Option<i64>;
    fn author(&self) -> Option<&str>;
    fn subreddit(&self) -> Option<&str>;
    fn created(&self) -> i64;
}

#[derive(Debug, Clone, Default)]
pub struct Filter {
    users: HashSet<String>,
    subreddits: HashSet<String>,
    min_score: Option<i64>,
    max_score: Option<i64>,
    min_date: Option<i64>,
    max_date: Option<i64>,
}

impl Filter {
    pub fn filter<T: Filterable>(&self, content: &T) -> bool {
        match (self.min_score, content.score()) {
            (Some(min_score), Some(content_score)) if content_score < min_score => {
                return false;
            }
            _ => (),
        }

        match (self.max_score, content.score()) {
            (Some(max_score), Some(content_score)) if max_score < content_score => {
                return false;
            }
            _ => (),
        }

        if Some(true) == self.min_date.map(|min_date| content.created() < min_date) {
            return false;
        }

        if Some(true) == self.max_date.map(|max_date| max_date < content.created()) {
            return false;
        }

        if self.users.is_empty() && self.subreddits.is_empty() {
            return true;
        }
        if content
            .author()
            .map(|author| self.users.contains(author))
            .unwrap_or_default()
        {
            return true;
        }
        if content
            .subreddit()
            .map(|subreddit| self.subreddits.contains(subreddit))
            .unwrap_or_default()
        {
            return true;
        }
        false
    }

    pub fn from_args(matches: &ArgMatches) -> Self {
        let users: HashSet<String> = matches
            .values_of("username")
            .map(|users| users.map(|user| user.to_string()).collect())
            .unwrap_or_default();
        let subreddits: HashSet<String> = matches
            .values_of("subreddit")
            .map(|users| users.map(|user| user.to_string()).collect())
            .unwrap_or_default();
        let min_score = matches
            .value_of("min-score")
            .map(|min_score| min_score.parse().expect("expected integer for min-score"));
        let max_score = matches
            .value_of("max-score")
            .map(|max_score| max_score.parse().expect("expected integer for max-score"));
        match (min_score, max_score) {
            (Some(min), Some(max)) if max < min => {
                warn!("max-score < min-score, only posts with no score will be stored")
            }
            _ => (),
        };
        let min_date = matches.value_of("min-datetime").map(|min_date| {
            NaiveDateTime::parse_from_str(min_date, DATE_FORMAT)
                .expect("expected valid date")
                .and_utc()
                .timestamp()
        });
        let max_date = matches.value_of("max-datetime").map(|max_date| {
            NaiveDateTime::parse_from_str(max_date, DATE_FORMAT)
                .expect("expected valid date")
                .and_utc()
                .timestamp()
        });

        match (min_date, max_date) {
            (Some(min), Some(max)) if max < min => {
                warn!("max-datetime < min-datetime, only posts with no date will be stored")
            }
            _ => (),
        };
        Filter {
            users,
            subreddits,
            min_score,
            max_score,
            min_date,
            max_date,
        }
    }
}

pub fn date_format_validator(date: String) -> Result<(), String> {
    NaiveDateTime::parse_from_str(&date, DATE_FORMAT).map_err(|_err| {
        format!(
            "unable to parse date {}, expected string in format {}. eg 2015-09-05-23:56:04",
            &date, DATE_FORMAT
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Default)]
    struct ContentMock {
        pub score: Option<i32>,
        pub author: Option<String>,
        pub subreddit: Option<String>,
        pub created: i64,
    }

    impl Filterable for ContentMock {
        fn score(&self) -> Option<i32> {
            self.score
        }
        fn author(&self) -> Option<&str> {
            self.author.as_deref()
        }
        fn subreddit(&self) -> Option<&str> {
            self.subreddit.as_deref()
        }
        fn created(&self) -> i64 {
            self.created
        }
    }

    fn set_subreddits(filter: &mut Filter, subreddits: &[&str]) {
        filter.subreddits = subreddits.iter().map(|sub| sub.to_string()).collect();
    }

    fn set_authors(filter: &mut Filter, authors: &[&str]) {
        filter.users = authors.iter().map(|sub| sub.to_string()).collect();
    }

    #[test]
    fn test_subreddit() {
        let mut filter = Filter::default();
        let content = ContentMock {
            subreddit: Some("test".into()),
            ..Default::default()
        };
        assert!(filter.filter(&content));

        set_subreddits(&mut filter, &["test"]);
        assert!(filter.filter(&content));

        set_subreddits(&mut filter, &["test", "test2"]);
        assert!(filter.filter(&content));

        set_subreddits(&mut filter, &["test2"]);
        assert!(!filter.filter(&content));
    }

    #[test]
    fn test_authors() {
        let mut filter = Filter::default();
        let content = ContentMock {
            author: Some("test".into()),
            ..Default::default()
        };
        assert!(filter.filter(&content));

        set_authors(&mut filter, &["test"]);
        assert!(filter.filter(&content));

        set_authors(&mut filter, &["test", "test2"]);
        assert!(filter.filter(&content));

        set_authors(&mut filter, &["test2"]);
        assert!(!filter.filter(&content));
    }

    #[test]
    fn test_score() {
        let mut filter = Filter::default();
        let mut content = ContentMock {
            score: None,
            ..Default::default()
        };

        assert!(filter.filter(&content));

        filter.min_score = Some(5);
        assert!(filter.filter(&content));

        content.score = Some(10);
        assert!(filter.filter(&content));

        content.score = Some(1);
        assert!(!filter.filter(&content));

        filter.max_score = Some(10);
        filter.min_score = None;
        assert!(filter.filter(&content));

        content.score = Some(10);
        assert!(filter.filter(&content));
    }

    #[test]
    fn test_date() {
        let mut filter = Filter::default();
        let mut content = ContentMock {
            created: 100,
            ..Default::default()
        };

        assert!(filter.filter(&content));

        filter.min_date = Some(5);
        assert!(filter.filter(&content));

        content.created = 10;
        assert!(filter.filter(&content));

        content.created = 1;
        assert!(!filter.filter(&content));

        filter.max_date = Some(10);
        filter.min_date = None;
        assert!(filter.filter(&content));

        content.created = 10;
        assert!(filter.filter(&content));
    }

    #[test]
    fn test_composite() {
        let mut filter = Filter {
            min_score: Some(5),
            ..Default::default()
        };
        let content = ContentMock {
            subreddit: Some("test".into()),
            score: Some(10),
            ..Default::default()
        };
        assert!(filter.filter(&content));

        set_subreddits(&mut filter, &["test"]);
        assert!(filter.filter(&content));

        set_subreddits(&mut filter, &["test", "test2"]);
        assert!(filter.filter(&content));

        set_subreddits(&mut filter, &["test2"]);
        assert!(!filter.filter(&content));

        set_subreddits(&mut filter, &["test", "test2"]);
        filter.min_score = Some(11);
        assert!(!filter.filter(&content));
    }
}
