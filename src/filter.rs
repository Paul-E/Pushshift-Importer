use hashbrown::HashSet;
use serde::Deserialize;
use toml::value::Datetime;
use std::{fs::read_to_string, path::Path, io};

pub trait Filterable {
    fn score(&self) -> i32;
    fn author(&self) -> Option<&str>;
    fn subreddit(&self) -> &str;
    fn created(&self) -> i64;
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    user_file: Option<String>,
    subreddit_file: Option<String>,
    #[serde(default)]
    users: Vec<String>,
    #[serde(default)]
    subreddits: Vec<String>,
    min_date: Option<Datetime>,
    max_date: Option<Datetime>
}

pub enum Error {
    Config,
    SubredditFile(io::Error),
    UserFile(io::Error),
}

#[derive(Debug, Clone, Default)]
pub struct Filter {
    users: HashSet<String>,
    subreddits: HashSet<String>,
}

impl Filter {
    pub fn new(users: HashSet<String>, subreddits: HashSet<String>) -> Self {
        Self {
            users,
            subreddits
        }
    }

    pub fn from_config<T: AsRef<Path>>(path: &T) -> Result<Self, Error> {
        let config: Config = toml::from_str(
            read_to_string(path)
                .expect("Unable to read filter config")
                .as_str(),
        )
        .expect("Failed to parse filter config");
        let mut subreddits: HashSet<_> = config.subreddits.into_iter().collect();
        if let Some(subreddit_file) = config.subreddit_file {
            let contents = read_to_string(subreddit_file).map_err(|error| Error::SubredditFile(error))?;
            for subreddit in contents.split_ascii_whitespace() {
                subreddits.insert(subreddit.into());
            }
        }
        let mut users: HashSet<_> = config.users.into_iter().collect();
        if let Some(user_file) = config.user_file {
            let contents = read_to_string(user_file).map_err(|error| Error::UserFile(error))?;
            for user in contents.split_ascii_whitespace() {
                users.insert(user.into());
            }
        }
        Ok(Self {subreddits, users})
    }

    pub fn add_user(&mut self, user: &str) {
        self.users.insert(user.into());
    }

    pub fn add_subreddit(&mut self, subreddit: &str) {
        self.subreddits.insert(subreddit.into());
    }

    pub fn filter<T: Filterable>(&self, content: &T) -> bool {
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
        if self.subreddits.contains(content.subreddit()) {
            return true;
        }
        false
    }
}
