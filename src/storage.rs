use crate::reddit_types::{comment::Comment, submission::Submission};
use anyhow::Result;

pub trait Storage: Sized {
    fn insert_comment(&mut self, comment: &Comment) -> Result<usize>;
    fn insert_submission(&mut self, comment: &Submission) -> Result<usize>;
}

pub trait Storable: Sized {
    fn store<T: Storage>(&self, storage: &mut T) -> Result<usize>;
}
