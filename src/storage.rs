use crate::{comment::Comment, submission::Submission};
use anyhow::Result;

pub trait Storage: Sized {
    fn insert_comment(&self, comment: &Comment) -> Result<usize>;
    fn insert_submission(&self, comment: &Submission) -> Result<usize>;
}

pub trait Storable: Sized {
    fn store<T: Storage>(&self, storage: &T) -> Result<usize>;
}
