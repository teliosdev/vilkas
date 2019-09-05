use failure::Error;
use uuid::Uuid;

use crate::storage::sealed::Sealed;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserData {
    pub id: String,
    pub history: Vec<Uuid>,
}

impl UserData {
    pub fn new(id: impl Into<String>) -> UserData {
        UserData {
            id: id.into(),
            history: vec![],
        }
    }
}

pub trait UserStore: Sealed {
    fn find_user(&self, part: &str, id: &str) -> Result<UserData, Error>;
    fn user_push_history(&self, part: &str, id: &str, history: Uuid) -> Result<(), Error>;
}
