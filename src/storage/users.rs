use failure::Error;
use uuid::Uuid;

use super::Sealed;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserData {
    pub id: String,
    pub history: Vec<Uuid>,
}

pub trait UserStorage: Sealed {
    fn find_user(&self, part: &str, id: &str) -> Result<UserData, Error>;
    fn user_push_history(&self, part: &str, id: &str, history: Uuid) -> Result<(), Error>;
}
