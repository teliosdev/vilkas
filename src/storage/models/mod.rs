pub use self::example::{BasicExample, Example, ListPosition};
pub use self::list::FeatureList;
use super::Sealed;
use failure::Error;
use uuid::Uuid;

mod example;
mod list;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Activity {
    pub id: Uuid,
    pub part: String,
    pub current: Example,
    pub visible: Vec<Example>,
    pub chosen: Option<Vec<Uuid>>,
}

pub trait ModelStorage: Sealed {
    fn set_default_model(&self, list: FeatureList<'_>) -> Result<(), Error>;
    fn find_default_model(&self) -> Result<FeatureList<'static>, Error>;
    fn find_model(&self, part: &str) -> Result<Option<FeatureList<'static>>, Error>;

    fn model_activity_save(&self, part: &str, activity: &Activity) -> Result<(), Error>;
    fn model_activity_load(&self, part: &str, id: Uuid) -> Result<Option<Activity>, Error>;
    fn model_activity_choose(&self, part: &str, id: Uuid, chosen: &[Uuid]) -> Result<(), Error>;

    fn model_activity_pluck(&self) -> Result<Vec<Activity>, Error>;
    fn model_activity_delete_all<'p, Ids>(&self, id: Ids) -> Result<(), Error>
    where
        Ids: IntoIterator<Item = (&'p str, Uuid)>;
}
