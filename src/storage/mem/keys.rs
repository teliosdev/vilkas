use crate::storage::items::TimeScope;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) struct Keys {
    item_database: String,
    user_database: String,
    model_database: String,
    activity_database: String,
}

impl Default for Keys {
    fn default() -> Keys {
        Keys {
            item_database: "items".to_owned(),
            user_database: "users".to_owned(),
            model_database: "models".to_owned(),
            activity_database: "activity".to_owned(),
        }
    }
}

#[allow(dead_code)]
impl Keys {
    pub(super) fn item_database(&self) -> &str {
        &self.item_database
    }

    pub(super) fn user_database(&self) -> &str {
        &self.user_database
    }

    pub(super) fn model_database(&self) -> &str {
        &self.model_database
    }

    pub(super) fn activity_database(&self) -> &str {
        &self.activity_database
    }

    pub(super) fn item_key(&self, part: &str, item: Uuid) -> String {
        format!("item:definition:{}:{}", part, item)
    }

    pub(super) fn item_near_key(&self, part: &str, item: Uuid) -> String {
        format!("item:list:near:{}:{}", part, item)
    }

    pub(super) fn item_top_key(&self, part: &str, scope: TimeScope) -> String {
        format!("item:list:top:{}:{}", part, scope)
    }

    pub(super) fn item_pop_key(&self, part: &str, scope: TimeScope) -> String {
        format!("item:list:pop:{}:{}", part, scope)
    }

    pub(super) fn user_key(&self, part: &str, id: &str) -> String {
        format!("user:data:{}:{}", part, id)
    }

    pub(super) fn model_key(&self, part: &str) -> String {
        format!("model:scope:{}", part)
    }

    pub(super) fn default_model_key(&self) -> String {
        "model:default".to_owned()
    }

    pub(super) fn activity_key(&self, part: &str, id: Uuid) -> String {
        format!("activity:item:{}:{}", part, id)
    }

    pub(super) fn activity_list_key(&self, part: &str) -> String {
        format!("activity:list:scope:{}", part)
    }

    pub(super) fn default_activity_list_key(&self) -> String {
        "activity:list:default".to_string()
    }
}
