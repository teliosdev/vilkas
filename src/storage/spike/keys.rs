use aerospike::Key;
use uuid::Uuid;

use super::super::items::TimeScope;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) struct Keys {
    item_namespace: String,
    user_namespace: String,
    model_namespace: String,
    activity_namespace: String,
}

impl Default for Keys {
    fn default() -> Keys {
        Keys {
            item_namespace: "items".to_owned(),
            user_namespace: "users".to_owned(),
            model_namespace: "models".to_owned(),
            activity_namespace: "activity".to_owned(),
        }
    }
}

impl Keys {
    pub(super) fn item_key(&self, part: &str, item: Uuid) -> Key {
        as_key!(
            &self.item_namespace[..],
            "items",
            format!("{}:{}", part, item)
        )
    }

    pub(super) fn item_near_key(&self, part: &str, item: Uuid) -> Key {
        as_key!(
            &self.item_namespace[..],
            "items:lists",
            format!("near:{}:{}", part, item)
        )
    }

    pub(super) fn item_top_key(&self, part: &str, scope: TimeScope) -> Key {
        as_key!(
            &self.item_namespace[..],
            "items:lists",
            format!("top:{}:{}", part, scope)
        )
    }

    pub(super) fn item_pop_key(&self, part: &str, scope: TimeScope) -> Key {
        as_key!(
            &self.item_namespace[..],
            "items:lists",
            format!("pop:{}:{}", part, scope)
        )
    }

    pub(super) fn user_key(&self, part: &str, id: &str) -> Key {
        as_key!(
            &self.user_namespace[..],
            "users:cache",
            format!("user:{}:{}", part, id)
        )
    }

    pub(super) fn model_key(&self, part: &str) -> Key {
        as_key!(
            &self.model_namespace[..],
            "models",
            format!("model:part:{}", part)
        )
    }

    pub(super) fn default_model_key(&self) -> Key {
        as_key!(&self.model_namespace[..], "models", "model:default")
    }

    pub(super) fn activity_key(&self, part: &str, id: Uuid) -> Key {
        as_key!(
            &self.activity_namespace[..],
            "activities",
            format!("activity:{}:{}", part, id)
        )
    }

    pub(super) fn activity_list_key(&self, part: &str) -> Key {
        as_key!(
            &self.activity_namespace[..],
            "activities",
            format!("activity:list:{}", part)
        )
    }

    pub(super) fn default_activity_list_key(&self) -> Key {
        as_key!(
            &self.activity_namespace[..],
            "activities",
            "activity:default-list"
        )
    }
}
