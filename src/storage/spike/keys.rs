use super::super::items::TimeScope;
use aerospike::Key;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct Keys {
    item_namespace: String,
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
}
