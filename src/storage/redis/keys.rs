use crate::storage::core::items::TimeScope;
use uuid::Uuid;

#[derive(Debug, Default, Copy, Clone)]
pub(super) struct Keys;

#[allow(dead_code)]
impl Keys {
    pub(super) fn item_key(&self, part: &str, item: Uuid) -> String {
        format!("item:definition:{}:{}", part, item)
    }

    pub(super) fn item_near_key<'p>(&self, part: &'p str, item: Uuid) -> ListKey<'p> {
        ListKey {
            kind: "near",
            part,
            item: item.to_string(),
        }
    }

    pub(super) fn item_top_key<'p>(&self, part: &'p str, scope: TimeScope) -> ListKey<'p> {
        ListKey {
            kind: "top",
            part,
            item: scope.to_string(),
        }
    }

    pub(super) fn item_pop_key<'p>(&self, part: &'p str, scope: TimeScope) -> ListKey<'p> {
        ListKey {
            kind: "pop",
            part,
            item: scope.to_string(),
        }
    }

    pub(super) fn item_recent_key(&self, part: &str) -> String {
        format!("item:list:recent:{}", part)
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

#[derive(Debug, Clone)]
pub(super) struct ListKey<'p> {
    kind: &'static str,
    part: &'p str,
    item: String,
}

impl<'p> ListKey<'p> {
    #[allow(dead_code)]
    pub fn base_key(&self) -> String {
        format!("item:list:{}:{}:{}", self.kind, self.part, self.item)
    }
    pub fn list_key(&self) -> String {
        format!("item:list:{}:{}:{}:list", self.kind, self.part, self.item)
    }
    pub fn nmods_key(&self) -> String {
        format!("item:list:{}:{}:{}:nmods", self.kind, self.part, self.item)
    }
    pub fn epoch_key(&self) -> String {
        format!("item:list:{}:{}:{}:epoch", self.kind, self.part, self.item)
    }
}
