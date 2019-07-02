use crate::recommend::PartConfig;
use crate::storage::{FeatureList, Item, TimeScope};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::ops::Add;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Example {
    pub basic: BasicExample,
    pub item: Item,
}

impl Example {
    pub fn features(&self, current: &Example, config: &PartConfig) -> FeatureList<'static> {
        let mut feat = FeatureList::default();
        let list = self.basic.near.unwrap_or_default();
        feat.insert("list:near:value:ln1p", list.value.ln_1p());
        feat.insert("list:near:rank", list.rank);

        for scope in TimeScope::variants() {
            let list = self.basic.top.get(&scope).cloned().unwrap_or_default();
            feat.insert(format!("list:top:{}:value:ln1p", scope), list.value.ln_1p());
            feat.insert(format!("list:top:{}:rank", scope), list.rank);
            let list = self.basic.pop.get(&scope).cloned().unwrap_or_default();
            feat.insert(format!("list:pop:{}:value:ln1p", scope), list.value.ln_1p());
            feat.insert(format!("list:pop:{}:rank", scope), list.rank);
        }

        config.extract_all(&mut feat, &self, current);

        feat
    }
}

#[derive(Debug, Default, Copy, Clone, Serialize, Deserialize)]
pub struct ListPosition {
    value: f64,
    rank: f64,
}

impl ListPosition {
    pub fn new(value: f64, rank: f64) -> ListPosition {
        ListPosition { value, rank }
    }
}

impl From<(f64, f64)> for ListPosition {
    fn from((value, rank): (f64, f64)) -> ListPosition {
        ListPosition::new(value, rank)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BasicExample {
    pub id: Uuid,
    pub near: Option<ListPosition>,
    pub top: HashMap<TimeScope, ListPosition>,
    pub pop: HashMap<TimeScope, ListPosition>,
}

impl BasicExample {
    pub fn new(id: Uuid) -> BasicExample {
        BasicExample {
            id,
            near: None,
            top: Default::default(),
            pop: Default::default(),
        }
    }

    pub fn with_near(&mut self, pos: impl Into<ListPosition>) -> &mut Self {
        let pos = pos.into();
        match self.near {
            Some(cur) if cur.rank > pos.rank => {}
            _ => self.near = Some(pos),
        }
        self
    }

    pub fn with_top(&mut self, scope: TimeScope, pos: impl Into<ListPosition>) -> &mut Self {
        self.top.insert(scope, pos.into());
        self
    }

    pub fn with_pop(&mut self, scope: TimeScope, pos: impl Into<ListPosition>) -> &mut Self {
        self.pop.insert(scope, pos.into());
        self
    }

    pub fn complete(self, item: Item) -> Example {
        Example { basic: self, item }
    }

    pub fn importance(&self) -> f64 {
        let near = self.near.map(|v| v.value).unwrap_or_default().powi(2) + 1.0;
        let tops = self
            .top
            .values()
            .map(|v| v.value.powi(2))
            .fold(0.0, Add::add)
            + 1.0;
        let pops = self
            .pop
            .values()
            .map(|v| v.value.powi(2))
            .fold(0.0, Add::add)
            + 1.0;
        (near * tops * pops).sqrt() - 1.0
    }
}
