use crate::storage::{Example, FeatureList};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartConfig {
    #[serde(default = "defaults::max_candidate_count")]
    pub max_candidate_count: usize,
    #[serde(default)]
    pub meta_features: HashMap<String, MetaFeature>,
    #[serde(default = "defaults::upgrade_chance")]
    pub upgrade_chance: f64,
}

mod defaults {
    pub fn max_candidate_count() -> usize {
        256
    }
    pub fn upgrade_chance() -> f64 {
        0.10
    }
}

impl Default for PartConfig {
    fn default() -> PartConfig {
        PartConfig {
            max_candidate_count: defaults::max_candidate_count(),
            meta_features: HashMap::new(),
            upgrade_chance: defaults::upgrade_chance(),
        }
    }
}

impl PartConfig {
    pub fn extract_all(&self, list: &mut FeatureList, given: &Example, current: &Example) {
        for k in given.item.meta.keys() {
            if let Some(meta) = self.meta_features.get(k) {
                meta.extract(k, list, given, current);
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum MetaFeature {
    Ignore,
    Overlap,
}

impl Default for MetaFeature {
    fn default() -> MetaFeature {
        MetaFeature::Ignore
    }
}

impl MetaFeature {
    fn extract(&self, name: &str, list: &mut FeatureList, given: &Example, current: &Example) {
        match self {
            MetaFeature::Ignore => {}
            MetaFeature::Overlap => {
                let blank = HashSet::new();
                let left = given.item.meta.get(name).unwrap_or(&blank);
                let right = current.item.meta.get(name).unwrap_or(&blank);
                let overlap = left.intersection(right).count();

                list.insert(format!("meta:{}:overlap", name), overlap as f64);
            }
        }
    }
}
