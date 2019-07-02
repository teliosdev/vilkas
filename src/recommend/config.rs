use crate::storage::{Example, FeatureList};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartConfig {
    pub max_candidate_count: usize,
    pub meta_features: HashMap<String, MetaFeature>,
}

impl Default for PartConfig {
    fn default() -> PartConfig {
        PartConfig {
            max_candidate_count: 256,
            meta_features: HashMap::new(),
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
