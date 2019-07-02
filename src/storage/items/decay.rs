use std::cmp::Ordering;

use super::{ItemList, TimeScope};

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// This describes the decay function used in the near list.
pub struct NearListDecay {
    pub(crate) max_modifications: u64,
    pub(crate) max_count: u32,
    pub(crate) func: DecayFunction,
}

impl NearListDecay {
    pub fn decay(&self, list: &mut ItemList) {
        if list.nmods > self.max_modifications {
            list.items
                .sort_unstable_by(|(_, a), (_, b)| b.partial_cmp(a).unwrap_or(Ordering::Equal));
            let mut local = vec![];
            // we have to do this because we can't really move the list
            // items out of a borrowed context - so we do a swap,
            // temporarily setting the list items to an empty vec (which
            // is zero-alloc) while giving us complete ownership over
            // the list.  We then place it back in the list items,
            // passing ownership back over to them.
            std::mem::swap(&mut local, &mut list.items);
            list.items = local
                .into_iter()
                .map(|(id, count)| (id, self.func.decay(count, 1.0)))
                .take(self.max_count as usize)
                .collect();
        }
    }
}

impl Default for NearListDecay {
    fn default() -> Self {
        NearListDecay {
            max_modifications: 512,
            max_count: 64,
            func: DecayFunction::Log {
                base: std::f64::consts::E,
                coefficient: 1.0,
            },
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum DecayFunction {
    Log { base: f64, coefficient: f64 },
    ExpMul { base: f64, powmul: f64 },
}

impl DecayFunction {
    pub fn decay(self, value: f64, lambda: f64) -> f64 {
        match self {
            DecayFunction::Log { base, coefficient } => value.log(base) * coefficient * lambda,
            DecayFunction::ExpMul { base, powmul } => value * base.powf(powmul * lambda),
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ItemListDecay {
    pub(crate) max_modifications: u64,
    pub(crate) max_count: u32,
    pub(crate) func: DecayFunction,
}

impl ItemListDecay {
    pub fn decay(self, scope: TimeScope, since: u128, list: &mut ItemList) {
        if list.nmods > self.max_modifications {
            list.items
                .sort_unstable_by(|(_, a), (_, b)| b.partial_cmp(a).unwrap_or(Ordering::Equal));
            let mut local = vec![];
            // we have to do this because we can't really move the list
            // items out of a borrowed context - so we do a swap,
            // temporarily setting the list items to an empty vec (which
            // is zero-alloc) while giving us complete ownership over
            // the list.  We then place it back in the list items,
            // passing ownership back over to them.
            std::mem::swap(&mut local, &mut list.items);
            list.items = local
                .into_iter()
                .map(|(id, count)| (id, self.func.decay(count, since as f64 / scope.half_life())))
                .take(self.max_count as usize)
                .collect();
        }
    }

    pub fn top_default() -> Self {
        ItemListDecay {
            max_modifications: 512,
            max_count: 64,
            func: DecayFunction::ExpMul {
                base: 2.0,
                powmul: -1.0,
            },
        }
    }

    pub fn pop_default() -> Self {
        ItemListDecay {
            max_modifications: 512,
            max_count: 64,
            func: DecayFunction::ExpMul {
                base: 2.0,
                powmul: -0.25,
            },
        }
    }
}
