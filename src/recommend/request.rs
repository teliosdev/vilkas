use crate::recommend::Core;
use crate::storage::{BasicExample, Example, Item, Storage, TimeScope};
use failure::Error;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    #[serde(alias = "p")]
    pub part: String,
    #[serde(alias = "u")]
    pub user: Uuid,
    #[serde(alias = "t")]
    pub current: Uuid,
    #[serde(alias = "w")]
    pub whitelist: Option<Vec<Uuid>>,
    #[serde(alias = "c")]
    pub count: usize,
}

impl Request {
    pub fn current<T: Storage + 'static>(&self, core: &Core<T>) -> Result<Item, Error> {
        core.storage
            .find_item(&self.part, self.current)
            .map(|item| {
                item.unwrap_or_else(|| Item {
                    id: self.current,
                    part: self.part.clone(),
                    views: 1,
                    meta: Default::default(),
                })
            })
    }

    pub fn examples<'t, T: Storage + 'static>(
        &'t self,
        core: &Core<T>,
    ) -> Result<impl Iterator<Item = Example> + 't, Error> {
        let candidates = self.candidates(core)?;
        let buf = BufIter::new(candidates.into_iter(), 32);
        let storage = core.storage.clone();
        let iter = buf.flat_map(move |group: Vec<BasicExample>| {
            let result = storage.find_items(&self.part, Box::new(group.iter().map(|e| e.id)));
            result
                .ok()
                .into_iter()
                .flatten()
                .zip(group.into_iter())
                .flat_map(|(i, ex)| i.map(|item| ex.complete(item)))
        });

        Ok(iter)
    }

    pub fn candidates<T: Storage>(&self, core: &Core<T>) -> Result<Vec<BasicExample>, Error> {
        let max = core.config_for(&self.part).max_candidate_count;
        if let Some(list) = self.whitelist.as_ref() {
            return Ok(list
                .iter()
                .cloned()
                .map(BasicExample::new)
                .take(max)
                .collect());
        }

        let mut candidate_list = CandidateList::new(max * 2);

        let storage = core.storage.clone();
        let list = storage.find_items_near(&self.part, self.current)?;
        for (i, (id, value)) in list.items.iter().cloned().enumerate() {
            candidate_list.mutate(id, |ex| {
                ex.with_near((value, i as f64));
            });
        }

        for scope in TimeScope::variants() {
            let list = storage.find_items_top(&self.part, scope)?;
            for (i, (id, value)) in list.items.iter().cloned().enumerate() {
                candidate_list.mutate(id, |ex| {
                    ex.with_top(scope, (value, i as f64));
                });
            }

            let list = storage.find_items_popular(&self.part, scope)?;
            for (i, (id, value)) in list.items.iter().cloned().enumerate() {
                candidate_list.mutate(id, |ex| {
                    ex.with_pop(scope, (value, i as f64));
                });
            }
        }

        let mut list = candidate_list
            .values()
            .cloned()
            .take(max)
            .collect::<Vec<BasicExample>>();
        crate::ord::sort_cached_float(&mut list, |a| a.importance());

        Ok(list)
    }
}

struct CandidateList {
    map: HashMap<Uuid, BasicExample>,
    max: usize,
    cnt: usize,
}

impl CandidateList {
    pub fn new(max: usize) -> CandidateList {
        CandidateList {
            map: Default::default(),
            max,
            cnt: 0,
        }
    }

    pub fn into_inner(self) -> HashMap<Uuid, BasicExample> {
        self.map
    }

    pub fn mutate(&mut self, id: Uuid, mut f: impl FnMut(&mut BasicExample)) {
        if let Some(v) = self.map.get_mut(&id) {
            f(v);
        } else if self.cnt < self.max {
            let mut ex = BasicExample::new(id);
            f(&mut ex);
            self.map.insert(id, ex);
        }
    }
}

impl Deref for CandidateList {
    type Target = HashMap<Uuid, BasicExample>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl DerefMut for CandidateList {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct BufIter<I>(Option<I>, usize);

impl<I> BufIter<I> {
    pub fn new(iter: I, size: usize) -> BufIter<I> {
        BufIter(Some(iter), size)
    }
}

impl<I> Iterator for BufIter<I>
where
    I: Iterator,
{
    type Item = Vec<<I as Iterator>::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            Some(i) => {
                let mut buf = Vec::with_capacity(self.1);
                while buf.len() < self.1 {
                    match i.next() {
                        Some(v) => buf.push(v),
                        None if !buf.is_empty() => {
                            self.0 = None;
                            return Some(buf);
                        }
                        None => {
                            self.0 = None;
                            return None;
                        }
                    }
                }

                Some(buf)
            }
            None => None,
        }
    }
}
