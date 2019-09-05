use crate::learn::Vector;
use std::borrow::Cow;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::ops::Add;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FeatureList<'k>(HashMap<Cow<'k, str>, f64>);

impl<'k> FeatureList<'k> {
    pub fn insert(&mut self, name: impl Into<Cow<'k, str>>, value: f64) {
        self.0.insert(name.into(), value);
    }

    pub fn ensure_has(&mut self, key: &Cow<'_, str>) {
        if !self.0.contains_key(key) {
            let key = key.clone().into_owned();
            self.0.insert(Cow::Owned(key), 0.0);
        }
    }

    pub fn to_vector(&self, required: &[&str]) -> Vector<f64> {
        required
            .iter()
            .map(|key| self.get(*key).cloned().unwrap_or(0.0))
            .collect()
    }

    pub fn keys(&self) -> impl Iterator<Item = &Cow<'k, str>> {
        self.0.keys()
    }

    pub fn combine<'c>(
        &'c self,
        other: &'c Self,
    ) -> impl Iterator<Item = (&'c str, f64, f64)> + 'c {
        self.iter()
            .flat_map(move |(k, v)| other.get(k).into_iter().map(move |s| (k.as_ref(), *v, *s)))
    }

    pub fn dot(&self, other: &Self) -> f64 {
        self.combine(other)
            .map(|(_, v, o)| v * o)
            .fold(0.0, Add::add)
    }

    pub fn union<'c>(
        &'c self,
        other: &'c Self,
    ) -> impl Iterator<Item = (Cow<'k, str>, f64, f64)> + 'c {
        self.iter()
            .map(move |(k, v)| match other.get(k) {
                Some(s) => (k.clone(), *v, *s),
                None => (k.clone(), *v, 0.0),
            })
            .chain(other.iter().flat_map(move |(k, v)| match self.get(k) {
                Some(_) => None,
                None => Some((k.clone(), 0.0, *v)),
            }))
    }
}

impl<'k> std::ops::Deref for FeatureList<'k> {
    type Target = HashMap<Cow<'k, str>, f64>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'k> std::ops::DerefMut for FeatureList<'k> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'k> std::ops::Sub for FeatureList<'k> {
    type Output = FeatureList<'k>;

    fn sub(self, other: Self) -> Self::Output {
        let map = self
            .union(&other)
            .map(|(k, a, b)| (k, a - b))
            .collect::<HashMap<_, _>>();
        FeatureList(map)
    }
}

impl<'c, 'k> std::ops::Sub for &'c FeatureList<'k> {
    type Output = FeatureList<'k>;

    fn sub(self, other: Self) -> Self::Output {
        let map = self
            .union(other)
            .map(|(k, a, b)| (k, a - b))
            .collect::<HashMap<_, _>>();
        FeatureList(map)
    }
}

impl<'k, K: Into<Cow<'k, str>>> FromIterator<(K, f64)> for FeatureList<'k> {
    fn from_iter<I: IntoIterator<Item = (K, f64)>>(iter: I) -> Self {
        let map = HashMap::from_iter(iter.into_iter().map(|(k, v)| (k.into(), v)));
        FeatureList(map)
    }
}

impl<'k, K: Into<Cow<'k, str>>> Extend<(K, f64)> for FeatureList<'k> {
    fn extend<I: IntoIterator<Item = (K, f64)>>(&mut self, iter: I) {
        self.0.extend(iter.into_iter().map(|(k, v)| (k.into(), v)))
    }
}

impl<'k> IntoIterator for FeatureList<'k> {
    type Item = (Cow<'k, str>, f64);
    type IntoIter = std::collections::hash_map::IntoIter<Cow<'k, str>, f64>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
