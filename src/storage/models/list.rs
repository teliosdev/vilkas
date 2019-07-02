use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Add;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FeatureList<'k>(HashMap<Cow<'k, str>, f64>);

impl<'k> FeatureList<'k> {
    pub fn insert(&mut self, name: impl Into<Cow<'k, str>>, value: f64) {
        self.0.insert(name.into(), value);
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
