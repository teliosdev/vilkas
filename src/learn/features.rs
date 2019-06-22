use num_traits::Float;
use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::iter::FromIterator;

/// A list of feature strings for consideration.  Each key-value pair refers
/// to one feature, and one weight.  Because weights are expected to be
/// representable as floats, the values must be a float type - either f32 or
/// f64, whatever the upper classes decide them to be.
///
/// Note here that the Float trait implies Copy.
#[derive(Debug, Clone)]
pub struct Features<'c, T: Float> {
    /// The inner map.  This is just the key-value store for the features of
    /// this list.
    inner: HashMap<Cow<'c, str>, T>,
    /// The default value to use if the feature doesn't exist in this map.
    /// Ideally, when using a Features map, this should not change when dealing
    /// with things of the same type, e.g. features of an example vs weights
    /// of a model.
    default: T,
}

impl<'c, T: Float> Features<'c, T> {
    /// Create a new instance of the Features list.  This populates the default
    /// value with a zero value.
    pub fn new() -> Features<'c, T> {
        // T::from(1) will never return `None`, so this is safe.
        Self::with_default(T::zero())
    }

    /// Creates a new instance of the features list, but with the given default
    /// specified.  This can be useful if you want all non-specified features
    /// to be the given value (e.g. zero) instead of the default of
    /// [`Self::new`], i.e. a normal, non-zero value.
    pub fn with_default(default: T) -> Features<'c, T> {
        Features {
            inner: HashMap::new(),
            default,
        }
    }

    /// Takes this feature list and converts it into a feature list that does
    /// not borrow its feature information from any other feature lists.
    pub fn to_owned(&self) -> Features<'static, T> {
        let inner = self
            .inner
            .iter()
            .map(|(key, value)| (Cow::Owned(key.clone().into_owned()), *value))
            .collect();

        Features {
            inner,
            default: self.default,
        }
    }

    /// Pushes a specific feature into the list.  If it already exists, it is
    /// overwritten.
    pub fn push(&mut self, name: impl Into<Cow<'c, str>>, value: impl Into<T>) {
        self.inner.insert(name.into(), value.into());
    }

    pub(crate) fn inner_mut(&mut self) -> &mut HashMap<Cow<'c, str>, T> {
        &mut self.inner
    }

    /// Creates an iterator of the current feature list.  This borrows self for
    /// the lifetime of the iterator, and the strings borrowed by the iterator.
    pub fn iter(&self) -> impl Iterator<Item=(&str, T)> {
        self.inner.iter().map(|(k, v)| (k.as_ref(), *v))
    }

    pub fn get<N>(&self, name: N) -> T
        where
            N: Borrow<Cow<'c, str>>,
    {
        self.inner
            .get(name.borrow())
            .cloned()
            .unwrap_or_else(|| self.default())
    }

    /// Zips this feature list with another feature list.  The output is an
    /// array of tuples - the first element in the tuple is the feature name,
    /// the second element is this feature list's value for that feature name
    /// (or the default, if it does not exist in this feature list), and the
    /// third element is the other feature list's value for that feature name.
    pub fn zip<'o: 'c>(&'o self, other: &'o Self) -> impl Iterator<Item=(&'o str, T, T)> + 'c {
        // All of the keys that the other feature list has that we don't have,
        // since we'll need both here.
        let other_keys = other
            .inner
            .keys()
            .filter(move |key| !self.inner.contains_key(key.as_ref()));
        let this_default = self.default();
        let other_default = other.default();
        self.inner.keys().chain(other_keys).map(move |key| {
            let this_value = self.inner.get(key).cloned().unwrap_or(this_default);
            let other_value = other.inner.get(key).cloned().unwrap_or(other_default);

            (key.as_ref(), this_value, other_value)
        })
    }

    /// Calculates the dot product of two feature lists.  Note that upon
    /// overflow or underflow, it cascades to the output; if needed, it may be
    /// necessary to check the result.
    pub fn dot(&self, other: &Self) -> T {
        self.zip(other)
            .fold(T::zero(), |acc, (_, a, b)| acc + a * b)
    }

    /// Calculates the magnitude of the feature list; or, the square root of the
    /// dot product of the feature list with itself.
    pub fn magnitude(&self) -> T {
        self.inner
            .values()
            .map(|v| v.powi(2))
            .fold(T::zero(), T::add)
            .sqrt()
    }

    /// The default value associated with this feature list.  This is set once
    /// upon initialization.
    pub fn default(&self) -> T {
        self.default
    }
}

impl<'c, K: Into<Cow<'c, str>>, T: Float> Extend<(K, T)> for Features<'c, T> {
    fn extend<I: IntoIterator<Item=(K, T)>>(&mut self, iter: I) {
        self.inner
            .extend(iter.into_iter().map(|(k, t)| (k.into(), t)))
    }
}

impl<'c, K: Into<Cow<'c, str>>, T: Float> FromIterator<(K, T)> for Features<'c, T> {
    fn from_iter<I: IntoIterator<Item=(K, T)>>(iter: I) -> Self {
        let mut features = Features::new();
        features.extend(iter);
        features
    }
}
