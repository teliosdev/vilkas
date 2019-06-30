use num_traits::Float;
use std::iter::FromIterator;
use std::ops::{Deref, DerefMut};

/// An encapsulated vector type.  This is interchangable with the
/// standard library's vec; this only adds some behaviour on top
/// of the standard library's - namely, useful features for the
/// handling of vectors (i.e. combining, dot products, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Vector<T>(Vec<T>);

impl<T> Vector<T> {
    pub fn empty() -> Vector<T> {
        Vector(Vec::new())
    }
}

impl<T: Float + Default> Vector<T> {
    /// Retrieves a specific element at a given index, or the default if it
    /// does not exist at that index.
    pub fn element(&self, idx: usize) -> T {
        self.get(idx).cloned().unwrap_or_default()
    }

    /// Returns a zip of this and the other vector.  The resulting items in the
    /// iterator are the index offset of the element in the vector, the
    /// corresponding element in this vector (or the default value, if it
    /// doesn't exist), and the corresponding element in the other vector
    /// (or the default value if it doesn't exist).
    pub fn combine<'b, O: Float + Default>(
        &'b self,
        other: &'b Vector<O>,
    ) -> impl Iterator<Item = (T, O)> + 'b {
        let max = self.len().max(other.len());
        (0..max).map(move |idx| (self.element(idx), other.element(idx)))
    }

    /// Calculates the dot product of this vector with another vector.
    /// The dot product is the sum of each elements multiplied by the
    /// corresponding elements from the other; or, if `a` and `b` are
    /// vectors, and of the same length, then the sum of i=0 to l of
    /// `a_i * b_i`, where l is the length of `a` and `b`.
    pub fn dot(&self, other: &Self) -> T {
        self.combine(other)
            .map(|(a, b)| a * b)
            .fold(T::zero(), T::add)
    }

    /// Calculates the magnitude of this vector (in math, that would
    /// be `||a||`, where `a` is this vector).  This is basically
    /// the square root of the dot product of this vector with
    /// itself.
    pub fn magnitude(&self) -> T {
        self.iter()
            .map(|value| value.powi(2))
            .fold(T::zero(), T::add)
            .sqrt()
    }

    /// Sets a given index to the given value.  If the index is out of
    /// bounds, the vector is resized to make the index in bounds.
    pub fn set_element(&mut self, idx: usize, value: T) {
        self.resize_to(idx);
        self[idx] = value;
    }

    /// Resizes the the vector to the given index, if the index is
    /// greater than the current index.  It fills all of the values
    /// in between with default values.
    pub fn resize_to(&mut self, idx: usize) {
        if idx >= self.len() {
            self.resize_with(idx + 1, Default::default);
        }
    }
}

impl<T> From<Vec<T>> for Vector<T> {
    fn from(vec: Vec<T>) -> Vector<T> {
        Vector(vec)
    }
}

impl<T> Into<Vec<T>> for Vector<T> {
    fn into(self) -> Vec<T> {
        self.0
    }
}

impl<T> Deref for Vector<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Vector<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> FromIterator<T> for Vector<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Vec::from_iter(iter).into()
    }
}

impl<T> Extend<T> for Vector<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.0.extend(iter)
    }
}

impl<T> IntoIterator for Vector<T> {
    type Item = T;
    type IntoIter = ::std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub fn combine<T: Default, I: Iterator<Item = T>, OT: Default, OI: Iterator<Item = OT>>(
    this: I,
    other: OI,
) -> impl Iterator<Item = (T, OT)> {
    IterCombine(this, other)
}

pub fn combine4<T1, I1, T2, I2, T3, I3, T4, I4>(
    a: I1,
    b: I2,
    c: I3,
    d: I4,
) -> impl Iterator<Item = (T1, T2, T3, T4)>
where
    T1: Default,
    T2: Default,
    T3: Default,
    T4: Default,
    I1: Iterator<Item = T1>,
    I2: Iterator<Item = T2>,
    I3: Iterator<Item = T3>,
    I4: Iterator<Item = T4>,
{
    combine(combine(combine(a, b), c), d).map(|(((a, b), c), d)| (a, b, c, d))
}

struct IterCombine<T: Default, OT: Default, I: Iterator<Item = T>, OI: Iterator<Item = OT>>(I, OI);

impl<T: Default, OT: Default, I: Iterator<Item = T>, OI: Iterator<Item = OT>> Iterator
    for IterCombine<T, OT, I, OI>
{
    type Item = (T, OT);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.0.next(), self.1.next()) {
            (Some(t), Some(ot)) => Some((t, ot)),
            (None, Some(ot)) => Some((Default::default(), ot)),
            (Some(t), None) => Some((t, Default::default())),
            (None, None) => None,
        }
    }
}
