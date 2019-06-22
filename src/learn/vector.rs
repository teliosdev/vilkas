use num_traits::Float;
use std::iter::FromIterator;
use std::ops::{Add, AddAssign, Mul, MulAssign};
use std::vec::IntoIter;

/// A vector of items.  This differs from the standard implementation in that it
/// focuses entirely on mathematical operations and the like; however, it is
/// backed by a plain standard library vector.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Vector<T>(Vec<T>);

impl<T> Vector<T> {
    /// Creates a new vector, with zero elements.
    pub fn new() -> Vector<T> {
        Vector(Vec::new())
    }

    /// The length of the vector, or the number of elements inside of it.  A
    /// freshly created vector has zero elements inside of it.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Retrieves a single element from the vector at the given index.  If the
    /// index is not inside of this vector, then this returns [`None`].
    /// Otherwise, it returns that element.
    pub fn element(&self, idx: usize) -> Option<&T> {
        self.0.get(idx)
    }

    /// Returns a zip of this and the other vector.  The resulting items are
    /// the index offset of the element in the vector, the corresponding element
    /// in this vector, if it exists, and the corresponding element in the other
    /// vector.
    pub fn zip_either<'b, O>(
        &'b self,
        other: &'b Vector<O>,
    ) -> impl Iterator<Item=(usize, Option<&T>, Option<&O>)> + 'b {
        let max = self.len().max(other.len());
        (0..max)
            .into_iter()
            .map(move |idx| (idx, self.element(idx), other.element(idx)))
    }

    /// Adds an element onto the end of the vector.
    pub fn push(&mut self, item: T) {
        self.0.push(item)
    }

    pub fn iter(&self) -> impl Iterator<Item=&T> {
        self.0.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item=&mut T> {
        self.0.iter_mut()
    }

    pub fn into_iter(self) -> impl Iterator<Item=T> {
        self.0.into_iter()
    }
}

impl<T: Copy + Default> Vector<T> {
    /// Retrieves a single element from the vector, or the default value of that
    /// item, if not inside of the vector.
    pub fn get(&self, idx: usize) -> T {
        self.element(idx).cloned().unwrap_or_default()
    }

    /// Returns a zip of this and the other vector.  The resulting items in the
    /// iterator are the index offset of the element in the vector, the
    /// corresponding element in this vector (or the default value, if it
    /// doesn't exist), and the corresponding element in the other vector
    /// (or the default value if it doesn't exist).
    pub fn zip<'b, O: Copy + Default>(
        &'b self,
        other: &'b Vector<O>,
    ) -> impl Iterator<Item=(usize, T, O)> + 'b {
        let max = self.len().max(other.len());
        (0..max)
            .into_iter()
            .map(move |idx| (idx, self.get(idx), other.get(idx)))
    }

    /// Sets a given index to the value.  If the index is out of bounds, the
    /// vector is resized to include the index, filling the other elements
    /// with the default value.
    pub fn set(&mut self, idx: usize, value: T) {
        if idx + 1 > self.len() {
            self.0.resize_with(idx + 1, Default::default)
        }

        self.0[idx] = value;
    }
}

impl<T: Float + Default> Vector<T> {
    /// Calculates the dot product of this vector with another vector.
    /// The dot product is the sum of each elements multiplied by the
    /// corresponding elements from the other; or, if `a` and `b` are
    /// vectors, and of the same length, then the sum of i=0 to l of
    /// `a_i * b_i`, where l is the length of `a` and `b`.
    pub fn dot(&self, other: &Self) -> T {
        self.zip(other)
            .map(|(_, a, b)| a * b)
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
}

impl<T: Float + Default> Add<Self> for &'_ Vector<T> {
    type Output = Vector<T>;

    fn add(self, rhs: &Vector<T>) -> Self::Output {
        self.zip(rhs).map(|(_, a, b)| a + b).collect()
    }
}

impl<T: Float + Default> AddAssign<&'_ Self> for Vector<T> {
    fn add_assign(&mut self, rhs: &Vector<T>) {
        if self.len() < rhs.len() {
            self.0.resize_with(rhs.len(), Default::default);
        }
        for (idx, value) in self.0.iter_mut().enumerate() {
            *value = *value + rhs.get(idx);
        }
    }
}

impl<T: Float + Default> Add<T> for &'_ Vector<T> {
    type Output = Vector<T>;

    fn add(self, rhs: T) -> Self::Output {
        self.0.iter().map(|v| *v + rhs).collect()
    }
}

impl<T: Float + Default> AddAssign<T> for Vector<T> {
    fn add_assign(&mut self, rhs: T) {
        for value in self.0.iter_mut() {
            *value = *value + rhs;
        }
    }
}

impl<T: Float + Default> Mul<Self> for &'_ Vector<T> {
    type Output = Vector<T>;

    fn mul(self, rhs: &Vector<T>) -> Self::Output {
        self.zip(rhs).map(|(_, a, b)| a * b).collect()
    }
}

impl<T: Float + Default> MulAssign<&'_ Self> for Vector<T> {
    fn mul_assign(&mut self, rhs: &Vector<T>) {
        if self.len() < rhs.len() {
            self.0.resize_with(rhs.len(), Default::default);
        }
        for (idx, value) in self.0.iter_mut().enumerate() {
            *value = *value * rhs.get(idx);
        }
    }
}

impl<T: Float + Default> Mul<T> for &'_ Vector<T> {
    type Output = Vector<T>;

    fn mul(self, rhs: T) -> Self::Output {
        self.0.iter().map(|v| *v * rhs).collect()
    }
}

impl<T: Float + Default> MulAssign<T> for Vector<T> {
    fn mul_assign(&mut self, rhs: T) {
        for value in self.0.iter_mut() {
            *value = *value * rhs;
        }
    }
}

impl<T> AsRef<[T]> for Vector<T> {
    fn as_ref(&self) -> &[T] {
        &self.0[..]
    }
}

impl<T> AsMut<[T]> for Vector<T> {
    fn as_mut(&mut self) -> &mut [T] {
        &mut self.0[..]
    }
}

impl<T, E: Into<T>> Extend<E> for Vector<T> {
    fn extend<I: IntoIterator<Item=E>>(&mut self, iter: I) {
        self.0.extend(iter.into_iter().map(Into::into));
    }
}

impl<T, E: Into<T>> FromIterator<E> for Vector<T> {
    fn from_iter<I: IntoIterator<Item=E>>(iter: I) -> Self {
        let mut vec = Vector::new();
        vec.0.extend(iter.into_iter().map(Into::into));
        vec
    }
}

impl<T> IntoIterator for Vector<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
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
