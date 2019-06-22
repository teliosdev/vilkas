/// A vector of items.  This differs from the standard implementation in that it
/// focuses entirely on mathematical operations and the like; however, it is
/// backed by a plain standard library vector.
pub struct Vector<T>(Vec<T>);
