use arrow2::array::PrimitiveArray;

/// Conversion corresponding to `From` with the difference that the validity mask
/// is set accordingly to the validity to the contained values.
pub trait FromWithValidity<T> {
    fn from_with_validity(value: T) -> Self;
}

/// Conversion corresponding to `FromIterator` with the difference that the validity mask
/// is set accordingly to the validity to the contained values.
pub trait FromIteratorWithValidity<A: Sized> {
    fn from_iter_with_validity<T: IntoIterator<Item = A>>(iter: T) -> Self;
}

impl<T> FromWithValidity<Vec<u64>> for T
where
    T: FromIteratorWithValidity<u64>,
{
    fn from_with_validity(value: Vec<u64>) -> Self {
        Self::from_iter_with_validity(value)
    }
}

impl<T> FromWithValidity<Vec<Option<u64>>> for T
where
    T: FromIteratorWithValidity<Option<u64>>,
{
    fn from_with_validity(value: Vec<Option<u64>>) -> Self {
        Self::from_iter_with_validity(value)
    }
}

impl<T> FromWithValidity<PrimitiveArray<u64>> for T
where
    T: FromIteratorWithValidity<Option<u64>>,
{
    fn from_with_validity(value: PrimitiveArray<u64>) -> Self {
        Self::from_iter_with_validity(value.iter().map(|v| v.copied()))
    }
}
