use crate::array::H3Array;
use crate::error::Error;
use arrow2::array::{Array, ListArray, PrimitiveArray};
use arrow2::datatypes::DataType;
use std::marker::PhantomData;

pub struct H3ListArray<A> {
    list_array: ListArray<i64>,
    array_phantom: PhantomData<A>,
}

impl<A> H3ListArray<A>
where
    A: H3Array + TryFrom<PrimitiveArray<u64>, Error = Error>,
{
    pub fn into_inner(self) -> ListArray<i64> {
        self.list_array
    }

    pub fn len(&self) -> usize {
        self.list_array.len()
    }

    pub fn is_empty(&self) -> bool {
        self.list_array.is_empty()
    }

    pub fn try_from_iter<I1, I2>(iter: I1) -> Result<Self, Error>
    where
        I1: Iterator<Item = Option<I2>>,
        I2: IterU64<IndexType = A::Index>,
    {
        let mut values = vec![];
        let mut offsets = vec![];

        for vc in iter {
            offsets.push(values.len() as i64);
            if let Some(vc) = vc {
                values.extend(vc.iter_u64());
            }
        }
        offsets.push(values.len() as i64);

        Ok(Self {
            list_array: ListArray::try_new(
                ListArray::<i64>::default_datatype(DataType::UInt64),
                offsets.try_into()?,
                PrimitiveArray::from_vec(values).to_boxed(),
                None,
            )?,
            array_phantom: PhantomData::<A>::default(),
        })
    }

    pub fn iter_arrays(&self) -> impl Iterator<Item = Option<Result<A, Error>>> + '_ {
        self.list_array.iter().map(|opt| {
            opt.map(|array| {
                array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()
                    .ok_or(Error::NotAPrimitiveArrayU64)
                    .and_then(|pa| A::try_from(pa.clone()))
            })
        })
    }
}

impl<A> From<H3ListArray<A>> for ListArray<i64> {
    fn from(value: H3ListArray<A>) -> Self {
        value.list_array
    }
}

impl<A> TryFrom<ListArray<i64>> for H3ListArray<A>
where
    A: H3Array + TryFrom<PrimitiveArray<u64>, Error = Error>,
{
    type Error = Error;

    fn try_from(value: ListArray<i64>) -> Result<Self, Self::Error> {
        let instance = Self {
            list_array: value,
            array_phantom: PhantomData::<A>::default(),
        };

        for a in instance.iter_arrays().flatten() {
            let _ = a?;
        }
        Ok(instance)
    }
}

pub trait IterU64 {
    type IndexType;
    type Iter: Iterator<Item = u64>;

    fn iter_u64(self) -> Self::Iter;
}

impl<T, I> IterU64 for I
where
    I: IntoIterator<Item = T>,
    T: Copy,
    u64: From<T>,
{
    type IndexType = T;
    type Iter = std::iter::Map<I::IntoIter, fn(T) -> u64>;

    fn iter_u64(self) -> Self::Iter {
        self.into_iter().map(u64::from)
    }
}
