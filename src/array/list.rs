use crate::array::CellIndexArray;
use crate::error::Error;
use arrow2::array::{Array, ListArray, PrimitiveArray};
use arrow2::datatypes::DataType;
use std::vec::IntoIter;

pub type H3ListArray = ListArray<i64>;

pub(crate) fn transform_iter_to_listarray<I, T, F, I2>(
    iter: I,
    transform: F,
) -> Result<H3ListArray, Error>
where
    I: Iterator<Item = Option<T>>,
    u64: From<T>,
    F: Fn(T) -> Result<I2, Error>,
    I2: Iterator<Item = T>,
{
    let mut values = vec![];
    let mut offsets = vec![];

    for cell in iter {
        offsets.push(values.len() as i64);
        if let Some(cell) = cell {
            values.extend(transform(cell)?.map(u64::from));
        }
    }
    offsets.push(values.len() as i64);

    H3ListArray::try_new(
        H3ListArray::default_datatype(DataType::UInt64),
        offsets.try_into()?,
        PrimitiveArray::from_vec(values).to_boxed(),
        None,
    )
    .map_err(Error::from)
}

pub(crate) trait IterU64 {
    type Iter: Iterator<Item = u64>;

    fn iter_u64(self) -> Self::Iter;
}

impl<T> IterU64 for Vec<T>
where
    T: Copy,
    u64: From<T>,
{
    type Iter = std::iter::Map<IntoIter<T>, fn(T) -> u64>;

    fn iter_u64(self) -> Self::Iter {
        self.into_iter().map(u64::from)
    }
}

impl<T> IterU64 for Option<Vec<T>>
where
    T: Copy,
    u64: From<T>,
{
    type Iter = std::iter::Map<IntoIter<T>, fn(T) -> u64>;

    fn iter_u64(self) -> Self::Iter {
        self.unwrap_or_default().iter_u64()
    }
}

pub(crate) fn collect_h3listarray<I, T>(iter: I) -> Result<H3ListArray, Error>
where
    I: Iterator<Item = T>,
    T: IterU64,
{
    let mut values = vec![];
    let mut offsets = vec![];

    for vc in iter {
        offsets.push(values.len() as i64);
        values.extend(vc.iter_u64());
    }
    offsets.push(values.len() as i64);

    H3ListArray::try_new(
        H3ListArray::default_datatype(DataType::UInt64),
        offsets.try_into()?,
        PrimitiveArray::from_vec(values).to_boxed(),
        None,
    )
    .map_err(Error::from)
}

pub(crate) fn iter_cellindexarrays(
    array: &H3ListArray,
) -> impl Iterator<Item = Option<Result<CellIndexArray, Error>>> + '_ {
    array.iter().map(|opt| {
        opt.map(|array| {
            array
                .as_any()
                .downcast_ref::<PrimitiveArray<u64>>()
                .ok_or(Error::NotAPrimitiveArrayU64)
                .and_then(|pa| CellIndexArray::try_from(pa.clone()))
        })
    })
}
