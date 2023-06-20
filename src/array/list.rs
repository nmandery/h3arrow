use crate::array::H3Array;
use crate::error::Error;
use arrow2::array::{Array, ListArray, PrimitiveArray};
use arrow2::bitmap::{Bitmap, MutableBitmap};
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

    pub fn iter_arrays(&self) -> impl Iterator<Item = Option<Result<A, Error>>> + '_ {
        self.list_array.iter().map(|opt| {
            opt.map(|array| {
                array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()
                    // TODO: this should already be validated. unwrap/expect?
                    .ok_or(Error::NotAPrimitiveArrayU64)
                    .and_then(|pa| A::try_from(pa.clone()))
            })
        })
    }

    pub fn into_flattened(self) -> Result<A, Error> {
        // TODO: check validity correctness
        self.list_array
            .values()
            .as_any()
            .downcast_ref::<PrimitiveArray<u64>>()
            // TODO: this should already be validated. unwrap/expect?
            .ok_or(Error::NotAPrimitiveArrayU64)
            .and_then(|pa| A::try_from(pa.clone()))
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

pub struct H3ListArrayBuilder<A> {
    array_phantom: PhantomData<A>,
    values: Vec<u64>,
    offsets: Vec<i64>,
    list_validity: Vec<bool>,
}

impl<A> Default for H3ListArrayBuilder<A>
where
    A: H3Array,
{
    fn default() -> Self {
        Self::with_capacity(100)
    }
}

impl<A> H3ListArrayBuilder<A>
where
    A: H3Array,
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            array_phantom: PhantomData::<A>::default(),
            values: Vec::with_capacity(capacity),
            offsets: Vec::with_capacity(capacity),
            list_validity: Vec::with_capacity(capacity),
        }
    }

    pub fn push_invalid(&mut self) {
        self.offsets.push(self.values.len() as i64);
        self.list_validity.push(false);
    }

    pub fn push_valid<I>(&mut self, it: I)
    where
        I: IterU64<IndexType = A::Index>,
    {
        self.offsets.push(self.values.len() as i64);
        self.values.extend(it.iter_u64());
        self.list_validity.push(true);
    }

    pub fn build(mut self) -> Result<H3ListArray<A>, Error> {
        self.offsets.push(self.values.len() as i64);
        let validity: Bitmap = MutableBitmap::from_iter(self.list_validity.into_iter()).into();
        Ok(H3ListArray {
            list_array: ListArray::try_new(
                ListArray::<i64>::default_datatype(DataType::UInt64),
                self.offsets.try_into()?,
                PrimitiveArray::from_vec(self.values).to_boxed(),
                if validity.unset_bits() == 0 {
                    None
                } else {
                    Some(validity)
                },
            )?,
            array_phantom: PhantomData::<A>::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{CellIndexArray, H3ListArray, H3ListArrayBuilder};
    use h3o::{LatLng, Resolution};

    #[test]
    fn construct() {
        let cell = LatLng::new(23.4, 12.4).unwrap().to_cell(Resolution::Five);

        let mut builder = H3ListArrayBuilder::<CellIndexArray>::default();
        builder.push_valid(cell.grid_disk::<Vec<_>>(1));
        builder.push_invalid();
        builder.push_valid(cell.grid_disk::<Vec<_>>(2));
        let list = builder.build().unwrap();

        /*
        let list = H3ListArray::<CellIndexArray>::try_from_iter(
            [Some(1), None, Some(2)]
                .into_iter()
                .map(|k| k.map(|k| cell.grid_disk::<Vec<_>>(k))),
        )
        .unwrap();

         */
        assert_eq!(list.len(), 3);
        let mut list_iter = list.iter_arrays();
        assert_eq!(list_iter.next().unwrap().unwrap().unwrap().len(), 7);
        assert!(list_iter.next().unwrap().is_none());
        assert_eq!(list_iter.next().unwrap().unwrap().unwrap().len(), 19);
        assert!(list_iter.next().is_none());
        drop(list_iter);

        let cells = list.into_flattened().unwrap();
        assert_eq!(cells.len(), 26);
    }
}
