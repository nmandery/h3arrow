use crate::array::{H3Array, H3IndexArrayValue};
use crate::error::Error;
use arrow::array::{Array, UInt64Array};
use arrow::array::{GenericListArray, OffsetSizeTrait};
use arrow::datatypes::DataType;
use std::marker::PhantomData;

pub struct H3ListArray<IX, O: OffsetSizeTrait = i64> {
    pub(crate) list_array: GenericListArray<O>,
    pub(crate) h3index_phantom: PhantomData<IX>,
}

impl<IX, O: OffsetSizeTrait> H3ListArray<IX, O>
where
    IX: H3IndexArrayValue,
    H3Array<IX>: TryFrom<UInt64Array, Error = Error>,
{
    pub fn listarray(&self) -> &GenericListArray<O> {
        &self.list_array
    }

    pub fn len(&self) -> usize {
        self.list_array.len()
    }

    pub fn is_empty(&self) -> bool {
        self.list_array.is_empty()
    }

    pub fn iter_arrays(&self) -> impl Iterator<Item = Option<Result<H3Array<IX>, Error>>> + '_ {
        self.list_array.iter().map(|opt| {
            opt.map(|array| {
                array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    // TODO: this should already be validated. unwrap/expect?
                    .ok_or(Error::NotAUint64Array)
                    .and_then(|pa| pa.clone().try_into())
            })
        })
    }

    pub fn into_flattened(self) -> Result<H3Array<IX>, Error> {
        // TODO: check validity correctness
        self.list_array
            .values()
            .as_any()
            .downcast_ref::<UInt64Array>()
            // TODO: this should already be validated. unwrap/expect?
            .ok_or(Error::NotAUint64Array)
            .and_then(|pa| pa.clone().try_into())
    }

    pub(crate) fn from_genericlistarray_unvalidated(
        value: GenericListArray<O>,
    ) -> Result<H3ListArray<IX, O>, Error> {
        if value.data_type() != &DataType::UInt64 {
            return Err(Error::NotAUint64Array);
        }

        Ok(Self {
            list_array: value,
            h3index_phantom: PhantomData::<IX>,
        })
    }
}

/*
impl<IX, O: OffsetSizeTrait> From<H3ListArray<IX>> for GenericListArray<O> {
    fn from(value: H3ListArray<IX, O>) -> Self {
        value.list_array
    }
}

 */

pub(crate) fn genericlistarray_to_h3listarray_unvalidated<IX, O: OffsetSizeTrait>(
    value: GenericListArray<O>,
) -> Result<H3ListArray<IX, O>, Error> {
    let nested_datatype = match value.data_type() {
        DataType::List(field_ref) => field_ref.data_type().clone(),
        DataType::LargeList(field_ref) => field_ref.data_type().clone(),
        _ => return Err(Error::NotAUint64Array),
    };
    if !nested_datatype.equals_datatype(&DataType::UInt64) {
        return Err(Error::NotAUint64Array);
    }

    Ok(H3ListArray {
        list_array: value,
        h3index_phantom: PhantomData::<IX>,
    })
}

impl<IX, O: OffsetSizeTrait> TryFrom<GenericListArray<O>> for H3ListArray<IX, O>
where
    IX: H3IndexArrayValue,
    H3Array<IX>: TryFrom<UInt64Array, Error = Error>,
{
    type Error = Error;

    fn try_from(value: GenericListArray<O>) -> Result<Self, Self::Error> {
        let instance = Self::from_genericlistarray_unvalidated(value)?;

        // validate all values
        for a in instance.iter_arrays().flatten() {
            let _ = a?;
        }
        Ok(instance)
    }
}

/*
pub(crate) struct ListArrayBuilder<T: ArrowPrimitiveType, O: OffsetSizeTrait> {
    values: Vec<T>,
    offsets: Vec<O>,
    list_validity: Vec<bool>,
}

impl<T: ArrowPrimitiveType, O: OffsetSizeTrait> Default for ListArrayBuilder<T, O> {
    fn default() -> Self {
        Self::with_capacity(100)
    }
}

impl<T: ArrowPrimitiveType, O: OffsetSizeTrait> ListArrayBuilder<T, O> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
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
        I: IntoIterator<Item = T>,
    {
        self.offsets.push(self.values.len() as i64);
        self.values.extend(it);
        self.list_validity.push(true);
    }

    pub fn build(mut self) -> Result<GenericListArray<O>, Error> {
        self.offsets.push(self.values.len() as i64);
        let validity: Bitmap = MutableBitmap::from_iter(self.list_validity).into();
        Ok(ListArray::try_new(
            ListArray::<i64>::default_datatype(DataType::UInt64),
            self.offsets.try_into()?,
            PrimitiveArray::from_vec(self.values).to_boxed(),
            if validity.unset_bits() == 0 {
                None
            } else {
                Some(validity)
            },
        )?)
    }
}

pub struct H3ListArrayBuilder<IX, O: OffsetSizeTrait = i64> {
    h3index_phantom: PhantomData<IX>,
    builder: ListArrayBuilder<u64, O>,
}

impl<IX, O: OffsetSizeTrait> Default for H3ListArrayBuilder<IX, O>
where
    IX: H3IndexArrayValue,
{
    fn default() -> Self {
        Self::with_capacity(100)
    }
}

impl<IX, O: OffsetSizeTrait> H3ListArrayBuilder<IX, O>
where
    IX: H3IndexArrayValue,
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            h3index_phantom: PhantomData::<IX>,
            builder: ListArrayBuilder::with_capacity(capacity),
        }
    }

    pub fn push_invalid(&mut self) {
        self.builder.push_invalid();
    }

    pub fn push_valid<I>(&mut self, it: I)
    where
        I: IntoIterator<Item = IX>,
    {
        self.builder
            .push_valid(it.into_iter().map(|index| index.into()));
    }

    pub fn extend<I1, I2>(&mut self, it: I1)
    where
        I1: Iterator<Item = Option<I2>>,
        I2: IntoIterator<Item = IX>,
    {
        for sub_iter in it {
            match sub_iter {
                Some(sub_iter) => self.push_valid(sub_iter),
                None => self.push_invalid(),
            }
        }
    }

    pub fn build(self) -> Result<H3ListArray<IX>, Error> {
        Ok(H3ListArray {
            list_array: self.builder.build()?,
            h3index_phantom: PhantomData::<IX>,
        })
    }
}

 */

#[cfg(test)]
mod tests {
    /*
    //use crate::array::H3ListArrayBuilder;
    use h3o::{CellIndex, LatLng, Resolution};

    #[test]
    fn construct() {
        let cell = LatLng::new(23.4, 12.4).unwrap().to_cell(Resolution::Five);

        let mut builder = H3ListArrayBuilder::<CellIndex>::default();
        builder.push_valid(cell.grid_disk::<Vec<_>>(1));
        builder.push_invalid();
        builder.push_valid(cell.grid_disk::<Vec<_>>(2));
        let list = builder.build().unwrap();

        /*
        let list = H3ListArray::<CellIndex>::try_from_iter(
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

     */
}
