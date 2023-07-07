use std::iter::Map;
use std::marker::PhantomData;
use std::mem::transmute;
use std::slice::Iter;
use std::str::FromStr;

use arrow2::array::{Array, PrimitiveArray, Utf8Array};
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::types::Offset;
use h3o::{CellIndex, DirectedEdgeIndex, VertexIndex};

pub use list::*;
pub use resolution::*;
pub use validity::*;

use crate::error::Error;

mod cell;
mod directededge;
pub mod from_geo;
#[cfg(feature = "geoarrow")]
pub mod from_geoarrow;
mod list;
mod resolution;
pub mod to_geo;
#[cfg(feature = "geoarrow")]
pub mod to_geoarrow;
mod validity;
mod vertex;

pub trait H3IndexArrayValue: Into<u64> + TryFrom<u64> + Clone {
    fn transmute_from_u64(value: u64) -> Self;
}

impl H3IndexArrayValue for CellIndex {
    fn transmute_from_u64(value: u64) -> Self {
        unsafe { transmute::<u64, Self>(value) }
    }
}

impl H3IndexArrayValue for VertexIndex {
    fn transmute_from_u64(value: u64) -> Self {
        unsafe { transmute::<u64, Self>(value) }
    }
}

impl H3IndexArrayValue for DirectedEdgeIndex {
    fn transmute_from_u64(value: u64) -> Self {
        unsafe { transmute::<u64, Self>(value) }
    }
}

#[derive(Clone, PartialEq)]
pub struct H3Array<IX> {
    h3index_phantom: PhantomData<IX>,
    primitive_array: PrimitiveArray<u64>,
}

impl<IX> H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    pub fn primitive_array(&self) -> &PrimitiveArray<u64> {
        &self.primitive_array
    }

    pub fn len(&self) -> usize {
        self.primitive_array().len()
    }

    pub fn is_empty(&self) -> bool {
        self.primitive_array().is_empty()
    }

    pub fn slice(&mut self, offset: usize, length: usize) {
        self.primitive_array.slice(offset, length)
    }

    /// Returns an iterator over the values and validity as Option.
    #[allow(clippy::type_complexity)]
    pub fn iter(
        &self,
    ) -> Map<ZipValidity<&u64, Iter<'_, u64>, BitmapIter<'_>>, fn(Option<&u64>) -> Option<IX>> {
        // as the array contents have been validated upon construction, we just transmute to the h3o type
        self.primitive_array
            .iter()
            .map(|h3index| h3index.copied().map(IX::transmute_from_u64))
    }

    /// Returns the element at index `i` or `None` if it is null
    /// # Panics
    /// iff `i >= self.len()`
    pub fn get(self, i: usize) -> Option<IX> {
        self.primitive_array.get(i).map(IX::transmute_from_u64)
    }
}

pub type CellIndexArray = H3Array<CellIndex>;
pub type VertexIndexArray = H3Array<VertexIndex>;
pub type DirectedEdgeIndexArray = H3Array<DirectedEdgeIndex>;

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

impl<IX> TryFrom<PrimitiveArray<u64>> for H3Array<IX>
where
    IX: H3IndexArrayValue + TryFrom<u64>,
    Error: From<<IX as TryFrom<u64>>::Error>,
{
    type Error = Error;

    fn try_from(value: PrimitiveArray<u64>) -> Result<Self, Self::Error> {
        // validate the contained h3 cells
        value
            .iter()
            .flatten()
            .try_for_each(|h3index| IX::try_from(*h3index).map(|_| ()))?;
        Ok(H3Array {
            primitive_array: value,
            h3index_phantom: PhantomData::<IX>::default(),
        })
    }
}

impl<IX> TryFrom<Vec<u64>> for H3Array<IX>
where
    IX: H3IndexArrayValue + TryFrom<u64>,
    Error: From<<IX as TryFrom<u64>>::Error>,
{
    type Error = Error;

    fn try_from(value: Vec<u64>) -> Result<Self, Self::Error> {
        // validate the contained h3 cells
        let validated = value
            .into_iter()
            .map(|h3index| IX::try_from(h3index).map(|v| v.into()))
            .collect::<Result<Vec<u64>, _>>()
            .map_err(Self::Error::from)?;
        Ok(Self {
            primitive_array: PrimitiveArray::from_vec(validated),
            h3index_phantom: PhantomData::<IX>::default(),
        })
    }
}

impl<IX> From<Vec<IX>> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from(value: Vec<IX>) -> Self {
        Self::from_iter(value.into_iter())
    }
}

impl<IX> From<Vec<Option<IX>>> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from(value: Vec<Option<IX>>) -> Self {
        Self::from_iter(value.into_iter())
    }
}

impl<IX> FromIterator<IX> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from_iter<T: IntoIterator<Item = IX>>(iter: T) -> Self {
        Self {
            primitive_array: PrimitiveArray::from_iter(iter.into_iter().map(|v| Some(v.into()))),
            h3index_phantom: PhantomData::<IX>::default(),
        }
    }
}

impl<IX> FromIterator<Option<IX>> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from_iter<T: IntoIterator<Item = Option<IX>>>(iter: T) -> Self {
        Self {
            primitive_array: PrimitiveArray::from_iter(
                iter.into_iter().map(|v| v.map(|v| v.into())),
            ),
            h3index_phantom: PhantomData::<IX>::default(),
        }
    }
}

impl<IX> From<H3Array<IX>> for PrimitiveArray<u64> {
    fn from(v: H3Array<IX>) -> Self {
        v.primitive_array
    }
}

impl<IX> FromIteratorWithValidity<u64> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from_iter_with_validity<T: IntoIterator<Item = u64>>(iter: T) -> Self {
        Self {
            primitive_array: PrimitiveArray::from_iter(
                iter.into_iter()
                    .map(|h3index| IX::try_from(h3index).ok().map(|v| v.into())),
            ),
            h3index_phantom: PhantomData::<IX>::default(),
        }
    }
}

impl<IX> FromIteratorWithValidity<Option<u64>> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from_iter_with_validity<T: IntoIterator<Item = Option<u64>>>(iter: T) -> Self {
        Self {
            primitive_array: PrimitiveArray::from_iter(iter.into_iter().map(|h3index| {
                h3index.and_then(|h3index| IX::try_from(h3index).ok().map(|v| v.into()))
            })),
            h3index_phantom: PhantomData::<IX>::default(),
        }
    }
}

impl<IX> FromWithValidity<Vec<u64>> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from_with_validity(value: Vec<u64>) -> Self {
        Self::from_iter_with_validity(value.into_iter())
    }
}

impl<IX> FromWithValidity<Vec<Option<u64>>> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from_with_validity(value: Vec<Option<u64>>) -> Self {
        Self::from_iter_with_validity(value.into_iter())
    }
}

impl<IX> FromWithValidity<PrimitiveArray<u64>> for H3Array<IX>
where
    IX: H3IndexArrayValue,
{
    fn from_with_validity(value: PrimitiveArray<u64>) -> Self {
        Self::from_iter_with_validity(value.iter().map(|v| v.copied()))
    }
}

impl<IX, O: Offset> TryFrom<Utf8Array<O>> for H3Array<IX>
where
    IX: H3IndexArrayValue + FromStr,
    Error: From<<IX as FromStr>::Err>,
{
    type Error = Error;

    fn try_from(value: Utf8Array<O>) -> Result<Self, Self::Error> {
        value
            .iter()
            .map(|v| v.map(IX::from_str).transpose())
            .collect::<Result<Vec<_>, _>>()
            .map(|v| v.into())
            .map_err(Self::Error::from)
    }
}

impl<IX, O: Offset> FromWithValidity<Utf8Array<O>> for H3Array<IX>
where
    IX: H3IndexArrayValue + FromStr,
{
    fn from_with_validity(value: Utf8Array<O>) -> Self {
        Self::from_iter(
            value
                .iter()
                .map(|v| v.and_then(|index| IX::from_str(index).ok())),
        )
    }
}

impl<IX, O: Offset> From<H3Array<IX>> for Utf8Array<O>
where
    IX: ToString + H3IndexArrayValue,
{
    fn from(value: H3Array<IX>) -> Self {
        value
            .iter()
            .map(|index| index.map(|i| i.to_string()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::CellIndexArray;
    use arrow2::array::Utf8Array;
    use h3o::{LatLng, Resolution};

    #[test]
    fn to_utf8array_roundtrip() {
        let arr: CellIndexArray = vec![
            LatLng::new(23.4, 12.4).unwrap().to_cell(Resolution::Five),
            LatLng::new(12.3, 0.5).unwrap().to_cell(Resolution::Nine),
        ]
        .into();

        let utf8: Utf8Array<i32> = arr.clone().into();
        assert_eq!(utf8.len(), arr.len());

        assert_eq!(
            utf8.iter().flatten().collect::<Vec<_>>(),
            vec!["855968a3fffffff", "89599da10d3ffff"]
        );

        let arr2: CellIndexArray = utf8.try_into().unwrap();
        assert!(arr == arr2);
    }
}
