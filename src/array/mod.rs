use std::str::FromStr;

use arrow2::array::{Array, PrimitiveArray, Utf8Array};
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

#[derive(Clone, PartialEq)]
pub struct CellIndexArray(PrimitiveArray<u64>);

#[derive(Clone, PartialEq)]
pub struct DirectedEdgeIndexArray(PrimitiveArray<u64>);

#[derive(Clone, PartialEq)]
pub struct VertexIndexArray(PrimitiveArray<u64>);

pub trait H3Array: TryFrom<PrimitiveArray<u64>, Error = Error> {
    type Index: Sized + Into<u64> + TryFrom<u64> + Copy;

    fn primitive_array(&self) -> &PrimitiveArray<u64>;
}

impl H3Array for DirectedEdgeIndexArray {
    type Index = DirectedEdgeIndex;

    fn primitive_array(&self) -> &PrimitiveArray<u64> {
        &self.0
    }
}

impl H3Array for CellIndexArray {
    type Index = CellIndex;

    fn primitive_array(&self) -> &PrimitiveArray<u64> {
        &self.0
    }
}

impl H3Array for VertexIndexArray {
    type Index = VertexIndex;

    fn primitive_array(&self) -> &PrimitiveArray<u64> {
        &self.0
    }
}

macro_rules! impl_conv {
    ($array_type:ty, $h3index_type:ty) => {
        impl TryFrom<PrimitiveArray<u64>> for $array_type {
            type Error = Error;

            fn try_from(value: PrimitiveArray<u64>) -> Result<Self, Self::Error> {
                // validate the contained h3 cells
                value
                    .iter()
                    .flatten()
                    .try_for_each(|h3index| <$h3index_type>::try_from(*h3index).map(|_| ()))?;
                Ok(Self(value))
            }
        }

        impl TryFrom<Vec<u64>> for $array_type {
            type Error = Error;

            fn try_from(value: Vec<u64>) -> Result<Self, Self::Error> {
                // validate the contained h3 cells
                let validated = value
                    .into_iter()
                    .map(|h3index| <$h3index_type>::try_from(h3index).map(u64::from))
                    .collect::<Result<Vec<u64>, _>>()?;
                Ok(Self(PrimitiveArray::from_vec(validated)))
            }
        }

        impl From<Vec<$h3index_type>> for $array_type {
            fn from(value: Vec<$h3index_type>) -> Self {
                Self::from_iter(value.into_iter())
            }
        }

        impl From<Vec<Option<$h3index_type>>> for $array_type {
            fn from(value: Vec<Option<$h3index_type>>) -> Self {
                Self::from_iter(value.into_iter())
            }
        }

        impl FromIterator<$h3index_type> for $array_type {
            fn from_iter<T: IntoIterator<Item = $h3index_type>>(iter: T) -> Self {
                Self(PrimitiveArray::from_iter(
                    iter.into_iter().map(|cell| Some(u64::from(cell))),
                ))
            }
        }

        impl FromIterator<Option<$h3index_type>> for $array_type {
            fn from_iter<T: IntoIterator<Item = Option<$h3index_type>>>(iter: T) -> Self {
                Self(PrimitiveArray::from_iter(
                    iter.into_iter().map(|cell| cell.map(u64::from)),
                ))
            }
        }

        impl FromIteratorWithValidity<u64> for $array_type {
            fn from_iter_with_validity<T: IntoIterator<Item = u64>>(iter: T) -> Self {
                Self(PrimitiveArray::from_iter(iter.into_iter().map(|h3index| {
                    <$h3index_type>::try_from(h3index).ok().map(u64::from)
                })))
            }
        }

        impl FromIteratorWithValidity<Option<u64>> for $array_type {
            fn from_iter_with_validity<T: IntoIterator<Item = Option<u64>>>(iter: T) -> Self {
                Self(PrimitiveArray::from_iter(iter.into_iter().map(|h3index| {
                    h3index
                        .and_then(|h3index| <$h3index_type>::try_from(h3index).ok().map(u64::from))
                })))
            }
        }

        impl From<$array_type> for PrimitiveArray<u64> {
            fn from(v: $array_type) -> Self {
                v.0
            }
        }

        impl<O: Offset> TryFrom<Utf8Array<O>> for $array_type {
            type Error = Error;

            fn try_from(value: Utf8Array<O>) -> Result<Self, Self::Error> {
                value
                    .iter()
                    .map(|v| v.map(<$h3index_type>::from_str).transpose())
                    .collect::<Result<Vec<_>, _>>()
                    .map(|v| v.into())
                    .map_err(Error::from)
            }
        }

        impl<O: Offset> FromWithValidity<Utf8Array<O>> for $array_type {
            fn from_with_validity(value: Utf8Array<O>) -> Self {
                Self::from_iter(
                    value
                        .iter()
                        .map(|v| v.and_then(|index| <$h3index_type>::from_str(index).ok())),
                )
            }
        }

        impl<O: Offset> From<$array_type> for Utf8Array<O> {
            fn from(value: $array_type) -> Self {
                value.map_values(|index| Some(index.to_string())).collect()
            }
        }
    };
}

macro_rules! impl_methods {
    ($array_type:ty, $h3index_type:ty) => {
        impl $array_type {
            /// Returns an iterator over the values and validity as Option.
            pub fn iter(&self) -> impl Iterator<Item = Option<$h3index_type>> + '_ {
                // as the array contents have been validated upon construction, we just transmute to the h3o type
                self.0.iter().map(|h3index| {
                    h3index.map(|hi| unsafe { std::mem::transmute::<u64, $h3index_type>(*hi) })
                })
            }

            pub fn len(&self) -> usize {
                self.0.len()
            }

            pub fn is_empty(&self) -> bool {
                self.0.is_empty()
            }

            pub fn slice(&mut self, offset: usize, length: usize) {
                self.0.slice(offset, length)
            }

            pub(crate) fn map_values<'s, F: 's, O>(
                &'s self,
                transform: F,
            ) -> impl Iterator<Item = Option<O>> + 's
            where
                F: Fn($h3index_type) -> Option<O>,
            {
                self.iter().map(move |v| v.and_then(&transform))
            }

            /*
            pub(crate) fn try_map_values_collect<'s, F: 's, O, C>(
                &'s self,
                transform: F,
            ) -> Result<C, Error>
            where
                C: FromIterator<Option<O>>,
                F: Fn($h3index_type) -> Result<Option<O>, Error>,
            {
                self.iter()
                    .map(move |v| match v {
                        Some(v) => transform(v),
                        _ => Ok(None),
                    })
                    .collect::<Result<C, _>>()
            }

             */
            pub fn into_inner(self) -> PrimitiveArray<u64> {
                self.0
            }
        }
    };
}

macro_rules! impl_full {
    ($array_type:ty, $h3index_type:ty) => {
        impl_conv!($array_type, $h3index_type);
        impl_methods!($array_type, $h3index_type);
    };
}

impl_full!(CellIndexArray, CellIndex);
impl_full!(DirectedEdgeIndexArray, DirectedEdgeIndex);
impl_full!(VertexIndexArray, VertexIndex);

#[cfg(test)]
mod tests {
    use arrow2::array::Utf8Array;
    use h3o::{LatLng, Resolution};

    use super::CellIndexArray;

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
