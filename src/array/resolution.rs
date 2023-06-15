use std::mem::transmute;

use arrow2::array::{Array, PrimitiveArray};
use h3o::Resolution;

use crate::error::Error;

use super::{FromIteratorWithValidity, FromWithValidity};

pub struct ResolutionArray(PrimitiveArray<u8>);

impl TryFrom<PrimitiveArray<u8>> for ResolutionArray {
    type Error = Error;

    fn try_from(value: PrimitiveArray<u8>) -> Result<Self, Self::Error> {
        // validate the contained h3 cells
        value
            .iter()
            .flatten()
            .try_for_each(|h3index| Resolution::try_from(*h3index).map(|_| ()))?;
        Ok(Self(value))
    }
}

impl ResolutionArray {
    /// Returns an iterator over the values and validity, Option.
    pub fn iter(&self) -> impl Iterator<Item = Option<Resolution>> + '_ {
        // as the array contents have been validated upon construction, we just transmute to the h3o type
        self.0
            .iter()
            .map(|v| v.map(|resolution_u8| unsafe { transmute::<u8, Resolution>(*resolution_u8) }))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn slice(&mut self, offset: usize, length: usize) {
        self.0.slice(offset, length);
    }

    pub fn area_rads2(&self) -> PrimitiveArray<f64> {
        PrimitiveArray::from_iter(self.iter().map(|v| v.map(|r| r.area_rads2())))
    }

    pub fn area_km2(&self) -> PrimitiveArray<f64> {
        PrimitiveArray::from_iter(self.iter().map(|v| v.map(|r| r.area_km2())))
    }

    pub fn area_m2(&self) -> PrimitiveArray<f64> {
        PrimitiveArray::from_iter(self.iter().map(|v| v.map(|r| r.area_m2())))
    }

    pub fn edge_length_rads(&self) -> PrimitiveArray<f64> {
        PrimitiveArray::from_iter(self.iter().map(|v| v.map(|r| r.edge_length_rads())))
    }

    pub fn edge_length_km(&self) -> PrimitiveArray<f64> {
        PrimitiveArray::from_iter(self.iter().map(|v| v.map(|r| r.edge_length_km())))
    }

    pub fn edge_length_m(&self) -> PrimitiveArray<f64> {
        PrimitiveArray::from_iter(self.iter().map(|v| v.map(|r| r.edge_length_m())))
    }

    pub fn cell_count(&self) -> PrimitiveArray<u64> {
        PrimitiveArray::from_iter(self.iter().map(|v| v.map(|r| r.cell_count())))
    }

    /// Return the next resolution, if any.
    pub fn succ(&self) -> Self {
        Self::from_iter(self.iter().map(|v| v.and_then(|r| r.succ())))
    }

    /// Return the previous resolution, if any.
    pub fn pred(&self) -> Self {
        Self::from_iter(self.iter().map(|v| v.and_then(|r| r.pred())))
    }

    pub fn into_inner(self) -> PrimitiveArray<u8> {
        self.0
    }
}

impl FromIterator<Resolution> for ResolutionArray {
    fn from_iter<T: IntoIterator<Item = Resolution>>(iter: T) -> Self {
        Self(PrimitiveArray::from_iter(
            iter.into_iter().map(|v| Some(u8::from(v))),
        ))
    }
}

impl FromIterator<Option<Resolution>> for ResolutionArray {
    fn from_iter<T: IntoIterator<Item = Option<Resolution>>>(iter: T) -> Self {
        Self(PrimitiveArray::from_iter(
            iter.into_iter().map(|v| v.map(u8::from)),
        ))
    }
}

impl From<Vec<Resolution>> for ResolutionArray {
    fn from(value: Vec<Resolution>) -> Self {
        Self::from_iter(value.into_iter())
    }
}

impl From<Vec<Option<Resolution>>> for ResolutionArray {
    fn from(value: Vec<Option<Resolution>>) -> Self {
        Self::from_iter(value.into_iter())
    }
}

impl From<ResolutionArray> for PrimitiveArray<u8> {
    fn from(value: ResolutionArray) -> Self {
        value.0
    }
}

impl FromIteratorWithValidity<u8> for ResolutionArray {
    fn from_iter_with_validity<T: IntoIterator<Item = u8>>(iter: T) -> Self {
        Self(PrimitiveArray::from_iter(
            iter.into_iter()
                .map(|v| Resolution::try_from(v).ok().map(u8::from)),
        ))
    }
}

impl FromIteratorWithValidity<Option<u8>> for ResolutionArray {
    fn from_iter_with_validity<T: IntoIterator<Item = Option<u8>>>(iter: T) -> Self {
        Self(PrimitiveArray::from_iter(iter.into_iter().map(|v| {
            v.and_then(|v| Resolution::try_from(v).ok().map(u8::from))
        })))
    }
}

impl FromWithValidity<Vec<u8>> for ResolutionArray {
    fn from_with_validity(value: Vec<u8>) -> Self {
        Self::from_iter_with_validity(value.into_iter())
    }
}

impl FromWithValidity<Vec<Option<u8>>> for ResolutionArray {
    fn from_with_validity(value: Vec<Option<u8>>) -> Self {
        Self::from_iter_with_validity(value.into_iter())
    }
}
