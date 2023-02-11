use super::from_geo::{
    IterToCellIndexArray, IterToCellListArray, ToCellIndexArray, ToCellListArray,
};
use crate::array::{CellIndexArray, H3ListArray};
use crate::error::Error;
use geo_types::Geometry;
use h3o::Resolution;

macro_rules! impl_from_geoarrow {
    ($($array_type:ty),*) => {
        $(
        impl ToCellListArray for $array_type {
            fn to_celllistarray(&self, resolution: Resolution) -> Result<H3ListArray, Error> {
                self.iter_geo()
                    .map(|v| v.map(Geometry::from))
                    .to_celllistarray(resolution)
            }
        }

        impl ToCellIndexArray for $array_type {
            fn to_cellindexarray(&self, resolution: Resolution) -> Result<CellIndexArray, Error> {
                self.iter_geo()
                    .map(|v| v.map(Geometry::from))
                    .to_cellindexarray(resolution)
            }
        }

        )*
    };
}

impl_from_geoarrow!(
    geoarrow::LineStringArray,
    geoarrow::MultiLineStringArray,
    geoarrow::MultiPointArray,
    geoarrow::MultiPolygonArray,
    geoarrow::PointArray,
    geoarrow::PolygonArray
);
