use super::from_geo::{
    IterToCellIndexArray, IterToCellListArray, ToCellIndexArray, ToCellListArray, ToCellsOptions,
};
use crate::algorithm::CompactOp;
use crate::array::from_geo::geometry_to_cells;
use crate::array::{CellIndexArray, H3ListArray, H3ListArrayBuilder};
use crate::error::Error;
use arrow::array::OffsetSizeTrait;
use arrow::datatypes::ArrowNativeType;
use geo_types::Geometry;
use geoarrow::array::WKBArray;
use geoarrow::trait_::GeoArrayAccessor;
use geoarrow::GeometryArrayTrait;
use h3o::CellIndex;
#[cfg(feature = "rayon")]
use rayon::prelude::{IntoParallelIterator, ParallelIterator};

macro_rules! impl_to_cells {
    ($array_type:ty, $offset:tt) => {
        impl<$offset: OffsetSizeTrait + ArrowNativeType> ToCellListArray for $array_type {
            fn to_celllistarray(
                &self,
                options: &ToCellsOptions,
            ) -> Result<H3ListArray<CellIndex, $offset>, Error> {
                self.iter_geo()
                    .map(|v| v.map(Geometry::from))
                    .to_celllistarray(options)
            }
        }

        impl<$offset: OffsetSizeTrait + ArrowNativeType> ToCellIndexArray for $array_type {
            fn to_cellindexarray(&self, options: &ToCellsOptions) -> Result<CellIndexArray, Error> {
                self.iter_geo()
                    .map(|v| v.map(Geometry::from))
                    .to_cellindexarray(options)
            }
        }
    };
    ($array_type:ty) => {
        impl ToCellListArray for $array_type {
            fn to_celllistarray(
                &self,
                options: &ToCellsOptions,
            ) -> Result<H3ListArray<CellIndex>, Error> {
                self.iter_geo()
                    .map(|v| v.map(Geometry::from))
                    .to_celllistarray(options)
            }
        }

        impl ToCellIndexArray for $array_type {
            fn to_cellindexarray(&self, options: &ToCellsOptions) -> Result<CellIndexArray, Error> {
                self.iter_geo()
                    .map(|v| v.map(Geometry::from))
                    .to_cellindexarray(options)
            }
        }
    };
}

impl_to_cells!(geoarrow::array::LineStringArray<O>, O);
impl_to_cells!(geoarrow::array::MultiLineStringArray<O>, O);
impl_to_cells!(geoarrow::array::MultiPointArray<O>, O);
impl_to_cells!(geoarrow::array::MultiPolygonArray<O>, O);
impl_to_cells!(geoarrow::array::PointArray);
impl_to_cells!(geoarrow::array::PolygonArray<O>, O);

impl<O: OffsetSizeTrait> ToCellListArray for WKBArray<O> {
    fn to_celllistarray(&self, options: &ToCellsOptions) -> Result<H3ListArray<CellIndex>, Error> {
        #[cfg(not(feature = "rayon"))]
        let pos_iter = (0..self.len()).into_iter();

        #[cfg(feature = "rayon")]
        let pos_iter = (0..self.len()).into_par_iter();

        let cell_vecs = pos_iter
            .map(|pos| {
                self.get_as_geo(pos)
                    .map(|geom| geometry_to_cells(&geom, options))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut builder = H3ListArrayBuilder::<CellIndex>::default();
        for cells in cell_vecs.into_iter() {
            if let Some(cells) = cells {
                builder.push_valid(cells.into_iter())
            } else {
                builder.push_invalid()
            }
        }
        builder.build()
    }
}

impl<O: OffsetSizeTrait> ToCellIndexArray for WKBArray<O> {
    fn to_cellindexarray(&self, options: &ToCellsOptions) -> Result<CellIndexArray, Error> {
        let cellindexarray = self.to_celllistarray(options)?.into_flattened()?;

        if options.compact {
            cellindexarray.compact()
        } else {
            Ok(cellindexarray) // may contain duplicates
        }
    }
}
