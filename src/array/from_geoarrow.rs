use super::from_geo::{
    IterToCellIndexArray, IterToCellListArray, ToCellIndexArray, ToCellListArray, ToCellsOptions,
};
use crate::algorithm::CompactOp;
use crate::array::from_geo::geometry_to_cells;
use crate::array::{CellIndexArray, H3ListArray};
use crate::error::Error;
use geo_types::Geometry;
use geoarrow::{GeometryArrayTrait, WKBArray};
use geozero::ToGeo;
#[cfg(feature = "rayon")]
use rayon::prelude::{IntoParallelIterator, ParallelIterator};

macro_rules! impl_from_geoarrow {
    ($($array_type:ty),*) => {
        $(
        impl ToCellListArray for $array_type {
            fn to_celllistarray(&self, options: &ToCellsOptions) -> Result<H3ListArray<CellIndexArray>, Error> {
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

impl ToCellListArray for WKBArray {
    fn to_celllistarray(
        &self,
        options: &ToCellsOptions,
    ) -> Result<H3ListArray<CellIndexArray>, Error> {
        #[cfg(not(feature = "rayon"))]
        let pos_iter = (0..self.len()).into_iter();

        #[cfg(feature = "rayon")]
        let pos_iter = (0..self.len()).into_par_iter();

        let cell_vecs = pos_iter
            .map(|pos| {
                self.get(pos)
                    .map(|wkb| {
                        // Geoarrow currently internally only unwraps the parsed geometry
                        match geozero::wkb::Wkb(wkb.arr.value(wkb.geom_index).to_vec()).to_geo() {
                            Ok(geom) => geometry_to_cells(&geom, options),
                            Err(_) => Err(Error::InvalidWKB),
                        }
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;

        H3ListArray::<CellIndexArray>::try_from_iter(cell_vecs.into_iter())
    }
}

impl ToCellIndexArray for WKBArray {
    fn to_cellindexarray(&self, options: &ToCellsOptions) -> Result<CellIndexArray, Error> {
        let cellindexarray = self.to_celllistarray(options)?.into_flattened()?;

        if options.compact {
            cellindexarray.compact()
        } else {
            Ok(cellindexarray) // may contain duplicates
        }
    }
}
