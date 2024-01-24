use arrow::array::{GenericListBuilder, UInt64Builder};
use geo_types::*;
use h3o::geom::{ContainmentMode, PolyfillConfig, ToCells};
use h3o::{CellIndex, Resolution};
#[cfg(feature = "rayon")]
use rayon::prelude::{IntoParallelIterator, ParallelIterator};

use crate::array::list::H3ListArray;
use crate::array::{genericlistarray_to_h3listarray_unvalidated, CellIndexArray};
use crate::error::Error;

#[derive(Clone, Copy, Debug)]
pub struct ToCellsOptions {
    pub(crate) polyfill_config: PolyfillConfig,
    pub(crate) compact: bool,
}

impl From<PolyfillConfig> for ToCellsOptions {
    fn from(polyfill_config: PolyfillConfig) -> Self {
        Self::new(polyfill_config)
    }
}

impl ToCellsOptions {
    pub fn new(polyfill_config: PolyfillConfig) -> Self {
        Self {
            polyfill_config,
            compact: false,
        }
    }

    pub fn compact(mut self, compact: bool) -> Self {
        self.compact = compact;
        self
    }
}

impl From<Resolution> for ToCellsOptions {
    fn from(resolution: Resolution) -> Self {
        PolyfillConfig::new(resolution)
            .containment_mode(ContainmentMode::ContainsCentroid)
            .into()
    }
}

pub trait ToClonedGeometry {
    fn to_cloned_geometry(&self) -> Option<Geometry>;
}

impl ToClonedGeometry for Geometry {
    fn to_cloned_geometry(&self) -> Option<Geometry> {
        Some(self.clone())
    }
}

impl ToClonedGeometry for Option<Geometry> {
    fn to_cloned_geometry(&self) -> Option<Geometry> {
        self.clone()
    }
}

impl ToClonedGeometry for Coord {
    fn to_cloned_geometry(&self) -> Option<Geometry> {
        Some(Geometry::from(Point::from(*self)))
    }
}

impl ToClonedGeometry for Option<Coord> {
    fn to_cloned_geometry(&self) -> Option<Geometry> {
        self.as_ref().map(|g| Geometry::from(Point::from(*g)))
    }
}

macro_rules! impl_to_cloned {
    ($($geomtype:ty),*) => {
        $(
        impl ToClonedGeometry for $geomtype {
            fn to_cloned_geometry(&self) -> Option<Geometry> {
                Some(Geometry::from(self.clone()))
            }
        }

        impl ToClonedGeometry for Option<$geomtype> {
            fn to_cloned_geometry(&self) -> Option<Geometry> {
                self.as_ref().map(|g| Geometry::from(g.clone()))
            }
        }
        )*
    };
}

impl_to_cloned!(
    Polygon,
    Point,
    LineString,
    Line,
    Rect,
    Triangle,
    MultiPoint,
    MultiPolygon,
    MultiLineString
);

/// convert to a single `CellIndexArray`
pub trait ToCellIndexArray {
    fn to_cellindexarray(&self, options: &ToCellsOptions) -> Result<CellIndexArray, Error>;
}

pub(crate) trait IterToCellIndexArray {
    fn to_cellindexarray(self, options: &ToCellsOptions) -> Result<CellIndexArray, Error>;
}

#[cfg(feature = "rayon")]
pub(crate) trait ParIterToCellIndexArray {
    fn par_to_cellindexarray(self, options: &ToCellsOptions) -> Result<CellIndexArray, Error>;
}

#[cfg(feature = "rayon")]
impl<T> ParIterToCellIndexArray for T
where
    T: ParallelIterator<Item = Option<Geometry>>,
{
    fn par_to_cellindexarray(self, options: &ToCellsOptions) -> Result<CellIndexArray, Error> {
        let cells = self
            .into_par_iter()
            .try_fold(Vec::new, |acc, geom| match geom {
                Some(geom) => to_cells(geom, options, acc),
                None => Ok(acc),
            })
            .try_reduce(Vec::new, |mut a, mut b| {
                if a.len() > b.len() {
                    a.append(&mut b);
                    Ok(a)
                } else {
                    b.append(&mut a);
                    Ok(b)
                }
            })?;
        Ok(cells.into())
    }
}

impl<T> IterToCellIndexArray for T
where
    T: Iterator<Item = Option<Geometry>>,
{
    fn to_cellindexarray(self, options: &ToCellsOptions) -> Result<CellIndexArray, Error> {
        let cells = self.into_iter().try_fold(vec![], |acc, geom| {
            if let Some(geom) = geom {
                to_cells(geom, options, acc)
            } else {
                Ok(acc)
            }
        })?;
        Ok(cells.into())
    }
}

#[cfg(feature = "rayon")]
impl<T> ToCellIndexArray for &[T]
where
    T: ToClonedGeometry + Sync,
{
    fn to_cellindexarray(&self, options: &ToCellsOptions) -> Result<CellIndexArray, Error> {
        self.into_par_iter()
            .map(|v| v.to_cloned_geometry())
            .par_to_cellindexarray(options)
    }
}

#[cfg(not(feature = "rayon"))]
impl<T> ToCellIndexArray for &[T]
where
    T: ToClonedGeometry,
{
    fn to_cellindexarray(&self, options: &ToCellsOptions) -> Result<CellIndexArray, Error> {
        self.iter()
            .map(|v| v.to_cloned_geometry())
            .to_cellindexarray(options)
    }
}

pub trait ToCellListArray {
    fn to_celllistarray(&self, options: &ToCellsOptions) -> Result<H3ListArray<CellIndex>, Error>;
}

pub(crate) trait IterToCellListArray {
    fn to_celllistarray(self, options: &ToCellsOptions) -> Result<H3ListArray<CellIndex>, Error>;
}

#[cfg(feature = "rayon")]
trait ParIterToCellListArray {
    fn par_to_celllistarray(
        self,
        options: &ToCellsOptions,
    ) -> Result<H3ListArray<CellIndex>, Error>;
}

#[cfg(feature = "rayon")]
impl<T> ParIterToCellListArray for T
where
    T: ParallelIterator<Item = Option<Geometry>>,
{
    fn par_to_celllistarray(
        self,
        options: &ToCellsOptions,
    ) -> Result<H3ListArray<CellIndex>, Error> {
        let cell_vecs = self
            .map(|geom| geom.map(|geom| to_cells(geom, options, vec![])).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        let mut builder = H3ListArrayBuilder::<CellIndex>::default();
        for cells in cell_vecs.into_iter() {
            if let Some(cells) = cells {
                builder.push_valid(cells.into_iter());
            } else {
                builder.push_invalid();
            }
        }
        builder.build()
    }
}

impl<T> IterToCellListArray for T
where
    T: Iterator<Item = Option<Geometry>>,
{
    fn to_celllistarray(self, options: &ToCellsOptions) -> Result<H3ListArray<CellIndex>, Error> {
        let mut builder =
            GenericListBuilder::with_capacity(UInt64Builder::new(), self.size_hint().0);

        for geom in self {
            if let Some(geom) = geom {
                geometry_to_cells(&geom, options)?
                    .into_iter()
                    .for_each(|cell| builder.values().append_value(u64::from(cell)));
                builder.append(true);
            } else {
                builder.append(false);
            }
        }
        genericlistarray_to_h3listarray_unvalidated(builder.finish())
    }
}

#[cfg(feature = "rayon")]
impl<T> ToCellListArray for &[T]
where
    T: ToClonedGeometry + Sync,
{
    fn to_celllistarray(&self, options: &ToCellsOptions) -> Result<H3ListArray<CellIndex>, Error> {
        self.into_par_iter()
            .map(|g| g.to_cloned_geometry())
            .par_to_celllistarray(options)
    }
}

#[cfg(not(feature = "rayon"))]
impl<T> ToCellListArray for &[T]
where
    T: ToClonedGeometry,
{
    fn to_celllistarray(&self, options: &ToCellsOptions) -> Result<H3ListArray<CellIndex>, Error> {
        self.iter()
            .map(|g| g.to_cloned_geometry())
            .to_celllistarray(options)
    }
}

pub fn geometry_to_cells(
    geom: &Geometry,
    options: &ToCellsOptions,
) -> Result<Vec<CellIndex>, Error> {
    let mut cells: Vec<_> = h3o::geom::Geometry::from_degrees(geom.clone())?
        .to_cells(options.polyfill_config)
        .collect();

    // deduplicate, in the case of overlaps or lines
    cells.sort_unstable();
    cells.dedup();

    let cells = if options.compact {
        CellIndex::compact(cells)?.collect()
    } else {
        cells
    };
    Ok(cells)
}

fn to_cells(
    geom: Geometry,
    options: &ToCellsOptions,
    mut acc: Vec<CellIndex>,
) -> Result<Vec<CellIndex>, Error> {
    acc.extend(geometry_to_cells(&geom, options)?);
    Ok(acc)
}

#[cfg(test)]
mod tests {
    use crate::array::from_geo::{ToCellIndexArray, ToCellsOptions};
    use geo_types::Rect;
    use h3o::Resolution;

    #[test]
    fn from_rect() {
        let rect = vec![Rect::new((10., 10.), (20., 20.))];
        let options = ToCellsOptions::from(Resolution::Four);
        let cells = rect.as_slice().to_cellindexarray(&options).unwrap();
        assert!(cells.len() > 400);
        let resolution = cells.resolution();
        assert_eq!(cells.len(), resolution.len());

        for r in resolution.iter() {
            assert_eq!(r, Some(Resolution::Four));
        }
    }
}
