use geo_types::*;
use h3o::geom::ToCells;
use h3o::{CellIndex, Resolution};
#[cfg(feature = "rayon")]
use rayon::prelude::{IntoParallelIterator, ParallelIterator};

use crate::array::list::{collect_h3listarray, H3ListArray};
use crate::array::CellIndexArray;
use crate::error::Error;

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
    fn to_cellindexarray(&self, resolution: Resolution) -> Result<CellIndexArray, Error>;
}

pub(crate) trait IterToCellIndexArray {
    fn to_cellindexarray(self, resolution: Resolution) -> Result<CellIndexArray, Error>;
}

#[cfg(feature = "rayon")]
pub(crate) trait ParIterToCellIndexArray {
    fn par_to_cellindexarray(self, resolution: Resolution) -> Result<CellIndexArray, Error>;
}

fn to_cells(
    geom: Geometry,
    resolution: Resolution,
    mut acc: Vec<CellIndex>,
) -> Result<Vec<CellIndex>, Error> {
    let g = h3o::geom::Geometry::from_degrees(geom)?;
    acc.extend(g.to_cells(resolution));
    Ok(acc)
}

#[cfg(feature = "rayon")]
impl<T> ParIterToCellIndexArray for T
where
    T: ParallelIterator<Item = Option<Geometry>>,
{
    fn par_to_cellindexarray(self, resolution: Resolution) -> Result<CellIndexArray, Error> {
        let cells = self
            .into_par_iter()
            .try_fold(Vec::new, |acc, geom| match geom {
                Some(geom) => to_cells(geom, resolution, acc),
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
    fn to_cellindexarray(self, resolution: Resolution) -> Result<CellIndexArray, Error> {
        let cells = self.into_iter().try_fold(vec![], |acc, geom| {
            if let Some(geom) = geom {
                to_cells(geom, resolution, acc)
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
    fn to_cellindexarray(&self, resolution: Resolution) -> Result<CellIndexArray, Error> {
        self.into_par_iter()
            .map(|v| v.to_cloned_geometry())
            .par_to_cellindexarray(resolution)
    }
}

#[cfg(not(feature = "rayon"))]
impl<T> ToCellIndexArray for &[T]
where
    T: ToClonedGeometry,
{
    fn to_cellindexarray(&self, resolution: Resolution) -> Result<CellIndexArray, Error> {
        self.iter()
            .map(|v| v.to_cloned_geometry())
            .to_cellindexarray(resolution)
    }
}

pub trait ToCellListArray {
    fn to_celllistarray(&self, resolution: Resolution) -> Result<H3ListArray, Error>;
}

pub(crate) trait IterToCellListArray {
    fn to_celllistarray(self, resolution: Resolution) -> Result<H3ListArray, Error>;
}

#[cfg(feature = "rayon")]
trait ParIterToCellListArray {
    fn par_to_celllistarray(self, resolution: Resolution) -> Result<H3ListArray, Error>;
}

#[cfg(feature = "rayon")]
impl<T> ParIterToCellListArray for T
where
    T: ParallelIterator<Item = Option<Geometry>>,
{
    fn par_to_celllistarray(self, resolution: Resolution) -> Result<H3ListArray, Error> {
        collect_h3listarray(
            self.map(|geom| {
                geom.map(|geom| to_cells(geom, resolution, vec![]))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter(),
        )
    }
}

impl<T> IterToCellListArray for T
where
    T: Iterator<Item = Option<Geometry>>,
{
    fn to_celllistarray(self, resolution: Resolution) -> Result<H3ListArray, Error> {
        collect_h3listarray(
            self.map(|geom| {
                geom.map(|geom| to_cells(geom, resolution, vec![]))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter(),
        )
    }
}

#[cfg(feature = "rayon")]
impl<T> ToCellListArray for &[T]
where
    T: ToClonedGeometry + Sync,
{
    fn to_celllistarray(&self, resolution: Resolution) -> Result<H3ListArray, Error> {
        self.into_par_iter()
            .map(|g| g.to_cloned_geometry())
            .par_to_celllistarray(resolution)
    }
}

#[cfg(not(feature = "rayon"))]
impl<T> ToCellListArray for &[T]
where
    T: ToClonedGeometry,
{
    fn to_celllistarray(&self, resolution: Resolution) -> Result<H3ListArray, Error> {
        self.iter()
            .map(|g| g.to_cloned_geometry())
            .to_celllistarray(resolution)
    }
}
#[cfg(test)]
mod tests {
    use crate::array::from_geo::ToCellIndexArray;
    use geo_types::Rect;
    use h3o::Resolution;

    #[test]
    fn from_rect() {
        let rect = vec![Rect::new((10., 10.), (20., 20.))];
        let cells = rect.as_slice().to_cellindexarray(Resolution::Four).unwrap();
        assert!(cells.len() > 400);
        let resolution = cells.resolution();
        assert_eq!(cells.len(), resolution.len());

        for r in resolution.iter() {
            assert_eq!(r, Some(Resolution::Four));
        }
    }
}
