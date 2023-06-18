use ahash::HashSet;
use geo::Intersects;
use geo_types::*;
use h3o::geom::{ToCells, ToGeo};
use h3o::{CellIndex, Resolution};
#[cfg(feature = "rayon")]
use rayon::prelude::{IntoParallelIterator, ParallelIterator};

use crate::array::list::H3ListArray;
use crate::array::CellIndexArray;
use crate::error::Error;

pub struct ToCellsOptions {
    pub resolution: Resolution,
    pub compact: bool,
    pub all_intersecting: bool,
}

impl From<Resolution> for ToCellsOptions {
    fn from(resolution: Resolution) -> Self {
        Self {
            resolution,
            compact: false,
            all_intersecting: false,
        }
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
    fn to_celllistarray(
        &self,
        options: &ToCellsOptions,
    ) -> Result<H3ListArray<CellIndexArray>, Error>;
}

pub(crate) trait IterToCellListArray {
    fn to_celllistarray(
        self,
        options: &ToCellsOptions,
    ) -> Result<H3ListArray<CellIndexArray>, Error>;
}

#[cfg(feature = "rayon")]
trait ParIterToCellListArray {
    fn par_to_celllistarray(
        self,
        options: &ToCellsOptions,
    ) -> Result<H3ListArray<CellIndexArray>, Error>;
}

#[cfg(feature = "rayon")]
impl<T> ParIterToCellListArray for T
where
    T: ParallelIterator<Item = Option<Geometry>>,
{
    fn par_to_celllistarray(
        self,
        options: &ToCellsOptions,
    ) -> Result<H3ListArray<CellIndexArray>, Error> {
        H3ListArray::try_from_iter(
            self.map(|geom| geom.map(|geom| to_cells(geom, options, vec![])).transpose())
                .collect::<Result<Vec<_>, _>>()?
                .into_iter(),
        )
    }
}

impl<T> IterToCellListArray for T
where
    T: Iterator<Item = Option<Geometry>>,
{
    fn to_celllistarray(
        self,
        options: &ToCellsOptions,
    ) -> Result<H3ListArray<CellIndexArray>, Error> {
        H3ListArray::try_from_iter(
            self.map(|geom| geom.map(|geom| to_cells(geom, options, vec![])).transpose())
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
    fn to_celllistarray(
        &self,
        options: &ToCellsOptions,
    ) -> Result<H3ListArray<CellIndexArray>, Error> {
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
    fn to_celllistarray(
        &self,
        options: &ToCellsOptions,
    ) -> Result<H3ListArray<CellIndexArray>, Error> {
        self.iter()
            .map(|g| g.to_cloned_geometry())
            .to_celllistarray(options)
    }
}

pub fn geometry_to_cells(
    geom: &Geometry,
    options: &ToCellsOptions,
) -> Result<Vec<CellIndex>, Error> {
    let mut cells: Vec<CellIndex> = match (geom, options.all_intersecting) {
        (Geometry::Polygon(poly), true) => {
            let mut cells = Vec::new();
            fill_including_intersecting(&mut cells, poly, options.resolution)?;
            cells
        }
        (Geometry::MultiPolygon(mpoly), true) => {
            let mut cells = Vec::new();
            for poly in mpoly.0.iter() {
                fill_including_intersecting(&mut cells, poly, options.resolution)?;
            }
            cells
        }
        _ => h3o::geom::Geometry::from_degrees(geom.clone())?
            .to_cells(options.resolution)
            .collect::<Vec<_>>(),
    };

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

fn fill_including_intersecting(
    sink: &mut Vec<CellIndex>,
    poly: &Polygon,
    resolution: Resolution,
) -> Result<(), Error> {
    let mut ring_cells: Vec<_> = h3o::geom::LineString::from_degrees(poly.exterior().clone())?
        .to_cells(resolution)
        .collect();
    for interior_ring in poly.interiors() {
        ring_cells.extend(
            h3o::geom::LineString::from_degrees(interior_ring.clone())?.to_cells(resolution),
        );
    }
    ring_cells.sort_unstable();
    ring_cells.dedup();

    let mut cells: HashSet<_> = h3o::geom::Polygon::from_degrees(poly.clone())?
        .to_cells(resolution)
        .collect();

    let mut non_intersecting_cells = HashSet::default();
    for ring_cell in ring_cells {
        let disk_cells: Vec<_> = ring_cell.grid_disk(1);
        for disk_cell in disk_cells {
            if cells.contains(&disk_cell) || non_intersecting_cells.contains(&disk_cell) {
                continue;
            }
            let disk_poly = disk_cell.to_geom(true).unwrap();
            if poly.intersects(&disk_poly) {
                cells.insert(disk_cell);
            } else {
                non_intersecting_cells.insert(disk_cell);
            }
        }
    }

    sink.extend(cells.into_iter());
    Ok(())
}

fn to_cells(
    geom: Geometry,
    options: &ToCellsOptions,
    mut acc: Vec<CellIndex>,
) -> Result<Vec<CellIndex>, Error> {
    acc.extend(geometry_to_cells(&geom, options)?.into_iter());
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
