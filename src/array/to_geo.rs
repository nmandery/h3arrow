use crate::array::{CellIndexArray, DirectedEdgeIndexArray, H3ListArray, VertexIndexArray};
use crate::error::Error;
use geo::CoordsIter;
use geo_types::{Coord, Line, LineString, MultiPoint, MultiPolygon, Point, Polygon};
use h3o::geom::ToGeo;
use h3o::LatLng;
use std::convert::Infallible;

pub trait ToPolygons {
    type Error;
    fn to_polygons(&self, use_degrees: bool) -> Result<Vec<Option<Polygon>>, Self::Error>;
}

impl ToPolygons for CellIndexArray {
    type Error = Infallible;

    fn to_polygons(&self, use_degrees: bool) -> Result<Vec<Option<Polygon>>, Self::Error> {
        Ok(self
            .map_values(|cell| cell.to_geom(use_degrees).ok())
            .collect())
    }
}

pub trait ToPoints {
    type Error;
    fn to_points(&self, use_degrees: bool) -> Result<Vec<Option<Point>>, Self::Error>;
}

macro_rules! impl_to_points {
    ($($array:ty),*) => {
        $(
        impl ToPoints for $array {
            type Error = Infallible;

            fn to_points(&self, use_degrees: bool) -> Result<Vec<Option<Point>>, Self::Error> {
                Ok(self.map_values(|cell| {
                    let ll = LatLng::from(cell);
                    Some(
                        if use_degrees {
                            Coord {
                                x: ll.lng(),
                                y: ll.lng(),
                            }
                        } else {
                            Coord {
                                x: ll.lng_radians(),
                                y: ll.lat_radians(),
                            }
                        }
                        .into(),
                    )
                })
                .collect())
            }
        }
        )*
    };
}

impl_to_points!(CellIndexArray, VertexIndexArray);

pub trait ToLines {
    type Error;
    fn to_lines(&self, use_degrees: bool) -> Result<Vec<Option<Line>>, Self::Error>;
}

impl ToLines for DirectedEdgeIndexArray {
    type Error = Infallible;

    fn to_lines(&self, use_degrees: bool) -> Result<Vec<Option<Line>>, Self::Error> {
        Ok(self
            .map_values(|cell| cell.to_geom(use_degrees).ok())
            .collect())
    }
}

pub trait ToLineStrings {
    type Error;
    fn to_linestrings(&self, use_degrees: bool) -> Result<Vec<Option<LineString>>, Self::Error>;
}

impl ToLineStrings for DirectedEdgeIndexArray {
    type Error = Infallible;
    fn to_linestrings(&self, use_degrees: bool) -> Result<Vec<Option<LineString>>, Self::Error> {
        Ok(self
            .to_lines(use_degrees)?
            .into_iter()
            .map(|l| l.map(LineString::from))
            .collect())
    }
}

pub trait ToMultiPolygons {
    type Error;
    type Output;
    fn to_multipolygons(&self, use_degrees: bool) -> Result<Self::Output, Self::Error>;
}

impl ToMultiPolygons for H3ListArray<CellIndexArray> {
    type Error = Error;
    type Output = Vec<Option<MultiPolygon>>;

    fn to_multipolygons(&self, use_degrees: bool) -> Result<Self::Output, Self::Error> {
        self.iter_arrays()
            .map(|opt| {
                opt.map(|res| {
                    res.and_then(|array| {
                        array
                            .to_multipolygons(use_degrees)
                            .map_err(Self::Error::from)
                    })
                })
                .transpose()
            })
            .collect()
    }
}

impl ToMultiPolygons for CellIndexArray {
    type Error = Error;
    type Output = MultiPolygon;

    fn to_multipolygons(&self, use_degrees: bool) -> Result<Self::Output, Self::Error> {
        self.iter()
            .flatten()
            .to_geom(use_degrees)
            .map_err(Into::into)
    }
}

/// used as base for the algorithms of the `geo` crate
pub(crate) fn directededgeindexarray_to_multipoint(array: &DirectedEdgeIndexArray) -> MultiPoint {
    MultiPoint::new(
        array
            .to_lines(true)
            .expect("line vec")
            .into_iter()
            .flatten()
            .flat_map(|line| line.coords_iter().map(Point::from))
            .collect(),
    )
}

/// used as base for the algorithms of the `geo` crate
pub(crate) fn vertexindexarray_to_multipoint(array: &VertexIndexArray) -> MultiPoint {
    MultiPoint::new(
        array
            .to_points(true)
            .expect("point vec")
            .into_iter()
            .flatten()
            .collect(),
    )
}

pub(crate) fn cellindexarray_to_multipolygon(array: &CellIndexArray) -> MultiPolygon {
    MultiPolygon::new(
        array
            .to_polygons(true)
            .expect("polygon vec")
            .into_iter()
            .flatten()
            .collect(),
    )
}
