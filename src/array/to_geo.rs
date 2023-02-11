use crate::array::list::iter_cellindexarrays;
use crate::array::{CellIndexArray, DirectedEdgeIndexArray, H3ListArray, VertexIndexArray};
use crate::error::Error;
use geo::CoordsIter;
use geo_types::{Coord, Line, LineString, MultiPoint, MultiPolygon, Point, Polygon};
use h3o::geom::ToGeo;
use h3o::LatLng;
use std::convert::Infallible;

pub trait ToGeoPolygons {
    type Error;
    fn to_geo_polygons(&self, use_degrees: bool) -> Result<Vec<Option<Polygon>>, Self::Error>;
}

impl ToGeoPolygons for CellIndexArray {
    type Error = Infallible;

    fn to_geo_polygons(&self, use_degrees: bool) -> Result<Vec<Option<Polygon>>, Self::Error> {
        Ok(self
            .map_values(|cell| cell.to_geom(use_degrees).ok())
            .collect())
    }
}

pub trait ToGeoPoints {
    type Error;
    fn to_geo_points(&self, use_degrees: bool) -> Result<Vec<Option<Point>>, Self::Error>;
}

macro_rules! impl_to_geo_points {
    ($($array:ty),*) => {
        $(
        impl ToGeoPoints for $array {
            type Error = Infallible;

            fn to_geo_points(&self, use_degrees: bool) -> Result<Vec<Option<Point>>, Self::Error> {
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

impl_to_geo_points!(CellIndexArray, VertexIndexArray);

pub trait ToGeoLines {
    type Error;
    fn to_geo_lines(&self, use_degrees: bool) -> Result<Vec<Option<Line>>, Self::Error>;
}

impl ToGeoLines for DirectedEdgeIndexArray {
    type Error = Infallible;

    fn to_geo_lines(&self, use_degrees: bool) -> Result<Vec<Option<Line>>, Self::Error> {
        Ok(self
            .map_values(|cell| cell.to_geom(use_degrees).ok())
            .collect())
    }
}

pub trait ToGeoLineStrings {
    type Error;
    fn to_geo_linestrings(&self, use_degrees: bool)
        -> Result<Vec<Option<LineString>>, Self::Error>;
}

impl ToGeoLineStrings for DirectedEdgeIndexArray {
    type Error = Infallible;
    fn to_geo_linestrings(
        &self,
        use_degrees: bool,
    ) -> Result<Vec<Option<LineString>>, Self::Error> {
        Ok(self
            .to_geo_lines(use_degrees)?
            .into_iter()
            .map(|l| l.map(LineString::from))
            .collect())
    }
}

pub trait ToGeoMultiPolygons {
    type Error;
    type Output;
    fn to_geo_multipolygons(&self, use_degrees: bool) -> Result<Self::Output, Self::Error>;
}

impl ToGeoMultiPolygons for H3ListArray {
    type Error = Error;
    type Output = Vec<Option<MultiPolygon>>;

    fn to_geo_multipolygons(&self, use_degrees: bool) -> Result<Self::Output, Self::Error> {
        iter_cellindexarrays(self)
            .map(|opt| {
                opt.map(|res| {
                    res.and_then(|array| {
                        array
                            .to_geo_multipolygons(use_degrees)
                            .map_err(Self::Error::from)
                    })
                })
                .transpose()
            })
            .collect()
    }
}

impl ToGeoMultiPolygons for CellIndexArray {
    type Error = Error;
    type Output = MultiPolygon;

    fn to_geo_multipolygons(&self, use_degrees: bool) -> Result<Self::Output, Self::Error> {
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
            .to_geo_lines(true)
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
            .to_geo_points(true)
            .expect("point vec")
            .into_iter()
            .flatten()
            .collect(),
    )
}

pub(crate) fn cellindexarray_to_multipolygon(array: &CellIndexArray) -> MultiPolygon {
    MultiPolygon::new(
        array
            .to_geo_polygons(true)
            .expect("polygon vec")
            .into_iter()
            .flatten()
            .collect(),
    )
}
