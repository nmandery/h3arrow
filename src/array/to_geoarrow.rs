use crate::array::to_geo::{ToGeoLineStrings, ToGeoPoints, ToGeoPolygons};
use geoarrow::{LineStringArray, PointArray, PolygonArray};

pub trait ToGeoArrowPolygons {
    type Error;
    fn to_geoarrow_polygons(&self, use_degrees: bool) -> Result<PolygonArray, Self::Error>;
}

impl<T> ToGeoArrowPolygons for T
where
    T: ToGeoPolygons,
{
    type Error = T::Error;

    fn to_geoarrow_polygons(&self, use_degrees: bool) -> Result<PolygonArray, Self::Error> {
        Ok(self.to_geo_polygons(use_degrees)?.into())
    }
}

pub trait ToGeoArrowPoints {
    type Error;
    fn to_geoarrow_points(&self, use_degrees: bool) -> Result<PointArray, Self::Error>;
}

impl<T> ToGeoArrowPoints for T
where
    T: ToGeoPoints,
{
    type Error = T::Error;
    fn to_geoarrow_points(&self, use_degrees: bool) -> Result<PointArray, Self::Error> {
        Ok(self.to_geo_points(use_degrees)?.into())
    }
}

pub trait ToGeoArrowLineStrings {
    type Error;
    fn to_geoarrow_lines(&self, use_degrees: bool) -> Result<LineStringArray, Self::Error>;
}

impl<T> ToGeoArrowLineStrings for T
where
    T: ToGeoLineStrings,
{
    type Error = T::Error;
    fn to_geoarrow_lines(&self, use_degrees: bool) -> Result<LineStringArray, Self::Error> {
        Ok(self.to_geo_linestrings(use_degrees)?.into())
    }
}
