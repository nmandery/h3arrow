use crate::array::to_geo::{ToLineStrings, ToLines, ToPoints, ToPolygons};
use arrow::array::OffsetSizeTrait;
use geo_types::Geometry;
use geoarrow::array::{LineStringArray, PointArray, PolygonArray, WKBArray};

pub trait ToGeoArrowPolygons {
    type Error;
    fn to_geoarrow_polygons<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<PolygonArray<O>, Self::Error>;
}

impl<T> ToGeoArrowPolygons for T
where
    T: ToPolygons,
{
    type Error = T::Error;

    fn to_geoarrow_polygons<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<PolygonArray<O>, Self::Error> {
        Ok(self.to_polygons(use_degrees)?.into())
    }
}

pub trait ToGeoArrowPoints {
    type Error;
    fn to_geoarrow_points(&self, use_degrees: bool) -> Result<PointArray, Self::Error>;
}

impl<T> ToGeoArrowPoints for T
where
    T: ToPoints,
{
    type Error = T::Error;
    fn to_geoarrow_points(&self, use_degrees: bool) -> Result<PointArray, Self::Error> {
        Ok(self.to_points(use_degrees)?.into())
    }
}

pub trait ToGeoArrowLineStrings {
    type Error;
    fn to_geoarrow_lines<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<LineStringArray<O>, Self::Error>;
}

impl<T> ToGeoArrowLineStrings for T
where
    T: ToLineStrings,
{
    type Error = T::Error;
    fn to_geoarrow_lines<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<LineStringArray<O>, Self::Error> {
        Ok(self.to_linestrings(use_degrees)?.into())
    }
}

pub trait ToWKBPolygons {
    type Error;
    fn to_wkb_polygons<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error>;
}

impl<T> ToWKBPolygons for T
where
    T: ToPolygons,
{
    type Error = T::Error;

    fn to_wkb_polygons<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error> {
        Ok(WKBArray::from(
            self.to_polygons(use_degrees)?
                .into_iter()
                .map(|v| v.map(Geometry::from))
                .collect::<Vec<_>>(),
        ))
    }
}

pub trait ToWKBLines {
    type Error;
    fn to_wkb_lines<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error>;
}

impl<T> ToWKBLines for T
where
    T: ToLines,
{
    type Error = T::Error;

    fn to_wkb_lines<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error> {
        Ok(WKBArray::from(
            self.to_lines(use_degrees)?
                .into_iter()
                .map(|v| v.map(Geometry::from))
                .collect::<Vec<_>>(),
        ))
    }
}

pub trait ToWKBLineStrings {
    type Error;
    fn to_wkb_linestrings<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error>;
}

impl<T> ToWKBLineStrings for T
where
    T: ToLineStrings,
{
    type Error = T::Error;

    fn to_wkb_linestrings<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error> {
        Ok(WKBArray::from(
            self.to_linestrings(use_degrees)?
                .into_iter()
                .map(|v| v.map(Geometry::from))
                .collect::<Vec<_>>(),
        ))
    }
}

pub trait ToWKBPoints {
    type Error;
    fn to_wkb_points<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error>;
}

impl<T> ToWKBPoints for T
where
    T: ToPoints,
{
    type Error = T::Error;

    fn to_wkb_points<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error> {
        Ok(WKBArray::from(
            self.to_points(use_degrees)?
                .into_iter()
                .map(|v| v.map(Geometry::from))
                .collect::<Vec<_>>(),
        ))
    }
}
