use crate::array::to_geo::{
    IterLines, IterPoints, IterPolygons, ToLineStrings, ToPoints, ToPolygons,
};
use crate::array::{H3Array, H3IndexArrayValue};
use arrow::array::OffsetSizeTrait;
use geo_types::LineString;
use geoarrow::array::{
    LineStringArray, PointArray, PolygonArray, WKBArray, WKBBuilder, WKBCapacity,
};

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

impl<T> ToWKBPolygons for H3Array<T>
where
    Self: IterPolygons,
    T: H3IndexArrayValue,
{
    type Error = <Self as IterPolygons>::Error;

    fn to_wkb_polygons<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error> {
        let mut builder = WKBBuilder::with_capacity(WKBCapacity::new(self.len(), self.len()));
        for poly in self.iter_polygons(use_degrees) {
            let poly = poly.transpose()?;
            builder.push_polygon(poly.as_ref())
        }
        Ok(builder.finish())
    }
}

pub trait ToWKBLineStrings {
    type Error;
    fn to_wkb_linestrings<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error>;
}

impl<T> ToWKBLineStrings for H3Array<T>
where
    Self: IterLines,
    T: H3IndexArrayValue,
{
    type Error = <Self as IterLines>::Error;

    fn to_wkb_linestrings<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error> {
        let mut builder = WKBBuilder::with_capacity(WKBCapacity::new(self.len(), self.len()));
        for line in self.iter_lines(use_degrees) {
            let linestring = line.transpose()?.map(LineString::from);
            builder.push_line_string(linestring.as_ref())
        }
        Ok(builder.finish())
    }
}

pub trait ToWKBPoints {
    type Error;
    fn to_wkb_points<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error>;
}

impl<T> ToWKBPoints for H3Array<T>
where
    Self: IterPoints,
    T: H3IndexArrayValue,
{
    type Error = <Self as IterPoints>::Error;

    fn to_wkb_points<O: OffsetSizeTrait>(
        &self,
        use_degrees: bool,
    ) -> Result<WKBArray<O>, Self::Error> {
        let mut builder = WKBBuilder::with_capacity(WKBCapacity::new(self.len(), self.len()));
        for point in self.iter_points(use_degrees) {
            let point = point.transpose()?;
            builder.push_point(point.as_ref())
        }
        Ok(builder.finish())
    }
}
