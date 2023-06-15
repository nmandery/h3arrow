use crate::array::{CellIndexArray, H3Array};
use crate::error::Error;
use arrow2::array::Float64Array;
use h3o::LatLng;

pub struct CoordinateArrays {
    pub lat: Float64Array,
    pub lng: Float64Array,
}

pub trait ToCoordinatesOp {
    /// convert to point coordinates in degrees
    fn to_coordinates(&self) -> Result<CoordinateArrays, Error>;

    /// convert to point coordinates in radians
    fn to_coordinates_radians(&self) -> Result<CoordinateArrays, Error>;
}

impl ToCoordinatesOp for CellIndexArray {
    fn to_coordinates(&self) -> Result<CoordinateArrays, Error> {
        Ok(to_coordinatearrays(self, |ll, lat, lng| {
            *lat = ll.lat();
            *lng = ll.lng();
        }))
    }

    fn to_coordinates_radians(&self) -> Result<CoordinateArrays, Error> {
        Ok(to_coordinatearrays(self, |ll, lat, lng| {
            *lat = ll.lat_radians();
            *lng = ll.lng_radians();
        }))
    }
}

fn to_coordinatearrays<F>(cellindexarray: &CellIndexArray, mut setter: F) -> CoordinateArrays
where
    F: FnMut(&LatLng, &mut f64, &mut f64),
{
    let mut lat_array = vec![0f64; cellindexarray.len()];
    let mut lng_array = vec![0f64; cellindexarray.len()];

    lat_array
        .iter_mut()
        .zip(lng_array.iter_mut())
        .zip(cellindexarray.iter())
        .for_each(|((lat, lng), cell)| {
            if let Some(cell) = cell {
                let ll = LatLng::from(cell);
                setter(&ll, lat, lng);
                *lat = ll.lat();
                *lng = ll.lng();
            }
        });

    CoordinateArrays {
        lat: Float64Array::from_vec(lat_array)
            .with_validity(cellindexarray.primitive_array().validity().cloned()),
        lng: Float64Array::from_vec(lng_array)
            .with_validity(cellindexarray.primitive_array().validity().cloned()),
    }
}
