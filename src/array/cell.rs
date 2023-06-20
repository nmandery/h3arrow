use arrow2::array::PrimitiveArray;

use crate::array::list::H3ListArray;
use crate::array::{CellIndexArray, H3ListArrayBuilder, ResolutionArray};
use crate::error::Error;

impl CellIndexArray {
    pub fn resolution(&self) -> ResolutionArray {
        self.map_values(|cell| Some(cell.resolution())).collect()
    }

    pub fn grid_disk(&self, k: u32) -> Result<H3ListArray<Self>, Error> {
        let mut builder = H3ListArrayBuilder::<Self>::default();
        for value in self.iter() {
            if let Some(cell) = value {
                builder.push_valid(cell.grid_disk::<Vec<_>>(k).into_iter())
            } else {
                builder.push_invalid()
            }
        }
        builder.build()
    }

    pub fn area_rads2(&self) -> PrimitiveArray<f64> {
        self.map_values(|cell| Some(cell.area_rads2())).collect()
    }

    pub fn area_km2(&self) -> PrimitiveArray<f64> {
        self.map_values(|cell| Some(cell.area_km2())).collect()
    }

    pub fn area_m2(&self) -> PrimitiveArray<f64> {
        self.map_values(|cell| Some(cell.area_m2())).collect()
    }
}

#[cfg(test)]
mod test {
    use h3o::{LatLng, Resolution};

    use crate::array::CellIndexArray;

    #[test]
    fn construct_invalid_fails() {
        let res: Result<CellIndexArray, _> = vec![
            u64::from(LatLng::new(23.4, 12.4).unwrap().to_cell(Resolution::Five)),
            0,
        ]
        .try_into();
        assert!(res.is_err());
    }

    #[test]
    fn resolution() {
        let arr: CellIndexArray = vec![
            LatLng::new(23.4, 12.4).unwrap().to_cell(Resolution::Five),
            LatLng::new(12.3, 0.5).unwrap().to_cell(Resolution::Nine),
        ]
        .into();

        let r_arr = arr.resolution();
        assert_eq!(r_arr.len(), arr.len());
        let r_values: Vec<_> = r_arr.iter().collect();
        assert_eq!(
            r_values,
            vec![Some(Resolution::Five), Some(Resolution::Nine)]
        );
    }
}
