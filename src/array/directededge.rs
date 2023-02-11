use crate::array::{CellIndexArray, DirectedEdgeIndexArray};
use arrow2::array::PrimitiveArray;

impl DirectedEdgeIndexArray {
    pub fn origin(&self) -> CellIndexArray {
        self.map_values(|edge| Some(edge.origin())).collect()
    }

    pub fn destination(&self) -> CellIndexArray {
        self.map_values(|edge| Some(edge.destination())).collect()
    }

    pub fn length_rads(&self) -> PrimitiveArray<f64> {
        self.map_values(|edge| Some(edge.length_rads())).collect()
    }

    pub fn length_km(&self) -> PrimitiveArray<f64> {
        self.map_values(|edge| Some(edge.length_km())).collect()
    }

    pub fn length_m(&self) -> PrimitiveArray<f64> {
        self.map_values(|edge| Some(edge.length_m())).collect()
    }
}
