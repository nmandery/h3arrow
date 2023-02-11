use crate::array::{CellIndexArray, VertexIndexArray};

impl VertexIndexArray {
    pub fn owner(&self) -> CellIndexArray {
        self.map_values(|vx| Some(vx.owner())).collect()
    }
}
