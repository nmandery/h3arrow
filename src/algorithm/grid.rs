use crate::array::{CellIndexArray, H3ListArray, H3ListArrayBuilder};
use crate::error::Error;

pub trait GridOp
where
    Self: Sized,
{
    fn grid_disk(&self, k: u32) -> Result<H3ListArray<Self>, Error>;
}

impl GridOp for CellIndexArray {
    fn grid_disk(&self, k: u32) -> Result<H3ListArray<Self>, Error> {
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
}
