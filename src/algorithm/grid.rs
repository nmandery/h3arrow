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
        builder.extend(
            self.iter()
                .map(|cell| cell.map(|cell| cell.grid_disk::<Vec<_>>(k))),
        );
        builder.build()
    }
}
