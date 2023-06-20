use crate::array::{CellIndexArray, H3ListArray, H3ListArrayBuilder};
use crate::error::Error;
use arrow2::array::PrimitiveArray;
use h3o::Resolution;

pub trait HierarchyOp
where
    Self: Sized,
{
    fn parent(&self, resolution: Resolution) -> Self;

    fn children(&self, resolution: Resolution) -> Result<H3ListArray<Self>, Error>;

    fn children_count(&self, resolution: Resolution) -> PrimitiveArray<u64>;
}

impl HierarchyOp for CellIndexArray {
    fn parent(&self, resolution: Resolution) -> Self {
        self.map_values(|cell| cell.parent(resolution)).collect()
    }

    fn children(&self, resolution: Resolution) -> Result<H3ListArray<Self>, Error> {
        let mut builder = H3ListArrayBuilder::<Self>::default();
        for value in self.iter() {
            if let Some(cell) = value {
                builder.push_valid(cell.children(resolution))
            } else {
                builder.push_invalid()
            }
        }
        builder.build()
    }

    fn children_count(&self, resolution: Resolution) -> PrimitiveArray<u64> {
        self.map_values(|cell| Some(cell.children_count(resolution)))
            .collect()
    }
}

#[cfg(test)]
mod test {
    use crate::algorithm::HierarchyOp;
    use h3o::{LatLng, Resolution};

    use crate::array::CellIndexArray;

    #[test]
    fn children() {
        let arr: CellIndexArray = vec![
            LatLng::new(23.4, 12.4).unwrap().to_cell(Resolution::Five),
            LatLng::new(12.3, 0.5).unwrap().to_cell(Resolution::Nine),
        ]
        .into();

        let children = arr.children(Resolution::Six).unwrap();
        assert_eq!(children.len(), 2);
        let cellarray = children.iter_arrays().next().flatten().unwrap().unwrap();
        assert_eq!(cellarray.len(), 7);

        assert_eq!(cellarray.len(), 7);
        for child in cellarray.iter().flatten() {
            assert_eq!(arr.iter().next().flatten(), child.parent(Resolution::Five));
        }
    }
}
