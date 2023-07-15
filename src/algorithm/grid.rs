use crate::array::{CellIndexArray, H3Array, H3ListArray, H3ListArrayBuilder};
use crate::error::Error;
use ahash::{HashMap, HashMapExt};
use arrow2::array::{Array, ListArray, PrimitiveArray};
use arrow2::bitmap::{Bitmap, MutableBitmap};
use arrow2::datatypes::DataType;
use arrow2::offset::OffsetsBuffer;
use h3o::CellIndex;
use std::cmp::{max, min};
use std::collections::hash_map::Entry;
use std::default::Default;
use std::marker::PhantomData;

pub struct GridDiskDistances {
    pub cells: H3ListArray<CellIndex>,
    pub distances: ListArray<i64>,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum KAggregationMethod {
    Min,
    Max,
}

pub struct GridDiskAggregateK {
    pub cells: CellIndexArray,
    pub distances: PrimitiveArray<u32>,
}

pub trait GridOp
where
    Self: Sized,
{
    fn grid_disk(&self, k: u32) -> Result<H3ListArray<CellIndex>, Error>;
    fn grid_disk_distances(&self, k: u32) -> Result<GridDiskDistances, Error>;
    fn grid_ring_distances(&self, k_min: u32, k_max: u32) -> Result<GridDiskDistances, Error>;
    fn grid_disk_aggregate_k(
        &self,
        k: u32,
        k_agg_method: KAggregationMethod,
    ) -> Result<GridDiskAggregateK, Error>;
}

impl GridOp for H3Array<CellIndex> {
    fn grid_disk(&self, k: u32) -> Result<H3ListArray<CellIndex>, Error> {
        let mut builder = H3ListArrayBuilder::<CellIndex>::default();
        builder.extend(
            self.iter()
                .map(|cell| cell.map(|cell| cell.grid_disk::<Vec<_>>(k))),
        );
        builder.build()
    }

    fn grid_disk_distances(&self, k: u32) -> Result<GridDiskDistances, Error> {
        build_grid_disk(self, k, |_, _| true)
    }

    fn grid_ring_distances(&self, k_min: u32, k_max: u32) -> Result<GridDiskDistances, Error> {
        build_grid_disk(self, k_max, |_, k| k >= k_min)
    }

    fn grid_disk_aggregate_k(
        &self,
        k: u32,
        k_agg_method: KAggregationMethod,
    ) -> Result<GridDiskAggregateK, Error> {
        let mut cellmap: HashMap<CellIndex, u32> = HashMap::with_capacity(self.len());
        for cell in self.iter().flatten() {
            for (grid_cell, grid_distance) in cell.grid_disk_distances::<Vec<_>>(k).into_iter() {
                match cellmap.entry(grid_cell) {
                    Entry::Occupied(mut e) => {
                        e.insert(match k_agg_method {
                            KAggregationMethod::Min => min(*e.get(), grid_distance),
                            KAggregationMethod::Max => max(*e.get(), grid_distance),
                        });
                    }
                    Entry::Vacant(e) => {
                        e.insert(grid_distance);
                    }
                };
            }
        }

        let mut cells = Vec::with_capacity(cellmap.len());
        let mut distances = Vec::with_capacity(cellmap.len());

        for (cell, distance) in cellmap.into_iter() {
            cells.push(cell);
            distances.push(distance);
        }

        Ok(GridDiskAggregateK {
            cells: CellIndexArray::from(cells),
            distances: PrimitiveArray::from_vec(distances),
        })
    }
}

fn build_grid_disk<F>(
    cellindexarray: &CellIndexArray,
    k: u32,
    filter: F,
) -> Result<GridDiskDistances, Error>
where
    F: Fn(CellIndex, u32) -> bool,
{
    let mut grid_cells = Vec::with_capacity(cellindexarray.len());
    let mut grid_distances = Vec::with_capacity(cellindexarray.len());
    let mut offsets = Vec::with_capacity(cellindexarray.len());
    let mut list_validity = Vec::with_capacity(cellindexarray.len());

    for cell in cellindexarray.iter() {
        offsets.push(grid_cells.len() as i64);
        match cell {
            Some(cell) => {
                list_validity.push(true);

                for (grid_cell, grid_distance) in cell.grid_disk_distances::<Vec<_>>(k).into_iter()
                {
                    if filter(grid_cell, grid_distance) {
                        grid_cells.push(u64::from(grid_cell));
                        grid_distances.push(grid_distance);
                    }
                }
            }
            None => {
                list_validity.push(false);
            }
        }
    }

    debug_assert_eq!(grid_cells.len(), grid_distances.len());

    offsets.push(grid_cells.len() as i64);
    let offsets: OffsetsBuffer<i64> = offsets.try_into()?;
    let list_validity = {
        let validity: Bitmap = MutableBitmap::from_iter(list_validity.into_iter()).into();
        if validity.unset_bits() == 0 {
            None
        } else {
            Some(validity)
        }
    };

    Ok(GridDiskDistances {
        cells: H3ListArray {
            list_array: ListArray::try_new(
                ListArray::<i64>::default_datatype(DataType::UInt64),
                offsets.clone(),
                PrimitiveArray::from_vec(grid_cells).to_boxed(),
                list_validity.clone(),
            )?,
            h3index_phantom: PhantomData::<CellIndex>,
        },
        distances: ListArray::try_new(
            ListArray::<i64>::default_datatype(DataType::UInt32),
            offsets,
            PrimitiveArray::from_vec(grid_distances).to_boxed(),
            list_validity,
        )?,
    })
}
