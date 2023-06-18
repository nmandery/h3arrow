#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    InvalidCellIndex(#[from] h3o::error::InvalidCellIndex),

    #[error(transparent)]
    InvalidVertexIndex(#[from] h3o::error::InvalidVertexIndex),

    #[error(transparent)]
    InvalidDirectedEdgeIndex(#[from] h3o::error::InvalidDirectedEdgeIndex),

    #[error(transparent)]
    InvalidResolution(#[from] h3o::error::InvalidResolution),

    #[error(transparent)]
    InvalidLatLng(#[from] h3o::error::InvalidLatLng),

    #[error(transparent)]
    InvalidGeometry(#[from] h3o::error::InvalidGeometry),

    #[error(transparent)]
    CompactionError(#[from] h3o::error::CompactionError),

    #[error(transparent)]
    OutlinerError(#[from] h3o::error::OutlinerError),

    #[error(transparent)]
    Arrow2(#[from] arrow2::error::Error),

    #[error("not a PrimitiveArray<u64>")]
    NotAPrimitiveArrayU64,

    #[error("non-parsable CellIndex")]
    NonParsableCellIndex,

    #[error("Invalid WKB encountered")]
    InvalidWKB,
}
