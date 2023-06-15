use arrow2::array::Utf8Array;
use arrow2::types::Offset;
use std::str::FromStr;

use crate::array::CellIndexArray;
use geo_types::Coord;
use h3o::{CellIndex, LatLng, Resolution};
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while, take_while_m_n};
use nom::combinator::map_res;
use nom::number::complete::double;
use nom::IResult;

use crate::error::Error;

/// parse H3 cells from strings
pub trait ParseCellsOp {
    /// parse H3 cells from string arrays
    ///
    /// Setting `set_failing_to_invalid` to true will trigger setting the validity bitmap according
    /// the successful parsing of an individual element. Having this set to false will cause the
    /// method to fail upon encountering the first unparsable value.
    fn parse_cells(&self, set_failing_to_invalid: bool) -> Result<CellIndexArray, Error>;
}

/// parse H3 cells from strings
impl<O: Offset> ParseCellsOp for Utf8Array<O> {
    fn parse_cells(&self, set_failing_to_invalid: bool) -> Result<CellIndexArray, Error> {
        let cells = self
            .iter()
            .map(|value| match value {
                Some(value_str) => match parse_cell(value_str) {
                    Ok(cell) => Ok(Some(cell)),
                    Err(e) => {
                        if set_failing_to_invalid {
                            Ok(None)
                        } else {
                            Err(e)
                        }
                    }
                },
                None => Ok(None),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(cells.into())
    }
}

pub fn parse_cell(s: &str) -> Result<CellIndex, Error> {
    if let Ok(cell) = CellIndex::from_str(s) {
        return Ok(cell);
    }

    if let Ok(cell_int) = u64::from_str(s) {
        if let Ok(cell) = CellIndex::try_from(cell_int) {
            return Ok(cell);
        }
    }

    // attempt to parse as coordinate pair and resolution
    if let Ok((_, (coord, res))) = parse_coordinate_and_resolution(s) {
        return Ok(LatLng::new(coord.y, coord.x)?.to_cell(Resolution::try_from(res)?));
    }

    Err(Error::NonParsableCellIndex)
}

fn is_whitespace(c: char) -> bool {
    c.is_ascii_whitespace()
}

fn seperator(s: &str) -> IResult<&str, &str> {
    alt((tag(","), (tag(";"))))(s)
}

fn u8_str(s: &str) -> IResult<&str, u8> {
    map_res(take_while_m_n(1, 2, |c: char| c.is_ascii_digit()), |u8s| {
        u8::from_str(u8s)
    })(s)
}

fn parse_coordinate_and_resolution(s: &str) -> IResult<&str, (Coord, u8)> {
    let (s, _) = take_while(is_whitespace)(s)?;
    let (s, x) = double(s)?;
    let (s, _) = take_while(is_whitespace)(s)?;
    let (s, _) = seperator(s)?;
    let (s, _) = take_while(is_whitespace)(s)?;
    let (s, y) = double(s)?;
    let (s, _) = take_while(is_whitespace)(s)?;
    let (s, _) = seperator(s)?;
    let (s, _) = take_while(is_whitespace)(s)?;
    let (s, r) = u8_str(s)?;
    Ok((s, (Coord::from((x, y)), r)))
}

#[cfg(test)]
mod test {
    use crate::algorithm::{parse_cell, ParseCellsOp};
    use arrow2::array::Utf8Array;
    use h3o::{CellIndex, LatLng};

    #[test]
    fn parse_cell_from_numeric() {
        let cell: CellIndex = 0x89283080ddbffff_u64.try_into().unwrap();
        let s = format!("{}", u64::from(cell));

        let cell2 = parse_cell(&s).unwrap();
        assert_eq!(cell, cell2);
    }

    #[test]
    fn parse_cell_from_coordinate_and_resolution() {
        let cell: CellIndex = 0x89283080ddbffff_u64.try_into().unwrap();
        let ll = LatLng::from(cell);
        let s = format!("{},{},{}", ll.lng(), ll.lat(), cell.resolution());

        let cell2 = parse_cell(&s).unwrap();
        assert_eq!(cell, cell2);
    }

    #[test]
    fn parse_utf8_array_cells() {
        let cell: CellIndex = 0x89283080ddbffff_u64.try_into().unwrap();

        let utf8_array = Utf8Array::<i32>::from_iter(
            vec![cell.to_string(), u64::from(cell).to_string()]
                .into_iter()
                .map(Some),
        );
        let cell_array = utf8_array.parse_cells(false).unwrap();
        assert_eq!(cell_array.len(), utf8_array.len());
        assert!(cell_array.iter().all(|v| v == Some(cell)))
    }

    #[test]
    fn parse_utf8_array_cells_invalid_fail() {
        let utf8_array = Utf8Array::<i32>::from_iter(vec![Some("invalid".to_string())].into_iter());
        assert!(utf8_array.parse_cells(false).is_err());
    }

    #[test]
    fn parse_utf8_array_cells_invalid_to_invalid() {
        let utf8_array = Utf8Array::<i32>::from_iter(vec![Some("invalid".to_string())].into_iter());
        let cell_array = utf8_array.parse_cells(true).unwrap();
        assert_eq!(1, cell_array.len());
        assert!(cell_array.iter().all(|v| v.is_none()))
    }
}
