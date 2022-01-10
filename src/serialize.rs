// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::io::BufRead;

use anyhow::bail;

pub fn read_u64(buf: &mut &[u8]) -> anyhow::Result<u64> {
    if buf.len() < 8 {
        bail!("Buffer two short");
    }
    let val_bytes: [u8; 8] = buf[0..8].try_into()?;
    let val = u64::from_le_bytes(val_bytes);
    buf.consume(8);
    Ok(val)
}

pub fn read_u16(buf: &mut &[u8]) -> anyhow::Result<u16> {
    if buf.len() < 2 {
        bail!("Buffer two short");
    }
    let val = u16::from_le_bytes([buf[0], buf[1]]);
    buf.consume(2);
    Ok(val)
}

pub fn read_str<'a>(buf: &mut &'a [u8]) -> anyhow::Result<&'a str> {
    let len = read_u16(buf)? as usize;
    let s = std::str::from_utf8(&buf[..len])?;
    buf.consume(len as usize);
    Ok(s)
}

pub fn write_u16(val: u16, buf: &mut Vec<u8>) {
    buf.extend(&val.to_le_bytes());
}

pub fn write_u64(val: u64, buf: &mut Vec<u8>) {
    buf.extend(&val.to_le_bytes());
}

pub fn write_str(s: &str, buf: &mut Vec<u8>) {
    write_u16(s.len() as u16, buf);
    buf.extend(s.as_bytes())
}

pub trait Serializable: Sized {
    fn serialize(&self, buf: &mut Vec<u8>);
    fn serialize_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.serialize(&mut buf);
        buf
    }
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self>;
}

#[cfg(test)]
pub fn test_serdeser_aux<T: Serializable + PartialEq + std::fmt::Debug>(obj: &T, num_bytes: usize) {
    let mut buf = Vec::new();
    obj.serialize(&mut buf);
    assert_eq!(buf.len(), num_bytes);
    let obj_serdeser = T::deserialize(&mut &buf[..]).unwrap();
    assert_eq!(obj, &obj_serdeser);
}
