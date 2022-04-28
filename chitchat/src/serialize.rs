use std::io::BufRead;

use anyhow::bail;

use crate::NodeId;

fn read_str<'a>(buf: &mut &'a [u8]) -> anyhow::Result<&'a str> {
    let len: usize = u16::deserialize(buf)? as usize;
    let s = std::str::from_utf8(&buf[..len])?;
    buf.consume(len as usize);
    Ok(s)
}

impl Serializable for u16 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend(&self.to_le_bytes());
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        if buf.len() < 2 {
            bail!("Buffer two short");
        }
        let val = u16::from_le_bytes([buf[0], buf[1]]);
        buf.consume(2);
        Ok(val)
    }

    fn serialized_len(&self) -> usize {
        2
    }
}

impl Serializable for u64 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend(&self.to_le_bytes());
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        if buf.len() < 8 {
            bail!("Buffer two short");
        }
        let val_bytes: [u8; 8] = buf[0..8].try_into()?;
        let val = u64::from_le_bytes(val_bytes);
        buf.consume(8);
        Ok(val)
    }

    fn serialized_len(&self) -> usize {
        8
    }
}

impl Serializable for String {
    fn serialize(&self, buf: &mut Vec<u8>) {
        write_str(self.as_str(), buf)
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        read_str(buf).map(ToString::to_string)
    }

    fn serialized_len(&self) -> usize {
        2 + self.len()
    }
}

impl Serializable for NodeId {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.id.serialize(buf);
        self.gossip_public_address.serialize(buf)
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let id = String::deserialize(buf)?;
        let gossip_public_address = String::deserialize(buf)?;
        Ok(NodeId {
            id,
            gossip_public_address,
        })
    }

    fn serialized_len(&self) -> usize {
        self.id.serialized_len() + self.gossip_public_address.serialized_len()
    }
}

impl<A, B> Serializable for (A, B)
where
    A: Serializable,
    B: Serializable,
{
    fn serialize(&self, buf: &mut Vec<u8>) {
        let (a, b) = self;
        a.serialize(buf);
        b.serialize(buf);
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let a = A::deserialize(buf)?;
        let b = B::deserialize(buf)?;
        Ok((a, b))
    }

    fn serialized_len(&self) -> usize {
        let (a, b) = self;
        a.serialized_len() + b.serialized_len()
    }
}

impl<A, B, C> Serializable for (A, B, C)
where
    A: Serializable,
    B: Serializable,
    C: Serializable,
{
    fn serialize(&self, buf: &mut Vec<u8>) {
        let (a, b, c) = self;
        a.serialize(buf);
        b.serialize(buf);
        c.serialize(buf);
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let a = A::deserialize(buf)?;
        let b = B::deserialize(buf)?;
        let c = C::deserialize(buf)?;
        Ok((a, b, c))
    }

    fn serialized_len(&self) -> usize {
        let (a, b, c) = self;
        a.serialized_len() + b.serialized_len() + c.serialized_len()
    }
}

fn write_str(s: &str, buf: &mut Vec<u8>) {
    (s.len() as u16).serialize(buf);
    buf.extend(s.as_bytes())
}

/// Trait to serialize messages.
///
/// Chitchat uses a custom binary serialization format.
/// The point of this format is to make it possible
/// to truncate the delta payload to a given mtu.
pub trait Serializable: Sized {
    fn serialize(&self, buf: &mut Vec<u8>);
    fn serialize_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.serialize(&mut buf);
        buf
    }
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self>;
    fn serialized_len(&self) -> usize;
}

#[cfg(test)]
pub fn test_serdeser_aux<T: Serializable + PartialEq + std::fmt::Debug>(obj: &T, num_bytes: usize) {
    let mut buf = Vec::new();
    obj.serialize(&mut buf);
    assert_eq!(buf.len(), obj.serialized_len());
    assert_eq!(buf.len(), num_bytes);
    let obj_serdeser = T::deserialize(&mut &buf[..]).unwrap();
    assert_eq!(obj, &obj_serdeser);
}
