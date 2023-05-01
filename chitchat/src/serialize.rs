use std::io::BufRead;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use anyhow::bail;

use crate::{ChitchatId, Heartbeat};

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

impl Serializable for u16 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.to_le_bytes().serialize(buf);
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let u16_bytes: [u8; 2] = Serializable::deserialize(buf)?;
        Ok(Self::from_le_bytes(u16_bytes))
    }

    fn serialized_len(&self) -> usize {
        2
    }
}

impl Serializable for u64 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.to_le_bytes().serialize(buf);
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let u64_bytes: [u8; 8] = Serializable::deserialize(buf)?;
        Ok(Self::from_le_bytes(u64_bytes))
    }

    fn serialized_len(&self) -> usize {
        8
    }
}

impl Serializable for Option<u64> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.is_some().serialize(buf);
        if let Some(tombstone) = &self {
            tombstone.serialize(buf);
        }
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let is_some: bool = Serializable::deserialize(buf)?;
        if is_some {
            let u64_value = Serializable::deserialize(buf)?;
            return Ok(Some(u64_value));
        }
        Ok(None)
    }

    fn serialized_len(&self) -> usize {
        if self.is_some() {
            9
        } else {
            1
        }
    }
}

impl Serializable for bool {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.push(*self as u8);
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let bool_byte: [u8; 1] = Serializable::deserialize(buf)?;
        Ok(bool_byte[0] != 0)
    }

    fn serialized_len(&self) -> usize {
        1
    }
}

#[repr(u8)]
enum IpVersion {
    V4 = 4u8,
    V6 = 6u8,
}

impl TryFrom<u8> for IpVersion {
    type Error = anyhow::Error;

    fn try_from(ip_type_byte: u8) -> anyhow::Result<Self> {
        if ip_type_byte == IpVersion::V4 as u8 {
            Ok(IpVersion::V4)
        } else if ip_type_byte == IpVersion::V6 as u8 {
            Ok(IpVersion::V6)
        } else {
            bail!("Invalid IP version byte. Expected `4` or `6`, got `{ip_type_byte}`.");
        }
    }
}

impl Serializable for IpAddr {
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            IpAddr::V4(ip_v4) => {
                buf.push(IpVersion::V4 as u8);
                buf.extend_from_slice(&ip_v4.octets());
            }
            IpAddr::V6(ip_v6) => {
                buf.push(IpVersion::V6 as u8);
                buf.extend_from_slice(&ip_v6.octets());
            }
        }
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let ip_version_byte: [u8; 1] = Serializable::deserialize(buf)?;
        let ip_version = IpVersion::try_from(ip_version_byte[0])?;

        match ip_version {
            IpVersion::V4 => {
                let bytes: [u8; 4] = Serializable::deserialize(buf)?;
                Ok(Ipv4Addr::from(bytes).into())
            }
            IpVersion::V6 => {
                let bytes: [u8; 16] = Serializable::deserialize(buf)?;
                Ok(Ipv6Addr::from(bytes).into())
            }
        }
    }

    fn serialized_len(&self) -> usize {
        1 + match self {
            IpAddr::V4(_) => 4,
            IpAddr::V6(_) => 16,
        }
    }
}

impl Serializable for String {
    fn serialize(&self, buf: &mut Vec<u8>) {
        (self.len() as u16).serialize(buf);
        buf.extend(self.as_bytes())
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let len: usize = u16::deserialize(buf)? as usize;
        let s = std::str::from_utf8(&buf[..len])?.to_string();
        buf.consume(len);
        Ok(s)
    }

    fn serialized_len(&self) -> usize {
        2 + self.len()
    }
}

impl<const N: usize> Serializable for [u8; N] {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self[..]);
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        if buf.len() < N {
            bail!("Buffer too short");
        }
        let val_bytes: [u8; N] = buf[..N].try_into()?;
        buf.consume(N);
        Ok(val_bytes)
    }

    fn serialized_len(&self) -> usize {
        N
    }
}

impl Serializable for SocketAddr {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.ip().serialize(buf);
        self.port().serialize(buf);
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let ip_addr = IpAddr::deserialize(buf)?;
        let port = u16::deserialize(buf)?;
        Ok(SocketAddr::new(ip_addr, port))
    }

    fn serialized_len(&self) -> usize {
        self.ip().serialized_len() + self.port().serialized_len()
    }
}

impl Serializable for ChitchatId {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.node_id.serialize(buf);
        self.generation_id.serialize(buf);
        self.gossip_advertise_address.serialize(buf)
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let node_id = String::deserialize(buf)?;
        let generation_id = u64::deserialize(buf)?;
        let gossip_public_address = SocketAddr::deserialize(buf)?;
        Ok(Self {
            node_id,
            generation_id,
            gossip_advertise_address: gossip_public_address,
        })
    }

    fn serialized_len(&self) -> usize {
        self.node_id.serialized_len()
            + self.generation_id.serialized_len()
            + self.gossip_advertise_address.serialized_len()
    }
}

impl Serializable for Heartbeat {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.0.serialize(buf);
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let heartbeat = u64::deserialize(buf)?;
        Ok(Self(heartbeat))
    }

    fn serialized_len(&self) -> usize {
        self.0.serialized_len()
    }
}

#[cfg(test)]
#[track_caller]
pub fn test_serdeser_aux<T: Serializable + PartialEq + std::fmt::Debug>(obj: &T, num_bytes: usize) {
    let mut buf = Vec::new();
    obj.serialize(&mut buf);
    assert_eq!(buf.len(), obj.serialized_len());
    assert_eq!(buf.len(), num_bytes);
    let obj_serdeser = T::deserialize(&mut &buf[..]).unwrap();
    assert_eq!(obj, &obj_serdeser);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_bool() {
        test_serdeser_aux(&true, 1);
    }

    #[test]
    fn test_serialize_chitchat_id() {
        test_serdeser_aux(
            &ChitchatId::new("node-id".to_string(), 1, "127.0.0.1:7280".parse().unwrap()),
            24,
        );
    }

    #[test]
    fn test_serialize_heartbeat() {
        test_serdeser_aux(&Heartbeat(1), 8);
    }

    #[test]
    fn test_serialize_ip() {
        let ipv4 = IpAddr::from(Ipv4Addr::new(127, 1, 3, 9));
        test_serdeser_aux(&ipv4, 5);

        let ipv6 = IpAddr::from(Ipv6Addr::from([
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        ]));
        test_serdeser_aux(&ipv6, 17);
    }

    #[test]
    fn test_serialize_option_u64() {
        test_serdeser_aux(&Some(1), 9);
        test_serdeser_aux(&None, 1);
    }
}
