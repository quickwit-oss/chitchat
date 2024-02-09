use std::io::BufRead;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use anyhow::{bail, Context};
use bytes::Buf;
use zstd;

use crate::{ChitchatId, Heartbeat};

/// Trait to serialize messages.
///
/// Chitchat uses a custom binary serialization format.
/// The point of this format is to make it possible
/// to truncate the delta payload to a given mtu.
pub trait Serializable {
    fn serialize(&self, buf: &mut Vec<u8>);

    fn serialize_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.serialize(&mut buf);
        buf
    }

    fn serialized_len(&self) -> usize;
}

pub trait Deserializable: Sized {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self>;
}

impl Serializable for u8 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.push(*self)
    }

    fn serialized_len(&self) -> usize {
        1
    }
}

impl Deserializable for u8 {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let byte: [u8; 1] = Deserializable::deserialize(buf)?;
        Ok(byte[0])
    }
}

impl Serializable for u16 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.to_le_bytes().serialize(buf);
    }

    fn serialized_len(&self) -> usize {
        2
    }
}

impl Deserializable for u16 {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let u16_bytes: [u8; 2] = Deserializable::deserialize(buf)?;
        Ok(Self::from_le_bytes(u16_bytes))
    }
}

impl Serializable for u64 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.to_le_bytes().serialize(buf);
    }
    fn serialized_len(&self) -> usize {
        8
    }
}

impl Deserializable for u64 {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let u64_bytes: [u8; 8] = Deserializable::deserialize(buf)?;
        Ok(Self::from_le_bytes(u64_bytes))
    }
}

impl Serializable for Option<u64> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.is_some().serialize(buf);
        if let Some(tombstone) = &self {
            tombstone.serialize(buf);
        }
    }
    fn serialized_len(&self) -> usize {
        if self.is_some() {
            9
        } else {
            1
        }
    }
}

impl Deserializable for Option<u64> {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let is_some: bool = Deserializable::deserialize(buf)?;
        if is_some {
            let u64_value = Deserializable::deserialize(buf)?;
            return Ok(Some(u64_value));
        }
        Ok(None)
    }
}

impl Serializable for bool {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.push(*self as u8);
    }
    fn serialized_len(&self) -> usize {
        1
    }
}

impl Deserializable for bool {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let bool_byte: [u8; 1] = Deserializable::deserialize(buf)?;
        Ok(bool_byte[0] != 0)
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

    fn serialized_len(&self) -> usize {
        1 + match self {
            IpAddr::V4(_) => 4,
            IpAddr::V6(_) => 16,
        }
    }
}

impl Deserializable for IpAddr {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let ip_version_byte: [u8; 1] = Deserializable::deserialize(buf)?;
        let ip_version = IpVersion::try_from(ip_version_byte[0])?;
        match ip_version {
            IpVersion::V4 => {
                let bytes: [u8; 4] = Deserializable::deserialize(buf)?;
                Ok(Ipv4Addr::from(bytes).into())
            }
            IpVersion::V6 => {
                let bytes: [u8; 16] = Deserializable::deserialize(buf)?;
                Ok(Ipv6Addr::from(bytes).into())
            }
        }
    }
}

impl Serializable for String {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.as_str().serialize(buf)
    }

    fn serialized_len(&self) -> usize {
        self.as_str().serialized_len()
    }
}

impl Deserializable for String {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let len: usize = u16::deserialize(buf)? as usize;
        let s = std::str::from_utf8(&buf[..len])?.to_string();
        buf.consume(len);
        Ok(s)
    }
}

impl Serializable for str {
    fn serialize(&self, buf: &mut Vec<u8>) {
        (self.len() as u16).serialize(buf);
        buf.extend(self.as_bytes())
    }

    fn serialized_len(&self) -> usize {
        2 + self.len()
    }
}

impl<const N: usize> Serializable for [u8; N] {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self[..]);
    }
    fn serialized_len(&self) -> usize {
        N
    }
}

impl<const N: usize> Deserializable for [u8; N] {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        if buf.len() < N {
            bail!("Buffer too short");
        }
        let val_bytes: [u8; N] = buf[..N].try_into()?;
        buf.consume(N);
        Ok(val_bytes)
    }
}

impl Serializable for SocketAddr {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.ip().serialize(buf);
        self.port().serialize(buf);
    }

    fn serialized_len(&self) -> usize {
        self.ip().serialized_len() + self.port().serialized_len()
    }
}

impl Deserializable for SocketAddr {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let ip_addr = IpAddr::deserialize(buf)?;
        let port = u16::deserialize(buf)?;
        Ok(SocketAddr::new(ip_addr, port))
    }
}

impl Serializable for ChitchatId {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.node_id.serialize(buf);
        self.generation_id.serialize(buf);
        self.gossip_advertise_addr.serialize(buf)
    }

    fn serialized_len(&self) -> usize {
        self.node_id.serialized_len()
            + self.generation_id.serialized_len()
            + self.gossip_advertise_addr.serialized_len()
    }
}

impl Deserializable for ChitchatId {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let node_id = String::deserialize(buf)?;
        let generation_id = u64::deserialize(buf)?;
        let gossip_advertise_addr = SocketAddr::deserialize(buf)?;
        Ok(Self {
            node_id,
            generation_id,
            gossip_advertise_addr,
        })
    }
}

impl Serializable for Heartbeat {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.0.serialize(buf);
    }

    fn serialized_len(&self) -> usize {
        self.0.serialized_len()
    }
}

impl Deserializable for Heartbeat {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let heartbeat = u64::deserialize(buf)?;
        Ok(Self(heartbeat))
    }
}

pub struct CompressedStreamWriter {
    output: Vec<u8>,
    // number of blocks written in output.
    num_blocks: u16,

    // temporary buffer used for block compression.
    uncompressed_block: Vec<u8>,
    // ongoing block being serialized.
    compressed_block: Vec<u8>,
    block_threshold: usize,
}

impl CompressedStreamWriter {
    pub fn with_block_threshold(block_threshold: u16) -> CompressedStreamWriter {
        let block_threshold = block_threshold as usize;
        let output = Vec::with_capacity(block_threshold);
        CompressedStreamWriter {
            output,
            uncompressed_block: Vec::with_capacity(block_threshold * 2),
            compressed_block: Vec::with_capacity(block_threshold),
            block_threshold,
            num_blocks: 0,
        }
    }

    /// Returns an upperbound of the serialized len after appending `s`
    pub fn serialized_len_upperbound_after<S: Serializable>(&self, item: &S) -> usize {
        self.output.len() + // already serialized block
        if self.uncompressed_block.is_empty() { 0 } else { 3 } + // current block len
        self.uncompressed_block.len() +
        3 + // possibly another block that will be created. (this is unlikely but possible and we want an upperbound)
        item.serialized_len() + // the new item. This assume no compression will be possible.
        1 // End of stream flag
    }

    pub fn append<S: Serializable + ?Sized>(&mut self, item: &S) {
        let item_len = item.serialized_len();
        assert!(item_len <= u16::MAX as usize);
        if self.uncompressed_block.len() + item_len >= self.block_threshold {
            // time to flush our current block.
            self.flush_block();
        }
        item.serialize(&mut self.uncompressed_block);
        if self.uncompressed_block.len() >= self.block_threshold {}
    }

    /// Flush the ongoing block as compressed or an uncompressed block (whichever is the smallest).
    /// If the ongoing block is empty, this function is no op.
    fn flush_block(&mut self) {
        if self.uncompressed_block.is_empty() {
            return;
        }
        let uncompressed_len = self.uncompressed_block.len();
        let uncompressed_len_u16 =
            u16::try_from(uncompressed_len).expect("uncompressed block too big");
        self.compressed_block.resize(uncompressed_len, 0u8);
        match zstd::bulk::compress_to_buffer(
            &self.uncompressed_block,
            &mut self.compressed_block[..],
            0,
        ) {
            Ok(compressed_len) => {
                let compressed_len_u16 = u16::try_from(compressed_len).unwrap();
                let block_meta = BlockMeta::CompressedBlock {
                    len: compressed_len_u16,
                };
                block_meta.serialize(&mut self.output);
                self.output.extend(&self.compressed_block[..compressed_len]);
            }
            // The compressed version was actually longer than the decompressed one.
            // Let's keep the block uncomopressed
            Err(_) => {
                let block_meta = BlockMeta::UncompressedBlock {
                    len: uncompressed_len_u16,
                };
                block_meta.serialize(&mut self.output);
                self.output.extend(&self.uncompressed_block);
            }
        }
        self.num_blocks += 1;
        self.uncompressed_block.clear();
        self.compressed_block.clear();
    }

    pub fn finalize(mut self) -> Vec<u8> {
        self.flush_block();
        BlockMeta::NoMoreBlocks.serialize(&mut self.output);
        self.output
    }
}

pub fn deserialize_stream<D: Deserializable>(buf: &mut &[u8]) -> anyhow::Result<Vec<D>> {
    let mut items: Vec<D> = Vec::new();
    let mut decompression_buffer = vec![0; u16::MAX as usize];
    while !buf.is_empty() {
        let block_meta = BlockMeta::deserialize(buf)?;
        match block_meta {
            BlockMeta::CompressedBlock { len } => {
                let len = len as usize;
                let compressed_block_bytes = &buf[..len];
                let uncompressed_len = zstd::bulk::decompress_to_buffer(
                    compressed_block_bytes,
                    &mut decompression_buffer[..u16::MAX as usize],
                )
                .context("failed to decompress block")?;
                buf.advance(len as usize);
                let mut block_bytes = &decompression_buffer[..uncompressed_len];
                while !block_bytes.is_empty() {
                    let item = D::deserialize(&mut block_bytes)?;
                    items.push(item);
                }
            }
            BlockMeta::UncompressedBlock { len } => {
                let len = len as usize;
                let mut block_bytes = &buf[..len];
                buf.advance(len as usize);
                while !block_bytes.is_empty() {
                    let item = D::deserialize(&mut block_bytes)?;
                    items.push(item);
                }
            }
            BlockMeta::NoMoreBlocks => {
                return Ok(items);
            }
        };
    }
    anyhow::bail!("compressed streams error: reached end of buffer without NoMoreBlock tag");
}

#[derive(Eq, PartialEq, Debug)]
enum BlockMeta {
    CompressedBlock { len: u16 },
    UncompressedBlock { len: u16 },
    NoMoreBlocks,
}

const NO_MORE_BLOCKS_TAG: u8 = 0u8;
const COMPRESSED_BLOCK_TAG: u8 = 1u8;
const UNCOMPRESSED_BLOCK_TAG: u8 = 2u8;

impl Serializable for BlockMeta {
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            BlockMeta::CompressedBlock { len } => {
                COMPRESSED_BLOCK_TAG.serialize(buf);
                len.serialize(buf);
            }
            BlockMeta::UncompressedBlock { len } => {
                UNCOMPRESSED_BLOCK_TAG.serialize(buf);
                len.serialize(buf);
            }
            BlockMeta::NoMoreBlocks => {
                NO_MORE_BLOCKS_TAG.serialize(buf);
            }
        }
    }

    fn serialized_len(&self) -> usize {
        match self {
            BlockMeta::CompressedBlock { .. } | BlockMeta::UncompressedBlock { .. } => 3,
            BlockMeta::NoMoreBlocks => 1,
        }
    }
}

impl Deserializable for BlockMeta {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let tag = u8::deserialize(buf)?;
        match tag {
            UNCOMPRESSED_BLOCK_TAG => {
                let len = u16::deserialize(buf)?;
                Ok(BlockMeta::UncompressedBlock { len })
            }
            COMPRESSED_BLOCK_TAG => {
                let len = u16::deserialize(buf)?;
                Ok(BlockMeta::CompressedBlock { len })
            }
            NO_MORE_BLOCKS_TAG => Ok(BlockMeta::NoMoreBlocks),
            _ => {
                anyhow::bail!("Unknown block meta tag: {tag}")
            }
        }
    }
}

#[cfg(test)]
#[track_caller]
pub fn test_serdeser_aux<T: Serializable + Deserializable + PartialEq + std::fmt::Debug>(
    obj: &T,
    num_bytes: usize,
) {
    let mut buf = Vec::new();
    obj.serialize(&mut buf);
    assert_eq!(buf.len(), obj.serialized_len());
    assert_eq!(buf.len(), num_bytes);
    let obj_serdeser = T::deserialize(&mut &buf[..]).unwrap();
    assert_eq!(obj, &obj_serdeser);
}

#[cfg(test)]
mod tests {
    use rand::distributions::Alphanumeric;
    use rand::Rng;

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

    #[test]
    fn test_serialize_block_meta() {
        test_serdeser_aux(&BlockMeta::CompressedBlock { len: 10u16 }, 3);
        test_serdeser_aux(&BlockMeta::UncompressedBlock { len: 18u16 }, 3);
        test_serdeser_aux(&BlockMeta::NoMoreBlocks, 1);
    }

    // An array of 10 small sentences for tests.
    const TEXT_SAMPLES: [&str; 10] = [
        "I'm happy.",
        "She exercises every morning.",
        "His dog barks loudly.",
        "My school starts at 8:00.",
        "We always eat dinner together.",
        "They take the bus to work.",
        "He doesn't like vegetables.",
        "I don't want anything to drink.",
        "hello Happy tax payer",
        "do you like tea?",
    ];

    #[test]
    fn test_compressed_serialized_stream() {
        let mut compressed_stream_writer: CompressedStreamWriter =
            CompressedStreamWriter::with_block_threshold(1_000);
        let mut uncompressed_len = 0;
        for i in 0..100 {
            let sentence = TEXT_SAMPLES[i % TEXT_SAMPLES.len()];
            compressed_stream_writer.append(sentence);
            uncompressed_len += sentence.len();
        }
        let buf = compressed_stream_writer.finalize();
        let mut cursor = &buf[..];
        assert!(buf.len() * 3 < uncompressed_len);
        let vals: Vec<String> = super::deserialize_stream(&mut cursor).unwrap();
        assert_eq!(vals.len(), 100);
        for i in 0..100 {
            let sentence = TEXT_SAMPLES[i % TEXT_SAMPLES.len()];
            assert_eq!(&vals[i], sentence);
        }
        assert!(cursor.is_empty());
    }

    #[test]
    fn test_compressed_serialized_stream_with_random_data() {
        let mut compressed_stream_writer: CompressedStreamWriter =
            CompressedStreamWriter::with_block_threshold(200);
        for i in 0..100 {
            let sentence = TEXT_SAMPLES[i % TEXT_SAMPLES.len()];
            compressed_stream_writer.append(sentence);
        }
        for _ in 0..30 {
            let rng = rand::thread_rng();
            let random_sentence: String = rng
                .sample_iter(&Alphanumeric)
                .take(30)
                .map(char::from)
                .collect();
            compressed_stream_writer.append(random_sentence.as_str());
        }
        let buf = compressed_stream_writer.finalize();
        let mut cursor = &buf[..];
        let vals: Vec<String> = deserialize_stream(&mut cursor).unwrap();
        assert_eq!(vals.len(), 130);
        for i in 0..100 {
            let sentence = TEXT_SAMPLES[i % TEXT_SAMPLES.len()];
            assert_eq!(&vals[i], sentence);
        }
        assert!(cursor.is_empty());
    }
}
