use std::io::BufRead;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use anyhow::{bail, Context};
use bytes::Buf;

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

impl Serializable for u32 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.to_le_bytes().serialize(buf);
    }

    fn serialized_len(&self) -> usize {
        4
    }
}

impl Deserializable for u32 {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let u32_bytes: [u8; 4] = Deserializable::deserialize(buf)?;
        Ok(Self::from_le_bytes(u32_bytes))
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
        let str_bytes = buf.get(..len).with_context(|| {
            format!(
                "failed to deserialize string, buffer too short (str_len={len}, buf_len={})",
                buf.len()
            )
        })?;
        let str = std::str::from_utf8(str_bytes)?.to_string();
        buf.consume(len);
        Ok(str)
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

/// A compressed stream writer receives a sequence of `Serializable` and
/// serialize/compresses into blocks of a configurable size.
///
/// Block are tagged, so that blocks with a high entropy are stored kept "uncompressed".
///
/// The stream gives the client an upperbound of what the overall payload length would
/// be if another item was appended.
/// This makes it possible to enforce a `mtu`.
pub struct CompressedStreamWriter {
    output: Vec<u8>,
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
        }
    }

    /// Returns an upperbound of the serialized len after appending `s`
    pub fn serialized_len_upperbound_after<S: Serializable + ?Sized>(&self, item: &S) -> usize {
        let new_item_serialized_len = item.serialized_len();
        assert!(new_item_serialized_len > 0);
        const BLOCK_META_LEN: usize = 3;
        let new_block_needed =
            self.uncompressed_block.len() + new_item_serialized_len > self.block_threshold;
        if new_block_needed {
            BLOCK_META_LEN + self.output.len() + self.uncompressed_block.len() + // current block
                BLOCK_META_LEN + new_item_serialized_len // new block
                + 1 // No more blocks tag.
        } else {
            BLOCK_META_LEN + self.output.len() + self.uncompressed_block.len() + new_item_serialized_len // current block
                + 1 // No more blocks tag.
        }
    }

    /// Appends a new item to the stream. Items must be at most `u16::MAX` bytes long.
    pub fn append<S: Serializable + ?Sized>(&mut self, item: &S) {
        let item_len = item.serialized_len();
        assert!(item_len <= u16::MAX as usize);
        item.serialize(&mut self.uncompressed_block);
        while self.uncompressed_block.len() > self.block_threshold {
            // time to flush our current block.
            self.flush_block();
        }
    }

    /// Flush the first `block_threshold` bytes from the `uncompressed_block` buffer,
    /// and remove them from the buffer.
    ///
    /// If the buffer less bytes than `block_threshold`, compress the bytes available.
    /// (This happens for the last block.)
    fn flush_block(&mut self) {
        if self.uncompressed_block.is_empty() {
            return;
        }
        let num_bytes_to_compress = self.uncompressed_block.len().min(self.block_threshold);

        self.compressed_block.resize(num_bytes_to_compress, 0u8);
        match zstd::bulk::compress_to_buffer(
            &self.uncompressed_block[..num_bytes_to_compress],
            &mut self.compressed_block[..],
            0, // default compression level
        ) {
            Ok(compressed_len) => {
                BlockType::Compressed.serialize(&mut self.output);
                let compressed_len_u16 = u16::try_from(compressed_len).unwrap();
                compressed_len_u16.serialize(&mut self.output);
                self.output.extend(&self.compressed_block[..compressed_len]);
            }
            // The compressed version was actually longer than the decompressed one.
            // Let's keep the block uncomopressed
            Err(_) => {
                BlockType::Uncompressed.serialize(&mut self.output);
                let num_bytes_to_compress_u16 =
                    u16::try_from(num_bytes_to_compress).expect("uncompressed block too big");
                num_bytes_to_compress_u16.serialize(&mut self.output);
                self.output
                    .extend(&self.uncompressed_block[..num_bytes_to_compress]);
            }
        }
        self.uncompressed_block.drain(..num_bytes_to_compress);
    }

    pub fn finish(mut self) -> Vec<u8> {
        self.flush_block();
        BlockType::NoMoreBlocks.serialize(&mut self.output);
        self.output
    }
}

pub fn deserialize_stream<D: Deserializable>(buf: &mut &[u8]) -> anyhow::Result<Vec<D>> {
    let mut decompressed_data = Vec::new();
    let mut decompressed_buffer = vec![0; u16::MAX as usize];
    loop {
        let block_type = BlockType::deserialize(buf)?;
        match block_type {
            BlockType::Compressed => {
                let len = u16::deserialize(buf)? as usize;
                let compressed_block_bytes = buf.get(..len).context(
                    "failed to download compressed stream (compressed block): buffer too short",
                )?;
                let uncompressed_len = zstd::bulk::decompress_to_buffer(
                    compressed_block_bytes,
                    &mut decompressed_buffer[..u16::MAX as usize],
                )
                .context("failed to decompress block")?;
                buf.advance(len);
                decompressed_data.extend_from_slice(&decompressed_buffer[..uncompressed_len]);
            }
            BlockType::Uncompressed => {
                let len = u16::deserialize(buf)? as usize;
                let block_bytes = buf.get(..len).context(
                    "failed to download compressed stream (uncompressed block): buffer too short",
                )?;
                decompressed_data.extend_from_slice(block_bytes);
                buf.advance(len);
            }
            BlockType::NoMoreBlocks => {
                break;
            }
        }
    }
    let mut decompressed_cursor = &decompressed_data[..];
    let mut items = Vec::new();
    while !decompressed_cursor.is_empty() {
        let item = D::deserialize(&mut decompressed_cursor)?;
        items.push(item);
    }
    Ok(items)
}

#[repr(u8)]
#[derive(Copy, Clone)]
enum BlockType {
    NoMoreBlocks,
    Compressed,
    Uncompressed,
}

impl Serializable for BlockType {
    fn serialize(&self, buf: &mut Vec<u8>) {
        (*self as u8).serialize(buf)
    }
    fn serialized_len(&self) -> usize {
        1
    }
}

impl Deserializable for BlockType {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let byte = u8::deserialize(buf)?;
        match byte {
            0 => Ok(BlockType::NoMoreBlocks),
            1 => Ok(BlockType::Compressed),
            2 => Ok(BlockType::Uncompressed),
            _ => anyhow::bail!("invalid block type"),
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
    use proptest::proptest;
    use rand::distr::Alphanumeric;
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
    fn test_serialize_block_type() {
        let mut valid_vals_count = 0;
        for b in 0..=u8::MAX {
            if let Ok(block_type) = BlockType::deserialize(&mut &[b][..]) {
                valid_vals_count += 1;
                let serialized = block_type.serialize_to_vec();
                assert_eq!(&serialized, &[b]);
            }
        }
        assert_eq!(valid_vals_count, 3);
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
        let buf = compressed_stream_writer.finish();
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
            let rng = rand::rng();
            let random_sentence: String = rng
                .sample_iter(&Alphanumeric)
                .take(30)
                .map(char::from)
                .collect();
            compressed_stream_writer.append(random_sentence.as_str());
        }
        let buf = compressed_stream_writer.finish();
        let mut cursor = &buf[..];
        let vals: Vec<String> = deserialize_stream(&mut cursor).unwrap();
        assert_eq!(vals.len(), 130);
        for i in 0..100 {
            let sentence = TEXT_SAMPLES[i % TEXT_SAMPLES.len()];
            assert_eq!(&vals[i], sentence);
        }
        assert!(cursor.is_empty());
    }

    #[test]
    fn test_compressed_serialized_stream_len_when_there_are_no_compressed_blocks() {
        {
            let mut compressed_stream_writer: CompressedStreamWriter =
                CompressedStreamWriter::with_block_threshold(10);
            let random_string = "xu1Y3l";
            let serialized_len_after_hello =
                compressed_stream_writer.serialized_len_upperbound_after(random_string);
            compressed_stream_writer.append(random_string);
            let buffer = compressed_stream_writer.finish();
            // There are no compression opportunity here. The foreseen serialized len should be the
            // same as the actual length.
            assert_eq!(buffer.len(), serialized_len_after_hello);
        }
        {
            let mut compressed_stream_writer: CompressedStreamWriter =
                CompressedStreamWriter::with_block_threshold(10);
            let random_string = "pTs2yYd";
            compressed_stream_writer.append(random_string);
            let random_string2 = "vLQRFPN6";
            let serialized_len_after_hello =
                compressed_stream_writer.serialized_len_upperbound_after(random_string2);
            compressed_stream_writer.append(random_string2);
            let buffer = compressed_stream_writer.finish();
            // There are no compression opportunity here. The foreseen serialized len should be the
            // same as the actual length.
            assert_eq!(buffer.len(), serialized_len_after_hello);
        }
    }

    #[test]
    fn test_empty() {
        let mut compressed_stream_writer: CompressedStreamWriter =
            CompressedStreamWriter::with_block_threshold(1_000);
        let len_upper_bound = compressed_stream_writer.serialized_len_upperbound_after("");
        compressed_stream_writer.append("");
        let buf = compressed_stream_writer.finish();
        let vals: Vec<String> = deserialize_stream(&mut &buf[..]).unwrap();
        assert!(buf.len() <= len_upper_bound);
        assert_eq!(vals.len(), 1);
        assert_eq!(vals[0], "");
    }

    proptest! {
        #[test]
            fn test_proptest_compressed_stream(payload in proptest::collection::vec(".{0,1000}", 1..100)) {
                let mut compressed_stream_writer: CompressedStreamWriter =
                    CompressedStreamWriter::with_block_threshold(1_000);
                for s in &payload[..payload.len() - 1] {
                    compressed_stream_writer.append(s);
                }
                let len_upper_bound = compressed_stream_writer.serialized_len_upperbound_after(&payload[payload.len() - 1]);
                compressed_stream_writer.append(&payload[payload.len() - 1]);
                let buf = compressed_stream_writer.finish();
                let vals: Vec<String> = deserialize_stream(&mut &buf[..]).unwrap();
                assert!(buf.len() <= len_upper_bound);
                assert_eq!(vals.len(), payload.len());
                for (left, right) in vals.iter().zip(payload.iter()) {
                    assert_eq!(left, right);
                }
            }
    }
}
