//! Compression containers recognized on the way in.
//!
//! The only module with conditional compilation, so every other file compiles the same way
//! whatever the feature set. Detection is by magic bytes rather than by name, because
//! `bgzip` output is conventionally called `.gz`, a `.fq` may be gzip, and a pipe has no
//! name at all.

use std::io::{self, Read};

/// A compression container.
///
/// Containers this build cannot decode are still recognized, which is what turns an
/// unreadable complaint about the data into an instruction the reader can follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    /// No container; the bytes are the records.
    Plain,
    /// RFC 1952 gzip, one member or many.
    Gzip,
    /// Blocked gzip: members carrying a `BC` extra field, each independently decodable.
    /// Decodes exactly like [`Codec::Gzip`], and is distinguished because a seekable BGZF
    /// file can be decoded block by block while a piped one cannot.
    Bgzf,
    /// RFC 8878 Zstandard, one frame or many.
    Zstd,
    /// bzip2, one stream or many.
    Bzip2,
    /// xz, one stream or many.
    Xz,
    /// lz4, one frame or many, in the framed layout rather than a bare block.
    Lz4,
}

impl Codec {
    /// Upper bound on what detection reads. The longest magic number is six bytes, and a
    /// BGZF extra field ends by eighteen in every file `bgzip` writes.
    pub const PEEK_BYTES: usize = 64;

    /// Reading less than this from a decoder makes inflate syscall-bound.
    ///
    /// Every user is a decode arm, so a build with no codec at all has none.
    #[cfg(any(
        feature = "gzip",
        feature = "zstd",
        feature = "bzip2",
        feature = "xz",
        feature = "lz4"
    ))]
    const READ_CHUNK_BYTES: usize = 256 << 10;

    /// The container `prefix` opens. A prefix too short to match reads as [`Codec::Plain`],
    /// and the format detector then reports the real problem.
    ///
    /// Which container, and nothing about which tier: a plain xz stream and a blocked one
    /// share every byte matched here, and [`crate::blocks::block_access`] is what tells them
    /// apart.
    pub fn detect(prefix: &[u8]) -> Self {
        match prefix {
            [0x28, 0xb5, 0x2f, 0xfd, ..] => Codec::Zstd,
            [0xfd, b'7', b'z', b'X', b'Z', 0x00, ..] => Codec::Xz,
            [b'B', b'Z', b'h', ..] => Codec::Bzip2,
            // The framed magic, a skippable frame ahead of it, and the legacy magic, all
            // little endian. A file opening on a skippable frame is legal and common enough
            // that missing it would send a whole container to the format detector.
            [0x04, 0x22, 0x4d, 0x18, ..] => Codec::Lz4,
            [0x50..=0x5f, 0x2a, 0x4d, 0x18, ..] => Codec::Lz4,
            [0x02, 0x21, 0x4c, 0x18, ..] => Codec::Lz4,
            [0x1f, 0x8b, ..] if declares_bgzf_block(prefix) => Codec::Bgzf,
            [0x1f, 0x8b, ..] => Codec::Gzip,
            _ => Codec::Plain,
        }
    }

    /// How many bytes must be on hand before [`Codec::detect`] stops wanting more.
    ///
    /// A first byte opening no container settles the question at once, which is what keeps
    /// an interactive stream responsive: nothing here waits for a byte it cannot use.
    pub fn wanted_bytes(prefix: &[u8]) -> usize {
        match prefix {
            [] | [0x1f] | [0x28] | [0xfd] | [b'B'] => 6,
            [0x04] | [0x02] | [0x50..=0x5f] => 4,
            [0x1f, 0x8b, ..] if prefix.len() >= 12 => {
                (12 + u16::from_le_bytes([prefix[10], prefix[11]]) as usize).min(Self::PEEK_BYTES)
            }
            [0x1f, 0x8b, ..] => 12,
            [0x28, 0xb5, ..] => 4,
            [0xfd, b'7', ..] => 6,
            [b'B', b'Z', ..] => 3,
            [0x04, 0x22, ..] | [0x02, 0x21, ..] | [0x50..=0x5f, 0x2a, ..] => 4,
            _ => 0,
        }
    }

    /// Human name, for an error message.
    pub fn name(self) -> &'static str {
        match self {
            Codec::Plain => "no container",
            Codec::Gzip => "gzip",
            Codec::Bgzf => "BGZF, a blocked gzip",
            Codec::Zstd => "Zstandard",
            Codec::Bzip2 => "bzip2",
            Codec::Xz => "xz",
            Codec::Lz4 => "lz4",
        }
    }

    /// The feature that would decode it, or `None` when no feature setting decodes it.
    pub fn feature(self) -> Option<&'static str> {
        match self {
            Codec::Gzip | Codec::Bgzf => Some("gzip"),
            Codec::Zstd => Some("zstd"),
            Codec::Bzip2 => Some("bzip2"),
            Codec::Xz => Some("xz"),
            Codec::Lz4 => Some("lz4"),
            Codec::Plain => None,
        }
    }

    /// A command that decompresses it on the way in.
    pub fn decompressor(self) -> &'static str {
        match self {
            Codec::Plain => "cat",
            Codec::Gzip | Codec::Bgzf => "gzip --decompress --stdout",
            Codec::Zstd => "zstd --decompress --stdout",
            Codec::Bzip2 => "bzip2 --decompress --stdout",
            Codec::Xz => "xz --decompress --stdout",
            Codec::Lz4 => "lz4 --decompress --stdout",
        }
    }

    /// Wrap `source` in a decoder, or explain that this build has none.
    pub fn decode<R: Read + 'static>(self, source: R) -> io::Result<Box<dyn Read>> {
        match self {
            Codec::Plain => Ok(Box::new(source)),

            #[cfg(feature = "gzip")]
            Codec::Gzip | Codec::Bgzf => Ok(Box::new(flate2::bufread::MultiGzDecoder::new(
                io::BufReader::with_capacity(Self::READ_CHUNK_BYTES, source),
            ))),
            #[cfg(not(feature = "gzip"))]
            Codec::Gzip | Codec::Bgzf => Err(self.unsupported()),

            // The zstd reader concatenates frames until end of input unless asked for a
            // single one, so `cat a.zst b.zst` reads back whole.
            #[cfg(feature = "zstd")]
            Codec::Zstd => Ok(Box::new(zstd::stream::read::Decoder::with_buffer(
                io::BufReader::with_capacity(Self::READ_CHUNK_BYTES, source),
            )?)),
            #[cfg(not(feature = "zstd"))]
            Codec::Zstd => Err(self.unsupported()),

            #[cfg(feature = "bzip2")]
            Codec::Bzip2 => Ok(Box::new(bzip2::bufread::MultiBzDecoder::new(
                io::BufReader::with_capacity(Self::READ_CHUNK_BYTES, source),
            ))),
            #[cfg(not(feature = "bzip2"))]
            Codec::Bzip2 => Err(self.unsupported()),

            // The `true` asks the xz reader to keep going past a stream footer, so
            // `cat a.xz b.xz` reads back whole.
            #[cfg(feature = "xz")]
            Codec::Xz => Ok(Box::new(lzma_rust2::XzReader::new(
                io::BufReader::with_capacity(Self::READ_CHUNK_BYTES, source),
                true,
            ))),
            #[cfg(not(feature = "xz"))]
            Codec::Xz => Err(self.unsupported()),

            #[cfg(feature = "lz4")]
            Codec::Lz4 => Ok(Box::new(Lz4Frames::new(io::BufReader::with_capacity(
                Self::READ_CHUNK_BYTES,
                source,
            )))),
            #[cfg(not(feature = "lz4"))]
            Codec::Lz4 => Err(self.unsupported()),
        }
    }

    /// `Unsupported` rather than `InvalidData`: nothing is wrong with the file, and the
    /// message names the feature, the flag, and a pipeline that works right now.
    ///
    /// A build carrying every codec has nothing to say this about, so it does not carry it.
    #[cfg(not(all(
        feature = "gzip",
        feature = "zstd",
        feature = "bzip2",
        feature = "xz",
        feature = "lz4"
    )))]
    fn unsupported(self) -> io::Error {
        let message = match self.feature() {
            Some(feature) => format!(
                "input is compressed with {}, which this build cannot decode: \
                 rebuild with `--features {feature}`, or pipe it through `{}` first",
                self.name(),
                self.decompressor()
            ),
            None => format!(
                "input is compressed with {}, which this toolkit does not decode: \
                 pipe it through `{}` first",
                self.name(),
                self.decompressor()
            ),
        };
        io::Error::new(io::ErrorKind::Unsupported, message)
    }
}

/// Whether a gzip member declares the BGZF `BC` extra field, which is what makes its members
/// independently decodable and therefore splittable.
///
/// Walks the extra subfields rather than assuming `BC` comes first, and answers `false` when
/// the peek ends inside them. A wrong answer costs the block-addressable path and never
/// correctness, since BGZF is valid gzip and decodes identically either way.
fn declares_bgzf_block(prefix: &[u8]) -> bool {
    const FEXTRA: u8 = 0x04;
    if prefix.len() < 12 || prefix[3] & FEXTRA == 0 {
        return false;
    }
    let extra_length = u16::from_le_bytes([prefix[10], prefix[11]]) as usize;
    let Some(extra) = prefix.get(12..12 + extra_length) else {
        return false;
    };
    let mut cursor = 0usize;
    while cursor + 4 <= extra.len() {
        let payload_length = u16::from_le_bytes([extra[cursor + 2], extra[cursor + 3]]) as usize;
        if &extra[cursor..cursor + 2] == b"BC" && payload_length == 2 {
            return true;
        }
        cursor += 4 + payload_length;
    }
    false
}

/// A source that counts what has been taken out of it, so a caller can tell a reader that made
/// no progress from one that made some.
#[cfg(feature = "lz4")]
struct CountingReader<R: Read> {
    inner: R,
    bytes_read: u64,
}

#[cfg(feature = "lz4")]
impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let bytes = self.inner.read(out)?;
        self.bytes_read += bytes as u64;
        Ok(bytes)
    }
}

/// Every lz4 frame of a source, one after another, so `cat a.lz4 b.lz4` reads back whole.
///
/// The frame decoder reports a frame's end mark as end of stream, which for a file of several
/// frames would drop everything past the first. Asking again is what distinguishes the two: a
/// file holding another frame consumes bytes to find it, and a file that is over consumes none.
#[cfg(feature = "lz4")]
struct Lz4Frames<R: Read> {
    decoder: lz4_flex::frame::FrameDecoder<CountingReader<R>>,
}

#[cfg(feature = "lz4")]
impl<R: Read> Lz4Frames<R> {
    fn new(inner: R) -> Self {
        Self {
            decoder: lz4_flex::frame::FrameDecoder::new(CountingReader {
                inner,
                bytes_read: 0,
            }),
        }
    }
}

#[cfg(feature = "lz4")]
impl<R: Read> Read for Lz4Frames<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        loop {
            let before = self.decoder.get_ref().bytes_read;
            match self.decoder.read(out)? {
                0 if self.decoder.get_ref().bytes_read == before => return Ok(0),
                0 => continue,
                bytes => return Ok(bytes),
            }
        }
    }
}

/// A forward-only source whose first bytes were read to decide a codec and are handed back
/// before the source is touched again.
///
/// The prefix is inline, so sniffing allocates nothing, and it is spent on the first read,
/// after which every request is forwarded whole rather than clipped.
pub struct PeekedReader<R: Read> {
    prefix: [u8; Codec::PEEK_BYTES],
    filled: usize,
    replayed: usize,
    inner: R,
}

impl<R: Read> PeekedReader<R> {
    /// Read until the bytes on hand settle the codec question, or the source ends.
    ///
    /// Loops over short reads, because a pipe may hand over one byte at a time and a magic
    /// number split across two reads would otherwise go unrecognized.
    pub fn sniffing(mut inner: R) -> io::Result<Self> {
        let mut prefix = [0u8; Codec::PEEK_BYTES];
        let mut filled = 0usize;
        while filled < Codec::wanted_bytes(&prefix[..filled]).min(Codec::PEEK_BYTES) {
            match inner.read(&mut prefix[filled..])? {
                0 => break,
                bytes => filled += bytes,
            }
        }
        Ok(Self {
            prefix,
            filled,
            replayed: 0,
            inner,
        })
    }

    /// The sniffed bytes, which is what a codec is decided from.
    pub fn peeked(&self) -> &[u8] {
        &self.prefix[..self.filled]
    }

    /// The source, for a caller that decided not to read it forward after all.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Read> Read for PeekedReader<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.replayed < self.filled {
            let bytes = (self.filled - self.replayed).min(out.len());
            out[..bytes].copy_from_slice(&self.prefix[self.replayed..self.replayed + bytes]);
            self.replayed += bytes;
            return Ok(bytes);
        }
        self.inner.read(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A BGZF header: gzip magic, `FEXTRA` set, and a `BC` subfield carrying `BSIZE - 1`.
    fn bgzf_header(block_size: usize) -> Vec<u8> {
        let mut header = vec![0x1f, 0x8b, 0x08, 0x04, 0, 0, 0, 0, 0, 0xff, 6, 0];
        header.extend_from_slice(b"BC");
        header.extend_from_slice(&2u16.to_le_bytes());
        header.extend_from_slice(&((block_size - 1) as u16).to_le_bytes());
        header
    }

    #[test]
    fn recognizes_each_container() {
        assert_eq!(Codec::detect(&[0x28, 0xb5, 0x2f, 0xfd, 0]), Codec::Zstd);
        assert_eq!(Codec::detect(b"\xfd7zXZ\x00"), Codec::Xz);
        assert_eq!(Codec::detect(b"BZh9"), Codec::Bzip2);
        assert_eq!(Codec::detect(&bgzf_header(1000)), Codec::Bgzf);
        assert_eq!(
            Codec::detect(&[0x1f, 0x8b, 0x08, 0x00, 0, 0, 0, 0, 0, 0]),
            Codec::Gzip
        );
        assert_eq!(
            Codec::detect(&[0x04, 0x22, 0x4d, 0x18, 0x64, 0x70]),
            Codec::Lz4
        );
    }

    /// An lz4 file may open on a skippable frame or carry the legacy layout, and both are the
    /// same container to a reader that has to pick a decoder.
    #[test]
    fn the_other_two_lz4_layouts_are_the_same_container() {
        assert_eq!(
            Codec::detect(&[0x50, 0x2a, 0x4d, 0x18, 8, 0, 0, 0]),
            Codec::Lz4
        );
        assert_eq!(
            Codec::detect(&[0x5f, 0x2a, 0x4d, 0x18, 8, 0, 0, 0]),
            Codec::Lz4
        );
        assert_eq!(
            Codec::detect(&[0x02, 0x21, 0x4c, 0x18, 8, 0, 0, 0]),
            Codec::Lz4
        );
    }

    #[test]
    fn sequence_data_is_not_a_container() {
        assert_eq!(Codec::detect(b">seq1\nACGT\n"), Codec::Plain);
        assert_eq!(Codec::detect(b"@read1\nACGT\n"), Codec::Plain);
        assert_eq!(Codec::detect(b""), Codec::Plain);
        // A lone magic byte decides nothing and must not be guessed at.
        assert_eq!(Codec::detect(&[0x1f]), Codec::Plain);
    }

    /// A gzip member without the `BC` subfield is plain gzip, however many other extras it
    /// carries, because only `BC` makes its members independently decodable.
    #[test]
    fn extra_fields_without_bc_are_plain_gzip() {
        let mut header = vec![0x1f, 0x8b, 0x08, 0x04, 0, 0, 0, 0, 0, 0xff, 6, 0];
        header.extend_from_slice(b"XY");
        header.extend_from_slice(&2u16.to_le_bytes());
        header.extend_from_slice(&[0, 0]);
        assert_eq!(Codec::detect(&header), Codec::Gzip);
    }

    /// Sequence data settles at one byte, so an interactive pipe is never held up.
    #[test]
    fn a_record_sigil_settles_detection_immediately() {
        assert_eq!(Codec::wanted_bytes(b">"), 0);
        assert_eq!(Codec::wanted_bytes(b"@"), 0);
        assert!(Codec::wanted_bytes(&[0x1f]) > 1);
    }

    /// Sniffing must reconstruct the source exactly, however the bytes arrive.
    #[test]
    fn peeked_bytes_are_replayed_in_full() {
        let source: Vec<u8> = (0..500u32).map(|value| value as u8).collect();
        for limit in [1, 3, 7, 64, 4096] {
            let mut reader = PeekedReader::sniffing(crate::fixtures::ShortReader::new(
                io::Cursor::new(source.clone()),
                limit,
            ))
            .unwrap();
            let mut replayed = Vec::new();
            reader.read_to_end(&mut replayed).unwrap();
            assert_eq!(replayed, source, "read limit {limit}");
        }
    }

    #[test]
    fn a_source_shorter_than_the_peek_still_replays() {
        let mut reader = PeekedReader::sniffing(io::Cursor::new(b">a\nACGT\n".to_vec())).unwrap();
        let mut replayed = Vec::new();
        reader.read_to_end(&mut replayed).unwrap();
        assert_eq!(replayed, b">a\nACGT\n");
    }

    #[cfg(not(feature = "gzip"))]
    #[test]
    fn a_disabled_gzip_names_its_feature() {
        let error = Codec::Gzip
            .decode(io::Cursor::new(Vec::new()))
            .err()
            .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(error.to_string().contains("--features gzip"), "{error}");
    }

    #[cfg(not(feature = "zstd"))]
    #[test]
    fn a_disabled_zstd_names_its_feature() {
        let error = Codec::Zstd
            .decode(io::Cursor::new(Vec::new()))
            .err()
            .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(error.to_string().contains("--features zstd"), "{error}");
    }

    #[cfg(not(feature = "bzip2"))]
    #[test]
    fn a_disabled_bzip2_names_its_feature() {
        let error = Codec::Bzip2
            .decode(io::Cursor::new(Vec::new()))
            .err()
            .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(error.to_string().contains("--features bzip2"), "{error}");
    }

    #[cfg(not(feature = "xz"))]
    #[test]
    fn a_disabled_xz_names_its_feature() {
        let error = Codec::Xz.decode(io::Cursor::new(Vec::new())).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(error.to_string().contains("--features xz"), "{error}");
        assert!(error.to_string().contains("xz --decompress"), "{error}");
    }

    /// Sequence text long enough to outrun the decoder's internal buffers.
    #[cfg(any(feature = "zstd", feature = "bzip2", feature = "xz", feature = "lz4"))]
    fn sequence_payload() -> Vec<u8> {
        let mut payload = Vec::new();
        for record in 0..8192u32 {
            payload.extend_from_slice(format!(">seq{record}\nACGTTGCAACGTTGCA\n").as_bytes());
        }
        payload
    }

    /// Everything `codec` yields from `encoded`, which is what a caller reads.
    #[cfg(any(feature = "zstd", feature = "bzip2", feature = "xz", feature = "lz4"))]
    fn decoded(codec: Codec, encoded: Vec<u8>) -> Vec<u8> {
        let mut plain = Vec::new();
        codec
            .decode(io::Cursor::new(encoded))
            .unwrap()
            .read_to_end(&mut plain)
            .unwrap();
        plain
    }

    #[cfg(feature = "bzip2")]
    fn bzip2_encoded(payload: &[u8]) -> Vec<u8> {
        let mut encoded = Vec::new();
        bzip2::read::BzEncoder::new(payload, bzip2::Compression::fast())
            .read_to_end(&mut encoded)
            .unwrap();
        encoded
    }

    #[cfg(feature = "xz")]
    fn xz_encoded(payload: &[u8]) -> Vec<u8> {
        use io::Write;
        let mut writer =
            lzma_rust2::XzWriter::new(Vec::new(), lzma_rust2::XzOptions::with_preset(1)).unwrap();
        writer.write_all(payload).unwrap();
        writer.finish().unwrap()
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_round_trips() {
        let payload = sequence_payload();
        let encoded = zstd::encode_all(payload.as_slice(), 3).unwrap();
        assert_eq!(Codec::detect(&encoded), Codec::Zstd);
        assert_eq!(decoded(Codec::Zstd, encoded), payload);
    }

    #[cfg(feature = "bzip2")]
    #[test]
    fn bzip2_round_trips() {
        let payload = sequence_payload();
        let encoded = bzip2_encoded(&payload);
        assert_eq!(Codec::detect(&encoded), Codec::Bzip2);
        assert_eq!(decoded(Codec::Bzip2, encoded), payload);
    }

    /// Two frames back to back are one stream, which is what `cat a.zst b.zst` writes.
    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_joins_concatenated_frames() {
        let mut joined = zstd::encode_all(&b">a\nACGT\n"[..], 3).unwrap();
        joined.extend_from_slice(&zstd::encode_all(&b">b\nTGCA\n"[..], 3).unwrap());
        assert_eq!(decoded(Codec::Zstd, joined), b">a\nACGT\n>b\nTGCA\n");
    }

    /// Two streams back to back are one stream, which is what `cat a.bz2 b.bz2` writes.
    #[cfg(feature = "bzip2")]
    #[test]
    fn bzip2_joins_concatenated_streams() {
        let mut joined = bzip2_encoded(b">a\nACGT\n");
        joined.extend_from_slice(&bzip2_encoded(b">b\nTGCA\n"));
        assert_eq!(decoded(Codec::Bzip2, joined), b">a\nACGT\n>b\nTGCA\n");
    }

    #[cfg(feature = "xz")]
    #[test]
    fn xz_round_trips() {
        let payload = sequence_payload();
        let encoded = xz_encoded(&payload);
        assert_eq!(Codec::detect(&encoded), Codec::Xz);
        assert_eq!(decoded(Codec::Xz, encoded), payload);
    }

    /// Two streams back to back are one stream, which is what `cat a.xz b.xz` writes.
    #[cfg(feature = "xz")]
    #[test]
    fn xz_joins_concatenated_streams() {
        let mut joined = xz_encoded(b">a\nACGT\n");
        joined.extend_from_slice(&xz_encoded(b">b\nTGCA\n"));
        assert_eq!(decoded(Codec::Xz, joined), b">a\nACGT\n>b\nTGCA\n");
    }

    #[cfg(feature = "lz4")]
    fn lz4_encoded(payload: &[u8]) -> Vec<u8> {
        use io::Write;
        let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
        encoder.write_all(payload).unwrap();
        encoder.finish().unwrap()
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn lz4_round_trips() {
        let payload = sequence_payload();
        let encoded = lz4_encoded(&payload);
        assert_eq!(Codec::detect(&encoded), Codec::Lz4);
        assert_eq!(decoded(Codec::Lz4, encoded), payload);
    }

    /// Two frames back to back are one stream, which is what `cat a.lz4 b.lz4` writes.
    #[cfg(feature = "lz4")]
    #[test]
    fn lz4_joins_concatenated_frames() {
        let mut joined = lz4_encoded(b">a\nACGT\n");
        joined.extend_from_slice(&lz4_encoded(b">b\nTGCA\n"));
        assert_eq!(decoded(Codec::Lz4, joined), b">a\nACGT\n>b\nTGCA\n");
    }

    #[cfg(not(feature = "lz4"))]
    #[test]
    fn a_disabled_lz4_names_its_feature() {
        let error = Codec::Lz4
            .decode(io::Cursor::new(Vec::new()))
            .err()
            .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(error.to_string().contains("--features lz4"), "{error}");
    }
}
