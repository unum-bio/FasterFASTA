//! What a record is, and the one parser that reads them.
//!
//! Two regions:
//!
//! - __Records__ — what a record is, plus format detection and Phred conversions.
//!   A record is always a borrow; nothing here owns per-record bytes.
//! - __Parsing__ — one parser core returning [`ParseOutcome`], whose
//!   [`ParseOutcome::Incomplete`] separates "the buffer ended mid-record" from "the input is
//!   malformed".
//!   That single distinction lets the same parser serve a whole memory map, a partially
//!   filled stream window, and a byte range handed to one worker, which is why there is no
//!   second parser anywhere in this crate.
//!
//! Everything here is pure: no file is opened, no thread is spawned, and no byte range is
//! chosen.
//! The one layer it reaches for is [`crate::codecs`], asked to name the container behind a
//! first byte that opens neither format, so the error says "gzip" rather than "expected '@'".
//! The shapes an input can arrive in live one level up, in [`crate::files`].

use std::io;

use stringzilla::sz::{bytesum, find, find_byteset, hash_with_seed, rfind, Byteset};

// region: Records

/// Which of the two sequence file formats a stream carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceFormat {
    /// Header and sequence, opened by `>`, with no per-base quality.
    Fasta,
    /// Header, sequence, separator, and an equal-length quality line, opened by `@`.
    Fastq,
}

impl SequenceFormat {
    /// The byte that opens a record header in this format.
    #[inline]
    pub fn sigil(self) -> u8 {
        match self {
            SequenceFormat::Fasta => b'>',
            SequenceFormat::Fastq => b'@',
        }
    }
}

/// One parsed record.
///
/// `sequence` and `quality` are logical values with every line break and interior space
/// already removed, so consumers never repeat that work and never have to ask whether the
/// underlying file was wrapped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Record<'a> {
    /// Header line including its leading `>` or `@`, excluding the line terminator.
    pub header: &'a [u8],
    /// Sequence bytes, whitespace removed.
    pub sequence: &'a [u8],
    /// Quality bytes, whitespace removed, equal in length to `sequence`.
    /// Empty for FASTA, which also makes an empty FASTQ read indistinguishable — consult
    /// the format rather than this field when the difference matters.
    pub quality: &'a [u8],
}

impl<'a> Record<'a> {
    /// Header with the format sigil stripped, which is what identifier matching wants.
    #[inline]
    pub fn header_without_sigil(&self) -> &'a [u8] {
        match self.header.split_first() {
            Some((&(b'>' | b'@'), rest)) => rest,
            _ => self.header,
        }
    }

    /// Format this record was parsed from, taken from its sigil.
    #[inline]
    pub fn format(&self) -> SequenceFormat {
        match self.header.first() {
            Some(b'@') => SequenceFormat::Fastq,
            _ => SequenceFormat::Fasta,
        }
    }

    /// Identifier: the header up to the first space or tab, sigil stripped, without any
    /// trailing `/1` or `/2` mate suffix.
    ///
    /// Dropping the mate suffix is what makes paired R1 and R2 files agree on a record's
    /// identity, so a keyed sample selects matching pairs. Modern Illumina headers put the
    /// mate number after the first blank and are unaffected.
    pub fn identifier(&self) -> &'a [u8] {
        let without_sigil = self.header_without_sigil();
        let end = find_byteset(without_sigil, FIELD_SEPARATORS).unwrap_or(without_sigil.len());
        match &without_sigil[..end] {
            [head @ .., b'/', b'1' | b'2'] => head,
            field => field,
        }
    }
}

/// Reusable normalization buffers owned by a parser and refilled per record.
///
/// Sequence and quality are kept apart so a single record can borrow both at once; one
/// shared buffer would make that impossible. Capacity is retained across records, so a run
/// over a uniform file allocates only while warming up.
#[derive(Debug, Default)]
pub struct RecordScratch {
    sequence: Vec<u8>,
    quality: Vec<u8>,
}

impl RecordScratch {
    /// Empty buffers that grow to the widest record seen.
    pub fn new() -> Self {
        Self::default()
    }

    /// Retained sequence capacity, for tests asserting that reuse actually happens.
    pub fn sequence_capacity(&self) -> usize {
        self.sequence.capacity()
    }

    /// Retained quality capacity, for tests asserting that reuse actually happens.
    pub fn quality_capacity(&self) -> usize {
        self.quality.capacity()
    }
}

/// Space and tab, the header field separators.
const FIELD_SEPARATORS: Byteset = Byteset::from_bytes(b" \t");

/// Everything stripped from a sequence or quality region.
const WHITESPACE: Byteset = Byteset::from_bytes(b" \t\r\n");

/// Format of the first record, or `None` when the input holds no records.
///
/// An input with nothing in it is empty, not malformed, so a filter that rejects every
/// record still composes into the next tool in a pipeline. Only a first byte that opens
/// neither format is an error.
pub fn sequence_format(data: &[u8]) -> io::Result<Option<SequenceFormat>> {
    // Spelled out rather than `is_ascii_whitespace`, which also accepts a form feed and a
    // vertical tab: neither opens a record, and neither may be skipped over to reach one.
    let blank = |byte: u8| matches!(byte, b' ' | b'\t' | b'\r' | b'\n');
    let Some(&byte) = data.iter().find(|&&byte| !blank(byte)) else {
        return Ok(None);
    };
    match byte {
        b'@' => Ok(Some(SequenceFormat::Fastq)),
        b'>' => Ok(Some(SequenceFormat::Fasta)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            match crate::codecs::Codec::detect(data) {
                crate::codecs::Codec::Plain => format!(
                    "unknown format: expected '@' or '>', found {:?}",
                    byte as char
                ),
                // Only one container is peeled, deliberately: a second layer is far more
                // often a mistake than an intention.
                codec => format!(
                    "input is compressed with {}, not FASTA or FASTQ",
                    codec.name()
                ),
            },
        )),
    }
}

/// Refuse `flag` when the input carries no per-base quality for it to read.
///
/// A FASTA record has no quality, so a quality flag would silently pass or reject every record
/// and exit successfully. Saying so is the only honest answer.
pub fn needs_fastq(flag: &str, format: SequenceFormat) -> io::Result<()> {
    match format {
        SequenceFormat::Fastq => Ok(()),
        SequenceFormat::Fasta => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{flag} needs FASTQ input, but this input is FASTA"),
        )),
    }
}

/// ASCII quality character to Phred score, Phred+33 as used by Illumina 1.8 and later.
#[inline]
pub fn ascii_to_phred33(ascii: u8) -> u8 {
    ascii.saturating_sub(33)
}

/// Phred score back to its ASCII quality character.
#[inline]
pub fn phred33_to_ascii(phred: u8) -> u8 {
    phred.saturating_add(33).min(126)
}

/// Mean Phred score across a quality string.
pub fn mean_quality(quality: &[u8]) -> f32 {
    phred33_sum(quality) as f32 / quality.len().max(1) as f32
}

/// Sum of Phred scores across a quality string.
///
/// Every score is its byte less 33, so one byte sum gives the total exactly.
pub fn phred33_sum(quality: &[u8]) -> u64 {
    bytesum(quality).saturating_sub(33 * quality.len() as u64)
}

/// Which part of a record its identity is taken from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordKey {
    /// Header up to the first blank, with any paired-end mate suffix removed.
    Identifier,
    /// Whole header line, excluding the sigil.
    Header,
    /// Sequence bytes, whitespace already removed.
    Sequence,
}

/// Uracil, whose presence without thymine marks a sequence as RNA.
const URACIL: Byteset = Byteset::from_bytes(b"Uu");

/// Thymine, whose presence marks a sequence as DNA even alongside uracil.
const THYMINE: Byteset = Byteset::from_bytes(b"Tt");

/// Complement tables over the full IUPAC nucleotide alphabet, in both cases.
///
/// Two tables rather than one, because adenine pairs with thymine in DNA and with uracil in
/// RNA. Folding both into a single table makes reverse complementing stop being an involution
/// and quietly transcribes one alphabet into the other.
#[derive(Debug, Clone)]
pub struct Complements {
    dna: [u8; 256],
    rna: [u8; 256],
}

impl Default for Complements {
    fn default() -> Self {
        Self::new()
    }
}

impl Complements {
    /// Both tables, built once and reused for the life of a run.
    pub fn new() -> Self {
        let mut dna: [u8; 256] = core::array::from_fn(|index| index as u8);

        // Anything outside the alphabet maps to itself, so unexpected bytes survive a round
        // trip rather than being silently rewritten.
        const PAIRS: &[(u8, u8)] = &[
            (b'A', b'T'),
            (b'T', b'A'),
            (b'G', b'C'),
            (b'C', b'G'),
            // Ambiguity codes: each maps to the complement of its base set.
            (b'R', b'Y'), // A/G  -> T/C
            (b'Y', b'R'),
            (b'S', b'S'), // G/C is self-complementary
            (b'W', b'W'), // A/T is self-complementary
            (b'K', b'M'), // G/T  -> C/A
            (b'M', b'K'),
            (b'B', b'V'), // not-A -> not-T
            (b'V', b'B'),
            (b'D', b'H'), // not-C -> not-G
            (b'H', b'D'),
            (b'N', b'N'),
        ];

        for &(base, complement) in PAIRS {
            dna[base as usize] = complement;
            dna[base.to_ascii_lowercase() as usize] = complement.to_ascii_lowercase();
        }

        let mut rna = dna;
        for (base, complement) in [(b'A', b'U'), (b'U', b'A')] {
            rna[base as usize] = complement;
            rna[base.to_ascii_lowercase() as usize] = complement.to_ascii_lowercase();
        }
        Self { dna, rna }
    }

    /// The table matching this sequence's alphabet.
    ///
    /// Uracil without thymine marks RNA; everything else, including an empty sequence, is
    /// complemented as DNA.
    pub fn table_for(&self, sequence: &[u8]) -> &[u8; 256] {
        match find_byteset(sequence, URACIL).is_some() && find_byteset(sequence, THYMINE).is_none()
        {
            true => &self.rna,
            false => &self.dna,
        }
    }
}

/// The bytes a record is identified by, borrowed from the record itself.
///
/// Deduplication compares these bytes when two records collide under [`digest`], so the two
/// must agree on what a key is; naming the selection once is what guarantees they do.
pub fn key_bytes<'a>(record: &Record<'a>, key: RecordKey) -> &'a [u8] {
    match key {
        RecordKey::Identifier => record.identifier(),
        RecordKey::Header => record.header_without_sigil(),
        RecordKey::Sequence => record.sequence,
    }
}

/// Seeded 64-bit digest of one record field.
///
/// Pure, so the same record yields the same digest whatever its position in the input, how
/// the inputs were ordered, or how many workers ran. That is what lets sampling and
/// deduplication run wide without changing their answers.
pub fn digest(record: &Record<'_>, key: RecordKey, seed: u64) -> u64 {
    hash_with_seed(key_bytes(record, key), seed)
}

// endregion: Records

// region: Parsing

/// Outcome of attempting to parse one record from the front of a buffer.
#[derive(Debug)]
pub enum ParseOutcome<'a> {
    /// A complete record, and how many bytes of the buffer it consumed.
    Record(Record<'a>, usize),
    /// The buffer ends mid-record. Read more and retry; this is not an error.
    Incomplete,
    /// The input is malformed and no amount of additional data will help.
    Invalid(io::Error),
}

/// At end of input a partial record is a truncation, not a request for more bytes.
fn incomplete_or_truncated<'a>(at_eof: bool, what: &str) -> ParseOutcome<'a> {
    if at_eof {
        ParseOutcome::Invalid(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("truncated input: {what} is incomplete"),
        ))
    } else {
        ParseOutcome::Incomplete
    }
}

/// End of the line starting at `from`, exclusive of the terminator.
#[inline]
fn find_line_end(buffer: &[u8], from: usize) -> Option<usize> {
    if from >= buffer.len() {
        return None;
    }
    find(&buffer[from..], b"\n").map(|offset| from + offset)
}

/// Anything that is not whitespace, so a scan can find where content starts.
const CONTENT: Byteset = WHITESPACE.inverted();

/// Offset of the first byte that is not a line terminator or blank.
#[inline]
fn skip_leading_whitespace(buffer: &[u8]) -> usize {
    find_byteset(buffer, CONTENT).unwrap_or(buffer.len())
}

/// Line length excluding a trailing carriage return, so CRLF files count correctly.
#[inline]
fn length_without_carriage_return(line: &[u8]) -> usize {
    match line.last() {
        Some(b'\r') => line.len() - 1,
        _ => line.len(),
    }
}

#[inline]
fn trim_carriage_return(line: &[u8]) -> &[u8] {
    match line.last() {
        Some(b'\r') => &line[..line.len() - 1],
        _ => line,
    }
}

/// `region` with all whitespace removed.
///
/// Returns `region` itself when it holds none, which is the common case for an unwrapped
/// FASTQ read and costs one `find_byteset` scan and no copy. Otherwise the bytes are
/// gathered into `scratch` and a view of that is returned. Sharing one lifetime across the
/// input, the scratch, and the result is what lets a single return type cover both.
fn strip_whitespace<'a>(region: &'a [u8], scratch: &'a mut Vec<u8>) -> &'a [u8] {
    if find_byteset(region, WHITESPACE).is_none() {
        return region;
    }

    scratch.clear();
    scratch.reserve(region.len());

    let mut cursor = 0;
    while cursor < region.len() {
        let next = find_byteset(&region[cursor..], WHITESPACE)
            .map(|offset| cursor + offset)
            .unwrap_or(region.len());
        if next > cursor {
            // Each run of payload bytes is gathered in a single write.
            scratch.extend_from_slice(&region[cursor..next]);
        }
        cursor = if next < region.len() { next + 1 } else { next };
    }
    scratch
}

/// Parse one record of either format from the front of `buffer`.
pub fn parse_leading_record<'a>(
    buffer: &'a [u8],
    format: SequenceFormat,
    at_eof: bool,
    scratch: &'a mut RecordScratch,
) -> ParseOutcome<'a> {
    match format {
        SequenceFormat::Fasta => parse_leading_fasta_record(buffer, at_eof, scratch),
        SequenceFormat::Fastq => parse_leading_fastq_record(buffer, at_eof, scratch),
    }
}

/// Parse one FASTA record.
///
/// A FASTA record ends only where the next one begins, so a record is complete either when
/// a `>` is seen at a line start or when the input is known to be exhausted. That is why
/// `at_eof` is a parameter rather than something the parser could infer.
pub fn parse_leading_fasta_record<'a>(
    buffer: &'a [u8],
    at_eof: bool,
    scratch: &'a mut RecordScratch,
) -> ParseOutcome<'a> {
    let start = skip_leading_whitespace(buffer);
    if start >= buffer.len() {
        return ParseOutcome::Incomplete;
    }
    if buffer[start] != b'>' {
        return ParseOutcome::Invalid(io::Error::new(
            io::ErrorKind::InvalidData,
            "FASTA record must start with '>'",
        ));
    }

    let header_end = match find_line_end(buffer, start) {
        Some(position) => position,
        None if at_eof => buffer.len(),
        None => return ParseOutcome::Incomplete,
    };
    let header = trim_carriage_return(&buffer[start..header_end]);

    let sequence_start = (header_end + 1).min(buffer.len());
    let mut cursor = sequence_start;
    let mut sequence_end = sequence_start;
    let consumed;

    loop {
        if cursor >= buffer.len() {
            if at_eof {
                sequence_end = buffer.len();
                consumed = buffer.len();
                break;
            }
            return ParseOutcome::Incomplete;
        }
        if buffer[cursor] == b'>' {
            // The next record starts here, so this one is complete.
            consumed = cursor;
            break;
        }
        match find_line_end(buffer, cursor) {
            Some(position) => {
                sequence_end = position;
                cursor = position + 1;
            }
            None if at_eof => {
                sequence_end = buffer.len();
                consumed = buffer.len();
                break;
            }
            None => return ParseOutcome::Incomplete,
        }
    }

    let region = &buffer[sequence_start..sequence_end.max(sequence_start)];
    let sequence = strip_whitespace(region, &mut scratch.sequence);

    ParseOutcome::Record(
        Record {
            header,
            sequence,
            quality: b"",
        },
        consumed,
    )
}

/// Parse one FASTQ record.
///
/// Sequence lines run until a line opening with `+`; quality lines are then collected until
/// they match the sequence length. Counting against the sequence length is what makes
/// multi-line quality work, and is why quality is normalized through the same path as
/// sequence rather than sliced contiguously.
pub fn parse_leading_fastq_record<'a>(
    buffer: &'a [u8],
    at_eof: bool,
    scratch: &'a mut RecordScratch,
) -> ParseOutcome<'a> {
    let start = skip_leading_whitespace(buffer);
    if start >= buffer.len() {
        return ParseOutcome::Incomplete;
    }
    if buffer[start] != b'@' {
        return ParseOutcome::Invalid(io::Error::new(
            io::ErrorKind::InvalidData,
            "FASTQ record must start with '@'",
        ));
    }

    let header_end = match find_line_end(buffer, start) {
        Some(position) => position,
        None => return incomplete_or_truncated(at_eof, "FASTQ header"),
    };
    let header = trim_carriage_return(&buffer[start..header_end]);

    // Sequence lines, up to the '+' separator.
    let sequence_start = header_end + 1;
    let mut cursor = sequence_start;
    let mut sequence_end = sequence_start;

    loop {
        if cursor >= buffer.len() {
            return incomplete_or_truncated(at_eof, "FASTQ sequence");
        }
        if buffer[cursor] == b'+' {
            break;
        }
        match find_line_end(buffer, cursor) {
            Some(position) => {
                sequence_end = position;
                cursor = position + 1;
            }
            None => return incomplete_or_truncated(at_eof, "FASTQ sequence"),
        }
    }

    // Destructured so sequence and quality borrow separate buffers simultaneously.
    let RecordScratch {
        sequence: sequence_scratch,
        quality: quality_scratch,
    } = scratch;

    let sequence_region = &buffer[sequence_start..sequence_end.max(sequence_start)];
    let sequence = strip_whitespace(sequence_region, sequence_scratch);
    let sequence_length = sequence.len();

    let separator_end = match find_line_end(buffer, cursor) {
        Some(position) => position,
        None => return incomplete_or_truncated(at_eof, "FASTQ '+' separator"),
    };

    // Quality lines, matched against the sequence length.
    let quality_start = separator_end + 1;
    let mut cursor = quality_start;
    let mut quality_end;
    let mut collected = 0usize;

    loop {
        if cursor > buffer.len() {
            return incomplete_or_truncated(at_eof, "FASTQ quality");
        }
        match find_line_end(buffer, cursor) {
            Some(position) => {
                quality_end = position;
                collected += length_without_carriage_return(&buffer[cursor..position]);
                cursor = position + 1;
            }
            None if at_eof => {
                quality_end = buffer.len();
                collected += length_without_carriage_return(&buffer[cursor.min(buffer.len())..]);
                cursor = buffer.len();
            }
            None => return ParseOutcome::Incomplete,
        }
        if collected >= sequence_length {
            break;
        }
        if cursor >= buffer.len() {
            return incomplete_or_truncated(at_eof, "FASTQ quality");
        }
    }

    let quality_region = &buffer[quality_start..quality_end.max(quality_start)];
    let quality = strip_whitespace(quality_region, quality_scratch);

    if sequence.len() != quality.len() {
        return ParseOutcome::Invalid(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "FASTQ sequence and quality length mismatch: sequence {}, quality {}",
                sequence.len(),
                quality.len()
            ),
        ));
    }

    ParseOutcome::Record(
        Record {
            header,
            sequence,
            quality,
        },
        cursor.min(buffer.len()),
    )
}

/// Whether a candidate `@` opens a FASTQ record, and when the bytes cannot yet say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CycleProof {
    /// The four lines have the shape of a record.
    Holds,
    /// They do not, whatever follows them.
    Fails,
    /// The buffer ended before the fourth line, so the question is still open.
    Truncated,
}

/// Whether a candidate `@` really opens a FASTQ record, judged by the shape of the next four
/// lines rather than by the byte alone.
///
/// This is deliberately stricter than [`parse_leading_record`], which may not stand in for it.
/// A parser assumes it is already at a record and tolerates a sequence wrapped over several
/// lines, so from a quality line it will swallow the following header as sequence and accept a
/// later `+`. Resynchronizing needs the opposite bias: exactly four lines, a `+` on the third,
/// and a fourth matching the second in length.
fn fastq_cycle(bytes: &[u8], candidate: usize) -> CycleProof {
    let mut cursor = candidate;
    let mut lines = [0usize; 4];
    // Where the third line begins, taken from the walk rather than rebuilt from the lengths.
    // The lengths exclude a carriage return, so adding one byte per terminator lands two short
    // of the separator on a CRLF file and no candidate is ever provable.
    let mut separator = candidate;
    for (index, slot) in lines.iter_mut().enumerate() {
        let Some(end) = find_line_end(bytes, cursor) else {
            return CycleProof::Truncated;
        };
        *slot = length_without_carriage_return(&bytes[cursor..end]);
        cursor = end + 1;
        if index == 1 {
            separator = cursor;
        }
    }
    match bytes.get(separator) {
        None => CycleProof::Truncated,
        Some(&byte) if byte == b'+' && lines[1] == lines[3] => CycleProof::Holds,
        Some(_) => CycleProof::Fails,
    }
}

/// The proof a candidate needs, for a caller holding all the bytes there will ever be.
fn cycle_holds(bytes: &[u8], candidate: usize, format: SequenceFormat) -> bool {
    format == SequenceFormat::Fasta || fastq_cycle(bytes, candidate) == CycleProof::Holds
}

/// Where a boundary search over a buffer that may still grow has got to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordBoundary {
    /// A record begins at this offset.
    At(usize),
    /// A candidate here outran the buffer. A caller that can append must search again from
    /// this offset rather than past it, since one more read may prove it.
    Undecided(usize),
    /// No candidate remains in these bytes.
    NotFound,
}

/// First record start at or after `from`, for a caller whose buffer may still be filling.
///
/// A four-line FASTQ cycle is tens of kilobytes on a long read, so a candidate near the end of
/// a parse window is undecided rather than rejected. Reporting the two alike is what makes a
/// streaming caller step past a boundary and drop every record up to the next one.
pub fn first_record_boundary(bytes: &[u8], from: usize, format: SequenceFormat) -> RecordBoundary {
    let sigil = format.sigil();
    let mut undecided = None;

    // Offset zero earns the same proof as any other candidate. A window handed to one worker
    // begins mid-record, so its first byte can be a quality `@` as easily as a header's, and
    // accepting it unproven would parse the previous worker's straddling record a second time.
    if from == 0 && bytes.first() == Some(&sigil) {
        match format {
            SequenceFormat::Fasta => return RecordBoundary::At(0),
            SequenceFormat::Fastq => match fastq_cycle(bytes, 0) {
                CycleProof::Holds => return RecordBoundary::At(0),
                CycleProof::Fails => {}
                CycleProof::Truncated => undecided = Some(0),
            },
        }
    }

    // A record opens a line, so a boundary is a newline immediately followed by the sigil.
    // Searching for both bytes at once leaves the whole scan to one SIMD pass.
    let needle = [b'\n', sigil];
    let mut cursor = from;
    while let Some(found) = find(&bytes[cursor..], needle) {
        let candidate = found + cursor + 1;
        if candidate >= bytes.len() {
            undecided = undecided.or(Some(found + cursor));
            break;
        }
        match format {
            SequenceFormat::Fasta => return RecordBoundary::At(candidate),
            SequenceFormat::Fastq => match fastq_cycle(bytes, candidate) {
                CycleProof::Holds => return RecordBoundary::At(candidate),
                CycleProof::Fails => {}
                CycleProof::Truncated => undecided = undecided.or(Some(candidate - 1)),
            },
        }
        cursor = candidate;
    }

    match undecided {
        Some(resume_from) => RecordBoundary::Undecided(resume_from),
        None => RecordBoundary::NotFound,
    }
}

/// Last record boundary in `bytes` that can be proven, for a driver that must end a slab
/// where a record ends.
///
/// A boundary whose fourth line the slab cut off is refused and an earlier one reported, which
/// is what the caller wants: those bytes are a partial record it carries into the next slab.
pub fn last_record_boundary(bytes: &[u8], format: SequenceFormat) -> Option<usize> {
    let sigil = format.sigil();
    let needle = [b'\n', sigil];
    // Walking backwards one occurrence at a time, since the last `\n` before a sigil need not
    // open a record: in FASTQ it is a quality line at Q31 until the cycle says otherwise.
    let mut searched_back_to = bytes.len();
    while let Some(found) = rfind(&bytes[..searched_back_to], needle) {
        let candidate = found + 1;
        if candidate < bytes.len() && cycle_holds(bytes, candidate, format) {
            return Some(candidate);
        }
        // The window shrinks past the newline just examined, and a two-byte needle cannot
        // match again at that position, so the walk always makes progress.
        searched_back_to = found + 1;
    }

    // Offset zero earns the same proof as any other candidate, and is the only boundary a
    // slab holding one record start can offer.
    (bytes.first() == Some(&sigil) && cycle_holds(bytes, 0, format)).then_some(0)
}

// endregion: Parsing

#[cfg(test)]
mod tests {
    use super::*;

    use crate::fixtures;

    #[test]
    fn detects_both_formats() {
        assert_eq!(
            sequence_format(b">seq1\nACGT\n").unwrap(),
            Some(SequenceFormat::Fasta)
        );
        assert_eq!(
            sequence_format(b"@seq1\nACGT\n+\nIIII\n").unwrap(),
            Some(SequenceFormat::Fastq)
        );
    }

    #[test]
    fn detects_past_leading_whitespace() {
        assert_eq!(
            sequence_format(b"  \n  >seq1\nACGT\n").unwrap(),
            Some(SequenceFormat::Fasta)
        );
    }

    #[test]
    fn rejects_an_unknown_first_byte() {
        assert_eq!(
            sequence_format(b"ACGT\n").unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    /// An input holding no records is empty, not malformed, so a filter that rejects
    /// everything still composes into the next tool in a pipeline.
    #[test]
    fn empty_input_has_no_format_and_is_not_an_error() {
        assert_eq!(sequence_format(b"").unwrap(), None);
        assert_eq!(sequence_format(b"   \n").unwrap(), None);
    }

    #[test]
    fn strips_sigil_and_splits_identifier() {
        let record = Record {
            header: b">sp|P12345| some description here",
            sequence: b"ACGT",
            quality: b"",
        };
        assert_eq!(
            record.header_without_sigil(),
            b"sp|P12345| some description here"
        );
        assert_eq!(record.identifier(), b"sp|P12345|");
    }

    #[test]
    fn identifier_without_description() {
        let record = Record {
            header: b"@read1",
            sequence: b"ACGT",
            quality: b"IIII",
        };
        assert_eq!(record.identifier(), b"read1");
    }

    #[test]
    fn quality_conversion_round_trips() {
        assert_eq!(ascii_to_phred33(b'!'), 0);
        assert_eq!(ascii_to_phred33(b'I'), 40);
        assert_eq!(phred33_to_ascii(0), b'!');
        assert_eq!(phred33_to_ascii(40), b'I');
    }

    #[test]
    fn mean_quality_values() {
        assert_eq!(mean_quality(b"IIII"), 40.0);
        assert_eq!(mean_quality(b"!!!!"), 0.0);
        assert_eq!(mean_quality(b"!I"), 20.0);
        assert_eq!(mean_quality(b""), 0.0);
    }

    /// Header, sequence, and quality of every record one parse pass produced.
    type ParsedRecords = Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>;

    /// Parses to exhaustion over a complete slice.
    fn parse_all(data: &[u8], format: SequenceFormat) -> io::Result<ParsedRecords> {
        let mut scratch = RecordScratch::new();
        let mut out = Vec::new();
        let mut cursor = 0;
        loop {
            let remaining = &data[cursor..];
            if skip_leading_whitespace(remaining) >= remaining.len() {
                return Ok(out);
            }
            match parse_leading_record(remaining, format, true, &mut scratch) {
                ParseOutcome::Record(record, consumed) => {
                    out.push((
                        record.header.to_vec(),
                        record.sequence.to_vec(),
                        record.quality.to_vec(),
                    ));
                    assert!(consumed > 0, "parser made no progress");
                    cursor += consumed;
                }
                ParseOutcome::Incomplete => return Ok(out),
                ParseOutcome::Invalid(error) => return Err(error),
            }
        }
    }

    #[test]
    fn fasta_single_line_records_borrow() {
        let data = b">seq1\nACGT\n>seq2\nTGCA\n";
        let mut scratch = RecordScratch::new();
        match parse_leading_fasta_record(data, true, &mut scratch) {
            ParseOutcome::Record(record, consumed) => {
                assert_eq!(record.header, b">seq1");
                assert_eq!(record.sequence, b"ACGT");
                assert_eq!(consumed, 11);
            }
            other => panic!("expected a record, got {other:?}"),
        }
        // No whitespace inside the region, so nothing was copied.
        assert_eq!(scratch.sequence_capacity(), 0);
    }

    #[test]
    fn fasta_multiline_is_joined() {
        let records =
            parse_all(b">seq1\nACGT\nTGCA\n>seq2\nAAAA\n", SequenceFormat::Fasta).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].1, b"ACGTTGCA");
        assert_eq!(records[1].1, b"AAAA");
    }

    #[test]
    fn fasta_tolerates_blank_lines_and_indentation() {
        let data = b">seq1\n  ACGT  \n\n\tTGCA\t\n>seq2\nAAAA \n TTTT\n";
        let records = parse_all(data, SequenceFormat::Fasta).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].1, b"ACGTTGCA");
        assert_eq!(records[1].1, b"AAAATTTT");
    }

    #[test]
    fn fasta_incomplete_without_eof() {
        let mut scratch = RecordScratch::new();
        // Without the next '>' or end of input, the record could still continue.
        assert!(matches!(
            parse_leading_fasta_record(b">seq1\nACGT\n", false, &mut scratch),
            ParseOutcome::Incomplete
        ));
        assert!(matches!(
            parse_leading_fasta_record(b">seq1\nACGT\n", true, &mut scratch),
            ParseOutcome::Record(..)
        ));
    }

    #[test]
    fn fastq_single_line_records() {
        let records = parse_all(
            b"@seq1\nACGT\n+\nIIII\n@seq2 with description\nTGCA\n+\nHHHH\n",
            SequenceFormat::Fastq,
        )
        .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, b"@seq1");
        assert_eq!(records[0].1, b"ACGT");
        assert_eq!(records[0].2, b"IIII");
        assert_eq!(records[1].0, b"@seq2 with description");
        assert_eq!(records[1].2, b"HHHH");
    }

    /// Multi-line quality is joined to match its sequence, not sliced across the newlines
    /// that separate its lines.
    #[test]
    fn fastq_multiline_quality_is_joined() {
        let records =
            parse_all(b"@seq1\nACGT\nTGCA\n+\nIIII\nHHHH\n", SequenceFormat::Fastq).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1, b"ACGTTGCA");
        assert_eq!(records[0].2, b"IIIIHHHH");
    }

    #[test]
    fn fastq_empty_sequence() {
        let records = parse_all(b"@seq1\n\n+\n\n", SequenceFormat::Fastq).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1, b"");
        assert_eq!(records[0].2, b"");
    }

    #[test]
    fn fastq_length_mismatch_is_invalid() {
        let error = parse_all(b"@seq1\nACGT\n+\n!!!\n", SequenceFormat::Fastq).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn fastq_crlf_is_handled() {
        let data = fixtures::records(fixtures::CARRIAGE_RETURN_FASTQ, 3);
        let records = parse_all(&data, SequenceFormat::Fastq).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].0, b"@read0 sample");
        assert_eq!(records[0].1, b"AAAAAAAA");
        assert_eq!(records[0].2, b"IIIIIIII");
        assert_eq!(records[2].1.len(), records[2].2.len());
    }

    /// Every prefix of a valid input must be `Incomplete`, never `Invalid`. This is the
    /// property the whole driver depends on.
    ///
    /// One record per shape, so every proper prefix really is short of a whole one. The two
    /// 20 kb shapes are left out because the sweep is quadratic in the fixture and they prove
    /// nothing the 4 kb shape does not.
    #[test]
    fn every_prefix_is_incomplete_not_invalid() {
        let mut scratch = RecordScratch::new();
        for shape in fixtures::ALL
            .into_iter()
            .filter(|shape| shape.read_bases < 8192)
        {
            let data = fixtures::records(shape, 1);
            for length in 1..data.len() {
                match parse_leading_record(&data[..length], shape.format, false, &mut scratch) {
                    ParseOutcome::Incomplete => {}
                    ParseOutcome::Record(..) => {
                        panic!("{shape:?}: prefix of {length} bytes parsed as a full record")
                    }
                    ParseOutcome::Invalid(error) => {
                        panic!("{shape:?}: prefix of {length} bytes reported as invalid: {error}")
                    }
                }
            }
        }
    }

    /// Scratch reuse: capacity may grow while warming up to the widest record, but a second
    /// pass over the same input must not allocate at all.
    #[test]
    fn scratch_allocates_only_while_warming_up() {
        let data = b">seq1\nACGT\nTGCA\n>seq2\nGGGGG\nCCCCC\n>seq3\nAAAA\nTTTT\n";
        let mut scratch = RecordScratch::new();

        let count_pass = |scratch: &mut RecordScratch| {
            let mut cursor = 0;
            let mut records = 0;
            while cursor < data.len() {
                match parse_leading_fasta_record(&data[cursor..], true, scratch) {
                    ParseOutcome::Record(_, consumed) => {
                        cursor += consumed;
                        records += 1;
                    }
                    other => panic!("expected a record, got {other:?}"),
                }
            }
            records
        };

        assert_eq!(count_pass(&mut scratch), 3);
        let warmed = scratch.sequence_capacity();
        assert!(warmed > 0, "multi-line records should use scratch");

        assert_eq!(count_pass(&mut scratch), 3);
        assert_eq!(
            scratch.sequence_capacity(),
            warmed,
            "second pass reallocated scratch"
        );
    }

    /// A FASTQ quality line may open with '@', so a naive boundary search lands mid-record.
    #[test]
    fn first_record_boundary_rejects_fastq_quality_opening_with_at() {
        // Quality '@' is Phred 31, entirely legal, and here it opens every fourth line.
        let data = fixtures::records(fixtures::SIGIL_QUALITY_FASTQ, 2);
        let RecordBoundary::At(boundary) = first_record_boundary(&data, 1, SequenceFormat::Fastq)
        else {
            panic!("the second record must be provable")
        };
        assert_eq!(&data[boundary..boundary + 6], b"@read1");
    }

    #[test]
    fn first_record_boundary_finds_the_next_fasta_record() {
        let data = b">a\nACGT\n>b\nTGCA\n";
        assert_eq!(
            first_record_boundary(data, 0, SequenceFormat::Fasta),
            RecordBoundary::At(0)
        );
        assert_eq!(
            first_record_boundary(data, 1, SequenceFormat::Fasta),
            RecordBoundary::At(8)
        );
    }

    /// A four-line proof cut short by the end of the buffer is undecided, and the offset it
    /// reports is one a rescan can still find the same boundary from.
    ///
    /// Long reads make this the common case: a cycle spans tens of kilobytes, so a candidate
    /// near the end of a parse window cannot be proven yet. The boundary is a two-byte needle,
    /// a newline and a sigil, so resuming at the sigil rather than at the newline steps over
    /// the very match the rescan exists to find and drops every record up to the next one.
    /// Asserting only that the offset does not overshoot the boundary tolerates exactly that.
    #[test]
    fn a_rescan_from_an_undecided_offset_finds_the_same_boundary() {
        for shape in fixtures::ALL {
            let data = fixtures::records(shape, fixtures::count_within(shape, 2 << 10));
            let format = shape.format;
            // Bounded, so a twenty-kilobase shape samples its length instead of walking every
            // offset of it.
            let stride = data.len().div_ceil(512).max(1);

            for from in [0usize, 1] {
                let reference = first_record_boundary(&data, from, format);
                let RecordBoundary::At(_) = reference else {
                    panic!("{shape:?}: the whole buffer must decide a boundary, got {reference:?}");
                };

                let mut undecided_count = 0usize;
                for cut in (1..data.len()).step_by(stride) {
                    match first_record_boundary(&data[..cut], from, format) {
                        // A candidate proven inside a prefix stays proven, and it is still the
                        // first one, so more bytes may not move it.
                        decided @ RecordBoundary::At(_) => assert_eq!(
                            decided, reference,
                            "{shape:?} from {from}: the boundary proven within {cut} bytes \
                             moved once the rest of the buffer arrived"
                        ),
                        RecordBoundary::Undecided(resume_from) => {
                            undecided_count += 1;
                            assert_eq!(
                                first_record_boundary(&data, resume_from, format),
                                reference,
                                "{shape:?} from {from}: cut at {cut}, a rescan resuming at \
                                 {resume_from} no longer finds the boundary"
                            );
                        }
                        RecordBoundary::NotFound => {}
                    }
                }

                // FASTA needs no proof, so only FASTQ can leave a candidate undecided, and a
                // property over an outcome that never occurs proves nothing.
                if format == SequenceFormat::Fastq {
                    assert!(
                        undecided_count > 0,
                        "{shape:?} from {from}: no truncation left a candidate unproven"
                    );
                }
            }
        }
    }

    /// A candidate that really is a quality line stays rejected once the proof can complete.
    #[test]
    fn a_complete_proof_still_rejects_a_quality_line() {
        // The fourth line opens with '@' at Phred 31, which is legal quality and not a header.
        let before = "@read0\nACGT\n+\n@III\n";
        let data = format!("{before}@read1\nTGCA\n+\nIIII\n").into_bytes();
        assert_eq!(
            first_record_boundary(&data, 1, SequenceFormat::Fastq),
            RecordBoundary::At(before.len())
        );
    }

    #[test]
    fn first_record_boundary_returns_none_past_the_end() {
        let data = b">a\nACGT\n";
        assert_eq!(
            first_record_boundary(data, 3, SequenceFormat::Fasta),
            RecordBoundary::NotFound
        );
    }

    #[test]
    fn last_record_boundary_finds_the_final_fasta_record() {
        let data = b">a\nACGT\n>b\nTGCA\n>c\nGGGG\n";
        let boundary = last_record_boundary(data, SequenceFormat::Fasta).unwrap();
        assert_eq!(&data[boundary..boundary + 2], b">c");
    }

    /// A slab ends mid-record, so the last boundary is the one before the cut rather than the
    /// candidate whose fourth line the slab never held.
    #[test]
    fn last_record_boundary_refuses_a_cycle_the_slab_cut_off() {
        let data = b"@read0\nACGT\n+\nIIII\n@read1\nTGCA\n+\nII";
        let boundary = last_record_boundary(data, SequenceFormat::Fastq).unwrap();
        assert_eq!(boundary, 0);
    }

    /// Quality '@' at Phred 31 opens a line here, and only the four-line proof tells it apart
    /// from a header.
    #[test]
    fn last_record_boundary_rejects_fastq_quality_opening_with_at() {
        let data = fixtures::records(fixtures::SIGIL_QUALITY_FASTQ, 2);
        let boundary = last_record_boundary(&data, SequenceFormat::Fastq).unwrap();
        assert_eq!(&data[boundary..boundary + 6], b"@read1");
    }

    #[test]
    fn last_record_boundary_finds_nothing_in_bytes_that_open_no_record() {
        assert_eq!(
            last_record_boundary(b"ACGT\nACGT\n", SequenceFormat::Fasta),
            None
        );
        assert_eq!(last_record_boundary(b"", SequenceFormat::Fastq), None);
    }
}
