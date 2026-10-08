//! The inputs an operating system hands a tool, and the bytes it hands back.
//!
//! Two regions:
//!
//! - __Inputs__ — the three shapes an input arrives in, and how records are read out of each.
//! - __Output__ — record formatting and the boilerplate every `main` shares.
//!
//! This layer sits above three others: every record is read through [`crate::records`], a
//! compressed source is decoded through [`crate::codecs`], and [`crate::blocks`] answers whether
//! a mapping is addressable at block granularity. The dependencies run one way only, so none of
//! the three names anything here and a record can be parsed with no file in sight.
//!
//! Scheduling lives in [`crate::scheduling`].
//! Nothing here spawns a thread or picks a range, and nothing here declares a sibling module.

use std::collections::HashSet;
use std::io::{self, Read, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use crate::records::{
    first_record_boundary, parse_leading_record, sequence_format, ParseOutcome, Record,
    RecordBoundary, RecordScratch, SequenceFormat,
};

// region: Inputs

/// A whole input held in memory or memory-mapped, addressable at any offset.
///
/// Any byte range can go to a worker, since [`crate::records::first_record_boundary`] recovers
/// a record boundary from any offset. Generic over its bytes so one type serves the owner
/// of a mapping and the borrower of a view of it alike.
pub struct RandomAccess<B: Deref<Target = [u8]>> {
    data: B,
}

/// A forward-only input: a pipe, a socket, or a decompressor over a plain gzip stream.
///
/// Nothing can be revisited and nothing can be split, so a stream is read once from front to
/// back, which the type states rather than leaving to a runtime check.
pub struct ForwardAccess<R: Read> {
    reader: R,
    window: Vec<u8>,
}

/// Where a run over a stream begins, and what becomes of the bytes before that point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamStart {
    /// Byte zero, which is where a whole file or a whole pipe begins.
    FirstByte,
    /// The first record boundary of the given format, dropping the partial record before it.
    ///
    /// A run of blocks starts at a block, and a block boundary is not a record boundary, so
    /// those leading bytes are the tail of a record the run before owns and emits. The format
    /// comes from the whole input rather than from these bytes, because a window opening
    /// mid-record has no sigil to detect and finding the boundary needs the format anyway.
    FirstBoundary(SequenceFormat),
}

/// Initial parse window for a [`ForwardAccess`], grown only when a single record does not fit.
pub const DEFAULT_WINDOW_BYTES: usize = 1 << 20;

impl<B: Deref<Target = [u8]>> RandomAccess<B> {
    /// Wraps bytes that outlive every record read from them.
    pub fn new(data: B) -> Self {
        Self { data }
    }

    /// A two-word view of the same bytes, for a worker that must not own them.
    pub fn borrowed(&self) -> RandomAccess<&[u8]> {
        RandomAccess { data: &self.data }
    }

    /// The bytes themselves, for a scheduler that slices before anything is parsed.
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Visit every record in `range`, which must start at a record boundary.
    ///
    /// A range holding no records is empty rather than malformed, so a worker handed one
    /// finishes quietly. Records borrow either the input or a scratch buffer reused across the
    /// whole call, so this allocates only while warming up.
    pub fn for_each_record_in(
        &self,
        range: core::ops::Range<usize>,
        mut visit: impl FnMut(Record<'_>) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut scratch = RecordScratch::new();
        let range = &self.data[range];
        let Some(format) = sequence_format(range)? else {
            return Ok(());
        };
        let mut consumed = 0usize;

        while consumed < range.len() {
            match parse_leading_record(&range[consumed..], format, true, &mut scratch) {
                ParseOutcome::Record(record, used) => {
                    visit(record)?;
                    debug_assert!(used > 0, "parser must make progress");
                    consumed += used;
                }
                // At end of input this only happens on trailing whitespace.
                ParseOutcome::Incomplete => break,
                ParseOutcome::Invalid(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Visit every record in the whole input.
    pub fn for_each_record(
        &self,
        visit: impl FnMut(Record<'_>) -> io::Result<()>,
    ) -> io::Result<()> {
        self.for_each_record_in(0..self.data.len(), visit)
    }
}

impl<R: Read> ForwardAccess<R> {
    /// Wraps a reader with the default parse window.
    pub fn new(reader: R) -> Self {
        Self::with_window(reader, DEFAULT_WINDOW_BYTES)
    }

    /// Wraps a reader with an explicit window, which the tests use to force refills.
    pub fn with_window(reader: R, window_bytes: usize) -> Self {
        Self {
            reader,
            // Allocated once and reused for the life of the run.
            window: vec![0u8; window_bytes.max(64)],
        }
    }

    /// Visit every record, in bounded memory.
    pub fn for_each_record(
        &mut self,
        visit: impl FnMut(Record<'_>) -> io::Result<()>,
    ) -> io::Result<()> {
        self.for_each_record_within(StreamStart::FirstByte, None, visit)
    }

    /// The source itself, for a driver that reads whole slabs rather than records.
    ///
    /// The parse window keeps whatever it holds, so a caller reads records or slabs from one
    /// stream, never both.
    pub fn source_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    /// Visit the records beginning in this stretch of the stream, in bounded memory.
    ///
    /// `decoded_bytes` is how much of the stream belongs to this unit of work, or `None` to read
    /// to the end. It is consulted before each record rather than after: a record straddling the
    /// end began inside and is emitted whole, while the first record starting past it is left
    /// alone. Consulting afterwards would emit that one twice.
    pub fn for_each_record_within(
        &mut self,
        start: StreamStart,
        decoded_bytes: Option<usize>,
        visit: impl FnMut(Record<'_>) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut window = ParseWindow::new(&mut self.reader, &mut self.window);

        // A whole stream announces its format with its first record; a window opening mid-record
        // cannot, so it is told. Either way a stream with no records is empty rather than
        // malformed.
        let format = match start {
            StreamStart::FirstBoundary(format) => format,
            StreamStart::FirstByte => match window.detected_format()? {
                Some(format) => format,
                None => return Ok(()),
            },
        };

        // A window opening mid-record inherits the tail of a record the unit of work before it
        // emits. Skipping it needs the boundary search rather than the parser, because the bytes
        // ahead of it are not a record.
        let skipped = match start {
            StreamStart::FirstByte => 0,
            StreamStart::FirstBoundary(_) => match window.offset_of_first_record(format)? {
                Some(boundary) => boundary,
                None => return Ok(()),
            },
        };

        window.visit_records(format, skipped, decoded_bytes, visit)
    }
}

/// The bytes one [`ForwardAccess`] run has read, and whether its source has more to give.
///
/// `filled` and `at_eof` move only in [`ParseWindow::refill`], so a step that grows the window
/// cannot miss the source ending, and the three steps of a run share one refill rather than one
/// each.
struct ParseWindow<'a, R: Read> {
    reader: &'a mut R,
    /// Allocated by the owning [`ForwardAccess`] and reused for the life of the run.
    window: &'a mut Vec<u8>,
    /// How much of `window` the source has filled.
    filled: usize,
    /// Set by the read that returns nothing, which is the only end a stream announces.
    at_eof: bool,
}

impl<'a, R: Read> ParseWindow<'a, R> {
    /// Opens on an empty window over `reader`, borrowing both rather than owning either.
    fn new(reader: &'a mut R, window: &'a mut Vec<u8>) -> Self {
        Self {
            reader,
            window,
            filled: 0,
            at_eof: false,
        }
    }

    /// The bytes read so far, which is what every step of a run looks at.
    fn filled_bytes(&self) -> &[u8] {
        &self.window[..self.filled]
    }

    /// Read once more, growing the window first when it is full, so a record larger than the
    /// whole window makes room rather than stalling the run.
    fn refill(&mut self) -> io::Result<()> {
        if self.filled == self.window.len() {
            self.window.resize(self.window.len() * 2, 0);
        }
        let read_bytes = self.reader.read(&mut self.window[self.filled..])?;
        if read_bytes == 0 {
            self.at_eof = true;
        } else {
            self.filled += read_bytes;
        }
        Ok(())
    }

    /// Move the bytes from `consumed` onwards to the front of the window.
    ///
    /// The one point a run compacts at, and the caller's only live index is `consumed`, which it
    /// resets in the same block, so no stale offset can survive a compaction.
    fn compact(&mut self, consumed: usize) {
        self.window.copy_within(consumed..self.filled, 0);
        self.filled -= consumed;
    }

    /// The format the stream's first record announces, reading on until one arrives.
    ///
    /// `None` for a source that ends with no record in it, which is empty rather than malformed.
    fn detected_format(&mut self) -> io::Result<Option<SequenceFormat>> {
        loop {
            if let Some(format) = sequence_format(self.filled_bytes())? {
                return Ok(Some(format));
            }
            if self.at_eof {
                return Ok(None);
            }
            self.refill()?;
        }
    }

    /// Offset of the first record boundary, past the partial record the window opens on.
    ///
    /// `None` when the source ends before any candidate is proven, since nothing more will
    /// arrive and an unproven candidate stays unproven.
    fn offset_of_first_record(&mut self, format: SequenceFormat) -> io::Result<Option<usize>> {
        let mut searched_through = 0usize;
        loop {
            let outcome = first_record_boundary(self.filled_bytes(), searched_through, format);
            if let RecordBoundary::At(boundary) = outcome {
                return Ok(Some(boundary));
            }
            if self.at_eof {
                return Ok(None);
            }
            // Resume where the proof ran out of bytes, not where the scan stopped. A FASTQ cycle
            // spans four lines, which on a long read is tens of kilobytes, so a candidate near
            // the end is undecided rather than rejected, and stepping over it drops every record
            // up to the next.
            searched_through = match outcome {
                RecordBoundary::Undecided(resume_from) => resume_from,
                // Every candidate was judged and rejected; only the newline a two-byte needle
                // straddles has to be reconsidered.
                _ => self.filled.saturating_sub(1),
            };
            self.refill()?;
        }
    }

    /// Parse records from `skipped` onwards and hand each to `visit`, until the budget in
    /// `decoded_bytes` runs out or the source does.
    ///
    /// `skipped` is the prefix ahead of the first record, which is where parsing begins in the
    /// window and is decoded input the budget has to count.
    fn visit_records(
        &mut self,
        format: SequenceFormat,
        skipped: usize,
        decoded_bytes: Option<usize>,
        mut visit: impl FnMut(Record<'_>) -> io::Result<()>,
    ) -> io::Result<()> {
        /// What the parse decided, carrying no borrow of the window.
        enum Outcome {
            Consumed(usize),
            NeedMore,
        }

        let mut scratch = RecordScratch::new();
        let mut consumed = skipped;
        let mut parsed_total = skipped;

        loop {
            if decoded_bytes.is_some_and(|limit| parsed_total >= limit) {
                return Ok(());
            }
            let outcome = match parse_leading_record(
                &self.filled_bytes()[consumed..],
                format,
                self.at_eof,
                &mut scratch,
            ) {
                ParseOutcome::Record(record, used) => {
                    visit(record)?;
                    debug_assert!(used > 0, "parser must make progress");
                    Outcome::Consumed(used)
                }
                ParseOutcome::Incomplete => Outcome::NeedMore,
                ParseOutcome::Invalid(error) => return Err(error),
            };
            // The immutable borrow of the window ends here, before any mutation below.

            match outcome {
                Outcome::Consumed(used) => {
                    consumed += used;
                    parsed_total += used;
                    continue;
                }
                Outcome::NeedMore => {}
            }

            if self.at_eof {
                // At end of input the parser reports `Incomplete` only for trailing whitespace;
                // a genuine truncation comes back as `Invalid`.
                return Ok(());
            }

            if consumed > 0 {
                self.compact(consumed);
                consumed = 0;
            }

            self.refill()?;
        }
    }
}

/// One opened input, in whichever shape its bytes allow. Variant names are the tier names.
///
/// The shape follows from the pair of codec and addressability rather than from the path: a
/// plain regular file can be mapped and addressed at any offset, while anything forward-only
/// — standard input, a named pipe, a file being decompressed a window at a time — is read
/// once from front to back.
///
/// Each variant owns its bytes rather than borrowing them, so the enum carries no
/// lifetime and a worker takes a view through `borrowed` while the mapping stays here.
pub enum InputFile {
    /// BYTE tier: every offset is addressable, so any byte range can go to a worker.
    Bytes(RandomAccess<memmap2::Mmap>),
    /// BLOCK tier: addressable at block granularity, since every block decodes alone.
    ///
    /// Present whatever the feature set. A build with no block codec never constructs one,
    /// because [`crate::blocks::block_access`] hands the mapping back instead, and an
    /// unconditional variant cannot drift out of step with the arms that match it.
    ///
    /// Boxed because three containers qualify and they share only their trait. Dispatch is one
    /// virtual call per block against decoding tens of kilobytes, so it does not register.
    Blocks(Box<dyn crate::blocks::BlockAccess>),
    /// STREAM tier: forward-only, so decoding stays serial.
    Stream(ForwardAccess<Box<dyn Read>>),
}

impl InputFile {
    /// Open `path`, or standard input when it is `None` or `"-"`, deciding from the bytes
    /// themselves whether they can be addressed or only streamed.
    ///
    /// Mirrors [`open_output`] and [`Rendering::for_output`], with one asymmetry worth
    /// naming: an output's shape follows from its destination, because the bytes are ours to
    /// choose; an input's shape follows from its bytes, because they are not.
    pub fn open(path: Option<&str>) -> io::Result<Self> {
        let (source, name): (Box<dyn Read>, &str) = match path {
            None | Some("-") => (Box::new(io::stdin()), "standard input"),
            Some(path) => {
                let file = std::fs::File::open(path)?;
                // One `fstat` in place of a mapping that fails on a fifo with an error
                // nobody can act on. A process substitution names a pipe, and reads as a
                // stream rather than failing.
                if file.metadata()?.file_type().is_file() {
                    return Self::from_mapping(unsafe { memmap2::Mmap::map(&file)? }, path);
                }
                (Box::new(file), path)
            }
        };
        // Only a forward-only source needs its prefix held aside and replayed.
        let sniffed = crate::codecs::PeekedReader::sniffing(source)?;
        let codec = crate::codecs::Codec::detect(sniffed.peeked());
        Ok(InputFile::Stream(ForwardAccess::new(
            codec
                .decode(sniffed)
                .map_err(|error| at_source(name, &error))?,
        )))
    }

    /// Decide a mapped regular file's tier from a prefix of the mapping itself.
    ///
    /// A mapping is random access, so the codec comes straight out of it with no prefix held
    /// aside, and a compressed one that is not block-addressable decodes from that same
    /// mapping through an [`io::Cursor`] rather than from a second pass over the file.
    fn from_mapping(mapping: memmap2::Mmap, name: &str) -> io::Result<Self> {
        let prefix_bytes = mapping.len().min(crate::codecs::Codec::PEEK_BYTES);
        let codec = crate::codecs::Codec::detect(&mapping[..prefix_bytes]);
        let mapping = match codec {
            crate::codecs::Codec::Plain => return Ok(InputFile::Bytes(RandomAccess::new(mapping))),
            // Whether an input is addressable is a second question from which container it
            // is, and only a whole mapping can answer it: a single-block xz file and a blocked
            // one share every byte of their magic. An input that declines hands the mapping
            // back and reads as a stream.
            #[cfg(any(feature = "gzip", feature = "xz", feature = "lz4"))]
            _ => match crate::blocks::block_access(mapping, codec) {
                Ok(blocks) => return Ok(InputFile::Blocks(blocks)),
                Err(mapping) => mapping,
            },
            #[cfg(not(any(feature = "gzip", feature = "xz", feature = "lz4")))]
            _ => mapping,
        };
        Ok(InputFile::Stream(ForwardAccess::new(
            codec
                .decode(io::Cursor::new(mapping))
                .map_err(|error| at_source(name, &error))?,
        )))
    }

    /// Visit every record, whichever shape the input has.
    ///
    /// The block tier reads forward from block zero, so a serial run never asks for a block
    /// table and never walks the chain to build one.
    pub fn for_each_record(
        &mut self,
        visit: impl FnMut(Record<'_>) -> io::Result<()>,
    ) -> io::Result<()> {
        match self {
            InputFile::Bytes(bytes) => bytes.for_each_record(visit),
            InputFile::Blocks(blocks) => {
                ForwardAccess::new(blocks.reader(0)).for_each_record(visit)
            }
            InputFile::Stream(stream) => stream.for_each_record(visit),
        }
    }
}

/// Prefixes an error with the input it came from, so a run over many paths says which.
///
/// Borrows rather than consumes: the message is rebuilt from the kind and the text, and the
/// original is dropped by the caller either way.
fn at_source(name: &str, error: &io::Error) -> io::Error {
    io::Error::new(error.kind(), format!("{name}: {error}"))
}

// endregion: Inputs

// region: Output

/// How records are rendered, decided by whether the destination is a terminal.
///
/// A human reading a chromosome wants it wrapped and coloured; a file or pipe wants neither,
/// so redirecting and piping produce identical bytes and nothing downstream meets an escape
/// sequence. This is the convention `ls` follows, and the reason the default is detected
/// rather than flagged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rendering {
    /// Columns per sequence line; `0` writes each sequence on one line.
    pub line_width: usize,
    /// Colour nucleotides by base.
    pub color: bool,
}

impl Rendering {
    /// Plain: one line per sequence, no colour. What a file or pipe receives.
    pub const PLAIN: Self = Self {
        line_width: 0,
        color: false,
    };

    /// Wrapped at 60 columns and coloured. What a terminal receives.
    pub const TERMINAL: Self = Self {
        line_width: 60,
        color: true,
    };

    /// [`Rendering::TERMINAL`] when `path` names standard output and that is a terminal,
    /// [`Rendering::PLAIN`] otherwise.
    pub fn for_output(path: Option<&str>) -> Self {
        let to_stdout = matches!(path, None | Some("-"));
        if to_stdout && std::io::IsTerminal::is_terminal(&io::stdout()) {
            Self::TERMINAL
        } else {
            Self::PLAIN
        }
    }
}

/// When to colour nucleotides, for a caller that will not accept the detected answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ColorWhen {
    /// Colour a terminal and nothing else, so redirecting and piping match.
    Auto,
    /// Colour whatever the destination is.
    Always,
    /// Colour nothing.
    Never,
}

/// How a tool renders what it emits, and what it says about the run afterwards.
///
/// Flattened into every record-emitting tool, so `--line-width` means one thing across the
/// suite and is documented once.
#[derive(clap::Args, Debug, Clone, Copy)]
pub struct Presentation {
    /// Columns per sequence line, 0 for one line per sequence [default: 60 on a terminal, else 0]
    #[arg(long, value_name = "N", help_heading = "Output Formats")]
    pub line_width: Option<usize>,

    /// Colour nucleotides by base
    #[arg(
        long,
        value_enum,
        value_name = "WHEN",
        default_value_t = ColorWhen::Auto,
        help_heading = "Output Formats"
    )]
    pub color: ColorWhen,

    /// Print one line about the whole run on standard error
    #[arg(long, help_heading = "Output Formats")]
    pub summary: bool,
}

impl Presentation {
    /// The rendering these flags ask for, over the one `path` would have detected.
    pub fn rendering(&self, path: Option<&str>) -> Rendering {
        let detected = Rendering::for_output(path);
        Rendering {
            line_width: self.line_width.unwrap_or(detected.line_width),
            color: match self.color {
                ColorWhen::Auto => detected.color,
                ColorWhen::Always => true,
                ColorWhen::Never => false,
            },
        }
    }
}

/// Where the records of a run are written.
///
/// A corpus is cleaned file by file and a pipeline wants one stream, so the two are named
/// apart rather than inferred from how many inputs there happen to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Destination {
    /// One stream carrying every input, which is what a pipeline reads.
    Stream(Option<String>),
    /// One file per input, basename preserved, which is what a corpus keeps.
    Directory(String),
    /// Each input rewritten where it lies, once its replacement is whole on disk.
    InPlace,
    /// Nothing is written, which is what `--dry-run` and `--quiet` both ask for.
    Discard,
}

impl Destination {
    /// Reject input aliases and duplicate outputs before opening writers.
    ///
    /// This checks the current paths; concurrent replacements can invalidate the result.
    pub fn validate_inputs(&self, inputs: &[String]) -> io::Result<()> {
        match self {
            Destination::Stream(Some(path)) if path != "-" => {
                let output = Path::new(path);
                let identity = match file_identity(output) {
                    Ok(identity) => identity,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                    Err(error) => return Err(error),
                };
                for input in inputs {
                    if input_matches_output(Path::new(input), output, identity)? {
                        return Err(output_collision(input, output));
                    }
                }
                Ok(())
            }
            Destination::Directory(directory) => self.validate_directory_inputs(directory, inputs),
            _ => Ok(()),
        }
    }

    fn validate_directory_inputs(&self, directory: &str, inputs: &[String]) -> io::Result<()> {
        let mut names = HashSet::new();
        let mut outputs = Vec::new();
        for input in inputs {
            let name = named_file(input, self)?;
            if !names.insert(name) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "--output-dir would write more than one input to '{}'",
                        name.display()
                    ),
                ));
            }
            let output = Path::new(directory).join(name);
            match file_identity(&output) {
                Ok(identity) => outputs.push((output, identity)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        drop(names);
        if outputs.is_empty() {
            return Ok(());
        }
        let mut sources = Vec::with_capacity(inputs.len());
        for input in inputs {
            let path = Path::new(input);
            sources.push((file_identity(path)?, path));
        }
        sources.sort_unstable_by_key(|(identity, _)| *identity);
        outputs.sort_unstable_by_key(|(_, identity)| *identity);
        for (index, (output, identity)) in outputs.iter().enumerate() {
            let first_source = sources.partition_point(|(source, _)| source < identity);
            for (_, source) in sources[first_source..]
                .iter()
                .take_while(|(source, _)| source == identity)
            {
                if input_matches_output(source, output, *identity)? {
                    return Err(output_collision(source.display(), output));
                }
            }
            let first_output = outputs[..index].partition_point(|(_, earlier)| earlier < identity);
            for (earlier, _) in &outputs[first_output..index] {
                if input_matches_output(earlier, output, *identity)? {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "outputs '{}' and '{}' refer to the same file",
                            earlier.display(),
                            output.display()
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// The path whose terminal-ness decides the default rendering, for [`Presentation`].
    ///
    /// Only a stream can reach a terminal, so every other destination answers as a file
    /// would, and `--color always` stays the way to override that.
    pub fn terminal_path(&self) -> Option<&str> {
        match self {
            Destination::Stream(path) => path.as_deref(),
            _ => Some(""),
        }
    }

    /// The one writer every input shares, or `None` when each input needs its own.
    pub fn shared_writer(&self, rendering: Rendering) -> io::Result<Option<RecordWriter<Sink>>> {
        match self {
            Destination::Stream(path) => Ok(Some(RecordWriter::with_rendering(
                open_output(path.as_deref())?,
                rendering,
                SequenceFormat::Fasta,
            ))),
            Destination::Discard => Ok(Some(RecordWriter::with_rendering(
                Box::new(io::sink()),
                rendering,
                SequenceFormat::Fasta,
            ))),
            Destination::Directory(_) | Destination::InPlace => Ok(None),
        }
    }

    /// A writer for `input`, or `None` when the run shares one across all inputs.
    ///
    /// The file is created before any record is read, so an input holding no records still
    /// leaves an empty output beside its siblings rather than a gap.
    pub fn writer_for(&self, input: &str, rendering: Rendering) -> io::Result<Option<PerInput>> {
        let path = match self {
            Destination::Stream(_) | Destination::Discard => return Ok(None),
            Destination::Directory(directory) => {
                Path::new(directory).join(named_file(input, self)?)
            }
            // The input's own path, not its name: an in-place run replaces the file where it
            // lies, and taking the name alone would write into the working directory instead.
            Destination::InPlace => {
                named_file(input, self)?;
                PathBuf::from(input)
            }
        };
        if matches!(self, Destination::Directory(_)) && path == Path::new(input) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("--output-dir would overwrite the input '{input}'; use --in-place"),
            ));
        }
        // An in-place run writes beside its target and renames, so a run that dies partway
        // leaves the input intact rather than half-rewritten.
        let (writing_to, swap_into) = match self {
            Destination::InPlace => (temporary_beside(&path), Some(path)),
            _ => (path, None),
        };
        Ok(Some(PerInput {
            writer: RecordWriter::with_rendering(
                open_output(writing_to.to_str())?,
                rendering,
                SequenceFormat::Fasta,
            ),
            writing_to,
            swap_into,
        }))
    }
}

#[cfg(unix)]
type FileIdentity = (u64, u64);
#[cfg(not(unix))]
type FileIdentity = u64;

#[cfg(unix)]
fn file_identity(path: &Path) -> io::Result<Option<FileIdentity>> {
    use std::os::unix::fs::MetadataExt;

    let metadata = path.metadata()?;
    Ok(Some((metadata.dev(), metadata.ino())))
}

#[cfg(not(unix))]
fn file_identity(path: &Path) -> io::Result<Option<FileIdentity>> {
    use std::hash::{Hash, Hasher};

    if !path.metadata()?.is_file() {
        return Ok(None);
    }
    let handle = same_file::Handle::from_path(path)?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    handle.hash(&mut hasher);
    Ok(Some(hasher.finish()))
}

#[cfg(unix)]
fn input_matches_output(
    input: &Path,
    _output: &Path,
    identity: Option<FileIdentity>,
) -> io::Result<bool> {
    let source = if input == Path::new("-") {
        let handle = same_file::Handle::stdin()?;
        Some((handle.dev(), handle.ino()))
    } else {
        file_identity(input)?
    };
    Ok(source == identity)
}

#[cfg(not(unix))]
fn input_matches_output(
    input: &Path,
    output: &Path,
    identity: Option<FileIdentity>,
) -> io::Result<bool> {
    if input == Path::new("-") {
        return Ok(same_file::Handle::stdin()? == same_file::Handle::from_path(output)?);
    }
    let source = file_identity(input)?;
    if source != identity {
        return Ok(false);
    }
    match source {
        Some(_) => same_file::is_same_file(input, output),
        None => Ok(input.canonicalize()? == output.canonicalize()?),
    }
}

fn output_collision(input: impl std::fmt::Display, output: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
            "output '{}' would overwrite input '{input}'",
            output.display()
        ),
    )
}

/// The name `input` contributes to its own output, which standard input does not have.
fn named_file<'a>(input: &'a str, destination: &Destination) -> io::Result<&'a std::ffi::OsStr> {
    let flag = match destination {
        Destination::InPlace => "--in-place",
        _ => "--output-dir",
    };
    match input {
        "-" => None,
        named => Path::new(named).file_name(),
    }
    .ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{flag} needs a named input, but '{input}' has no file name"),
        )
    })
}

/// A sibling of `path` that no concurrent run of this tool will also pick.
fn temporary_beside(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".fasterfasta-{}", std::process::id()));
    path.with_file_name(name)
}

/// One input's own writer, plus whatever has to happen once its records are all written.
pub struct PerInput {
    writer: RecordWriter<Sink>,
    writing_to: PathBuf,
    /// Set for an in-place run: the input this temporary replaces.
    swap_into: Option<PathBuf>,
}

impl PerInput {
    /// The writer itself, which the caller hands records to.
    pub fn writer_mut(&mut self) -> &mut RecordWriter<Sink> {
        &mut self.writer
    }

    /// Flush, and for an in-place run put the finished file where the input was.
    ///
    /// A failed rename leaves the temporary behind deliberately: the records are in it, and
    /// deleting them to tidy up would be the one unrecoverable move.
    pub fn finish(mut self) -> io::Result<()> {
        self.writer.flush()?;
        drop(self.writer);
        match self.swap_into {
            Some(target) => std::fs::rename(&self.writing_to, &target),
            None => Ok(()),
        }
    }
}

/// The writer every record-emitting tool ends up holding, whatever its destination.
pub type Sink = Box<dyn Write>;

/// Foreground colour for one nucleotide, or `None` to leave it uncoloured.
fn base_color(base: u8) -> Option<&'static [u8]> {
    match base.to_ascii_uppercase() {
        b'A' => Some(b"\x1b[32m"),
        b'C' => Some(b"\x1b[34m"),
        b'G' => Some(b"\x1b[33m"),
        b'T' | b'U' => Some(b"\x1b[31m"),
        b'N' => Some(b"\x1b[90m"),
        _ => None,
    }
}

/// Append `sequence`, wrapped per `rendering`, followed by a line terminator.
///
/// Split by case so the plain form — what every file and pipe receives — is one copy per
/// line rather than a decision per byte.
fn sequence_into(target: &mut Vec<u8>, sequence: &[u8], rendering: Rendering) {
    match (rendering.line_width, rendering.color) {
        (0, false) => {
            target.extend_from_slice(sequence);
            target.push(b'\n');
        }
        (width, false) => {
            for line in sequence.chunks(width) {
                target.extend_from_slice(line);
                target.push(b'\n');
            }
        }
        (width, true) => colored_sequence_into(target, sequence, width),
    }
}

/// Append `sequence` wrapped at `width` and coloured by base.
///
/// Reached only when the destination is a terminal, so its cost never falls on a pipe.
/// Colour is emitted per run rather than per byte, so a homopolymer costs one escape.
fn colored_sequence_into(target: &mut Vec<u8>, sequence: &[u8], width: usize) {
    const RESET: &[u8] = b"\x1b[0m";
    let width = if width == 0 { usize::MAX } else { width };

    for line in sequence.chunks(width) {
        let mut active: Option<&'static [u8]> = None;
        for &base in line {
            let wanted = base_color(base);
            if wanted != active {
                if active.is_some() {
                    target.extend_from_slice(RESET);
                }
                if let Some(escape) = wanted {
                    target.extend_from_slice(escape);
                }
                active = wanted;
            }
            target.push(base);
        }
        if active.is_some() {
            target.extend_from_slice(RESET);
        }
        target.push(b'\n');
    }
}

/// Append one formatted record to `target`. Quality is emitted only for FASTQ output.
fn format_into(
    target: &mut Vec<u8>,
    format: SequenceFormat,
    rendering: Rendering,
    header_body: &[u8],
    sequence: &[u8],
    quality: &[u8],
) {
    if rendering.color {
        target.extend_from_slice(b"\x1b[1;36m");
    }
    target.push(format.sigil());
    target.extend_from_slice(header_body);
    if rendering.color {
        target.extend_from_slice(b"\x1b[0m");
    }
    target.push(b'\n');

    sequence_into(target, sequence, rendering);

    if format == SequenceFormat::Fastq {
        target.extend_from_slice(b"+\n");
        // Quality wraps with the sequence so the two stay line-aligned, and is never
        // coloured, since its bytes are scores rather than bases.
        sequence_into(
            target,
            quality,
            Rendering {
                color: false,
                ..rendering
            },
        );
    }
}

/// Renders records to a sink, in whichever format the output carries.
///
/// A record written field by field would be four or five small writes, and a pipe charges for
/// bytes. Staging into one reusable buffer makes it one call, and costs one allocation
/// for the whole run.
pub struct RecordWriter<W: io::Write> {
    writer: W,
    staging: Vec<u8>,
    format: SequenceFormat,
    /// Format settled by [`RecordWriter::adopt`], so a later disagreement is caught.
    adopted: Option<SequenceFormat>,
    rendering: Rendering,
}

impl<W: io::Write> RecordWriter<W> {
    /// Wraps a writer, emitting plain records in `format` until told otherwise.
    pub fn new(writer: W, format: SequenceFormat) -> Self {
        Self {
            writer,
            staging: Vec::with_capacity(4 << 10),
            format,
            adopted: None,
            rendering: Rendering::PLAIN,
        }
    }

    /// Wraps a writer that renders per `rendering`.
    pub fn with_rendering(writer: W, rendering: Rendering, format: SequenceFormat) -> Self {
        Self {
            rendering,
            ..Self::new(writer, format)
        }
    }

    /// Mirror `format` onto the output, settling on the first record's and refusing a later
    /// change, since one stream cannot carry both.
    ///
    /// Idempotent, so a tool can call it per record instead of needing a hook that fires
    /// before the records do.
    pub fn adopt(&mut self, format: SequenceFormat) -> io::Result<()> {
        match self.adopted {
            Some(first) if first != format => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("input is {format:?} but an earlier record was {first:?}; one output stream cannot carry both"),
            )),
            Some(_) => Ok(()),
            None => {
                self.adopted = Some(format);
                self.format = format;
                Ok(())
            }
        }
    }

    /// Write `record` verbatim, re-sigiling the header to the output format.
    pub fn write_record(&mut self, record: Record<'_>) -> io::Result<()> {
        self.write_parts(
            record.header_without_sigil(),
            record.sequence,
            record.quality,
        )
    }

    /// Write a record from parts, where `header_body` excludes the sigil.
    ///
    /// Quality is emitted only for FASTQ output, and a FASTA source converted to FASTQ
    /// would have none — callers wanting that must supply it.
    pub fn write_parts(
        &mut self,
        header_body: &[u8],
        sequence: &[u8],
        quality: &[u8],
    ) -> io::Result<()> {
        self.staging.clear();
        format_into(
            &mut self.staging,
            self.format,
            self.rendering,
            header_body,
            sequence,
            quality,
        );
        self.writer.write_all(&self.staging)
    }

    /// Format a record into `target`, replacing its contents and keeping its capacity.
    ///
    /// For a tool that must retain records: a record borrows the input, so surviving past
    /// the next one means owning the bytes.
    pub fn render_into(&self, record: Record<'_>, target: &mut Vec<u8>) {
        target.clear();
        self.append_into(record, target);
    }

    /// Format a record onto the end of `target`, keeping what is already there.
    ///
    /// [`RecordWriter::render_into`] replaces the target because its caller keeps one buffer
    /// per retained record; a caller accumulating a whole range into one buffer needs the
    /// opposite, and would otherwise render into a scratch and copy it a second time.
    pub fn append_into(&self, record: Record<'_>, target: &mut Vec<u8>) {
        format_into(
            target,
            self.format,
            self.rendering,
            record.header_without_sigil(),
            record.sequence,
            record.quality,
        );
    }

    /// Write bytes produced by [`RecordWriter::render_into`], which are already formatted.
    pub fn write_rendered(&mut self, rendered: &[u8]) -> io::Result<()> {
        self.writer.write_all(rendered)
    }

    /// Flushes the underlying writer; the staging buffer never holds a partial record.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    /// Consumes the writer, which the tests use to recover a `Vec<u8>` sink.
    pub fn into_inner(self) -> W {
        self.writer
    }

    /// The sink itself, for a caller draining a buffer between units of work.
    pub fn inner_mut(&mut self) -> &mut W {
        &mut self.writer
    }
}

/// Writer for `path`, or standard output when it is `None` or `"-"`.
pub fn open_output(path: Option<&str>) -> io::Result<Box<dyn io::Write>> {
    match path {
        None | Some("-") => Ok(Box::new(io::BufWriter::new(io::stdout()))),
        Some(path) => Ok(Box::new(io::BufWriter::new(std::fs::File::create(path)?))),
    }
}

/// What a run produced, which is what its exit code reports.
///
/// Named apart from failure because "ran and produced nothing" is a different recovery from
/// "could not run": the first wants a wider filter, the second wants a fixed command line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// At least one record, row, or file was written.
    Produced,
    /// The run completed and produced nothing.
    Empty,
}

impl RunOutcome {
    /// [`RunOutcome::Produced`] when `count` is non-zero, which is how every tool decides.
    pub fn of(count: usize) -> Self {
        match count {
            0 => Self::Empty,
            _ => Self::Produced,
        }
    }
}

/// The epilogue every tool's `main` shares.
///
/// A broken pipe is a normal end rather than a failure, because `tool | head` is a routine
/// way to use these, and by then the records that mattered are already downstream.
pub fn finish_or_exit(context: &str, result: io::Result<RunOutcome>) -> ! {
    match result {
        Ok(RunOutcome::Produced) => std::process::exit(0),
        Ok(RunOutcome::Empty) => std::process::exit(1),
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => std::process::exit(0),
        Err(error) => {
            eprintln!("{context}: {error}");
            std::process::exit(2)
        }
    }
}

impl RecordWriter<Vec<u8>> {
    /// Hand the buffered records to `sink` and empty the buffer for the next unit of work.
    ///
    /// A record never straddles two calls, so what leaves is always whole records. Buffering
    /// into a `Vec` and draining in turn is what makes a parallel run emit the bytes a serial
    /// one would, whichever worker filled which buffer.
    pub fn drain_into(&mut self, sink: &mut impl io::Write) -> io::Result<()> {
        sink.write_all(&self.writer)?;
        self.writer.clear();
        Ok(())
    }
}

// endregion: Output

#[cfg(test)]
mod tests {
    use super::*;

    use crate::fixtures;

    /// Re-emits every record verbatim, so a driver run can be compared byte for byte
    /// against another driver run over the same input.
    #[derive(Debug, Default)]
    struct Collector {
        output: Vec<u8>,
        records: usize,
        finished: bool,
    }

    impl Collector {
        fn push(&mut self, record: Record<'_>) -> io::Result<()> {
            self.output.extend_from_slice(record.header);
            self.output.push(b'\n');
            self.output.extend_from_slice(record.sequence);
            self.output.push(b'\n');
            if !record.quality.is_empty() {
                self.output.extend_from_slice(b"+\n");
                self.output.extend_from_slice(record.quality);
                self.output.push(b'\n');
            }
            self.records += 1;
            Ok(())
        }

        fn finish(&mut self) -> io::Result<()> {
            self.finished = true;
            Ok(())
        }
    }

    fn drive_bytes(data: &[u8]) -> io::Result<Collector> {
        let mut sink = Collector::default();
        RandomAccess::new(data).for_each_record(|record| sink.push(record))?;
        sink.finish()?;
        Ok(sink)
    }

    fn drive_stream(data: &[u8], limit: usize, window: usize) -> io::Result<Collector> {
        let mut sink = Collector::default();
        let reader = fixtures::ShortReader::new(io::Cursor::new(data), limit);
        ForwardAccess::with_window(reader, window).for_each_record(|record| sink.push(record))?;
        sink.finish()?;
        Ok(sink)
    }

    /// The property the whole design rests on: however the bytes arrive, the records come
    /// out identical.
    ///
    /// Every FASTQ shape the table holds, so a read wider than the window, a CRLF file, a file
    /// with no final newline, and a quality line opening with '@' all take the same path.
    #[test]
    fn stream_matches_slice_for_fastq() {
        for shape in fixtures::ALL
            .into_iter()
            .filter(|shape| shape.format == SequenceFormat::Fastq)
        {
            let count = fixtures::count_within(shape, 4 << 10);
            let data = fixtures::records(shape, count);
            let expected = drive_bytes(&data).unwrap();
            assert_eq!(expected.records, count, "{shape:?}");

            for limit in [1, 3, 7, 13, 64, 4096] {
                for window in [64, 128, 1024, DEFAULT_WINDOW_BYTES] {
                    let actual = drive_stream(&data, limit, window).unwrap_or_else(|error| {
                        panic!("{shape:?}, limit {limit}, window {window}: {error}")
                    });
                    assert_eq!(
                        actual.output, expected.output,
                        "{shape:?}, limit {limit}, window {window}: output diverged"
                    );
                    assert_eq!(actual.records, expected.records, "{shape:?}");
                }
            }
        }
    }

    #[test]
    fn stream_matches_slice_for_fasta() {
        for shape in fixtures::ALL
            .into_iter()
            .filter(|shape| shape.format == SequenceFormat::Fasta)
        {
            let count = fixtures::count_within(shape, 4 << 10);
            let data = fixtures::records(shape, count);
            let expected = drive_bytes(&data).unwrap();
            assert_eq!(expected.records, count, "{shape:?}");

            for limit in [1, 5, 11, 64, 4096] {
                for window in [64, 256, DEFAULT_WINDOW_BYTES] {
                    let actual = drive_stream(&data, limit, window).unwrap_or_else(|error| {
                        panic!("{shape:?}, limit {limit}, window {window}: {error}")
                    });
                    assert_eq!(
                        actual.output, expected.output,
                        "{shape:?}, limit {limit}, window {window}: output diverged"
                    );
                }
            }
        }
    }

    /// A record larger than the initial window must grow it, not stall or truncate.
    #[test]
    fn record_larger_than_window_grows_it() {
        let shape = fixtures::FOUR_KILOBASE_FASTQ;
        let data = fixtures::records(shape, 1);

        let sink = drive_stream(&data, 17, 64).unwrap();
        assert_eq!(sink.records, 1);
        assert!(sink
            .output
            .windows(shape.read_bases)
            .any(|window| window.iter().all(|&byte| byte == b'A')));
    }

    #[test]
    fn truncated_input_is_an_error() {
        // Quality is one byte short of the sequence and the stream ends.
        let data = b"@seq1\nACGT\n+\nIII";
        let result = drive_stream(data, 4, 64);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn empty_input_yields_no_records() {
        let sink = drive_stream(b"", 8, 64).unwrap();
        assert_eq!(sink.records, 0);
        assert!(sink.finished);
    }

    #[test]
    fn trailing_whitespace_is_not_an_error() {
        let data = b"@seq1\nACGT\n+\nIIII\n\n\n  \n";
        let sink = drive_stream(data, 3, 64).unwrap();
        assert_eq!(sink.records, 1);
    }

    #[test]
    fn finish_runs_even_with_no_records() {
        let mut sink = Collector::default();
        RandomAccess::new(&b""[..])
            .for_each_record(|record| sink.push(record))
            .unwrap();
        sink.finish().unwrap();
        assert!(sink.finished);
    }

    #[test]
    fn format_detection_feeds_the_driver() {
        let data = fixtures::records(fixtures::WRAPPED_FASTA, 3);
        let format = sequence_format(&data).unwrap().unwrap();
        assert_eq!(format, SequenceFormat::Fasta);
        assert_eq!(drive_bytes(&data).unwrap().records, 3);
    }

    /// A worker reads through a view while the bytes stay with their owner.
    #[test]
    fn a_borrowed_view_yields_the_same_records() {
        let data = fixtures::records(fixtures::WRAPPED_FASTA, 12);
        let owned = RandomAccess::new(data.clone());
        let mut through_view = Vec::new();
        owned
            .borrowed()
            .for_each_record(|record| {
                through_view.push(record.header.to_vec());
                Ok(())
            })
            .unwrap();
        assert_eq!(through_view.len(), 12);
        assert_eq!(through_view, headers_in(&data));
    }

    /// One record as owned bytes — header, sequence, quality — so a comparison can outlive
    /// the window the record borrowed from.
    type OwnedRecord = (Vec<u8>, Vec<u8>, Vec<u8>);

    /// Every record a window over `data` yields.
    fn records_within(
        data: &[u8],
        window_bytes: usize,
        start: StreamStart,
        decoded_bytes: Option<usize>,
    ) -> io::Result<Vec<OwnedRecord>> {
        let mut records = Vec::new();
        ForwardAccess::with_window(data, window_bytes).for_each_record_within(
            start,
            decoded_bytes,
            |record| {
                records.push((
                    record.header.to_vec(),
                    record.sequence.to_vec(),
                    record.quality.to_vec(),
                ));
                Ok(())
            },
        )?;
        Ok(records)
    }

    /// Two windows split anywhere tile the input: the head keeps the record straddling the
    /// split, and the tail resumes at the first boundary after it.
    ///
    /// This is what a run of blocks does at every block but the first, and it needs no codec
    /// to reach — a plain buffer cut in two drives the same two entry points. Leaving it to
    /// the compressed tiers puts the whole of [`StreamStart::FirstBoundary`] behind a feature
    /// flag, so a build with no codec never runs the code that carried the bug.
    #[test]
    fn a_split_buffer_tiles_the_same_records() {
        for shape in fixtures::ALL {
            let data = fixtures::records(shape, fixtures::count_within(shape, 4 << 10));
            let format = shape.format;

            for window_bytes in [64usize, 256, 4096] {
                let whole = records_within(&data, window_bytes, StreamStart::FirstByte, None)
                    .unwrap_or_else(|error| {
                        panic!("{shape:?}, {window_bytes}-byte window: {error}")
                    });
                assert!(!whole.is_empty(), "{shape:?}: the fixture holds no records");

                for divisor in [7usize, 3, 2] {
                    let split = data.len() / divisor;
                    if split == 0 || split >= data.len() {
                        continue;
                    }

                    // The head owns every record beginning before the split, including the one
                    // straddling it, which is why the budget is consulted before a parse.
                    let head =
                        records_within(&data, window_bytes, StreamStart::FirstByte, Some(split))
                            .unwrap();
                    // The tail opens mid-record, so it is told the format and skips forward to
                    // the first boundary rather than parsing the fragment it inherits.
                    let tail = records_within(
                        &data[split..],
                        window_bytes,
                        StreamStart::FirstBoundary(format),
                        None,
                    )
                    .unwrap();

                    let mut tiled = head;
                    tiled.extend(tail);
                    assert_eq!(
                        tiled,
                        whole,
                        "{shape:?}, {window_bytes}-byte window, split at {split} of {}",
                        data.len()
                    );
                }
            }
        }
    }

    /// `decoded_bytes` counts the prefix a window skips, because both are measured from the
    /// front of the window and those bytes are decoded input.
    ///
    /// [`a_split_buffer_tiles_the_same_records`] never pairs the two: its head opens on a record
    /// and its tail states no `decoded_bytes`. Three windows that each open mid-record and each
    /// state one do, and a run counting only from its first record would read past its end and
    /// re-emit records the run before it owns.
    #[test]
    fn decoded_bytes_counts_the_prefix_a_window_skips() {
        for shape in fixtures::ALL {
            let data = fixtures::records(shape, fixtures::count_within(shape, 4 << 10));
            let format = shape.format;
            // Sevenths, so a window opens well inside a record rather than on the boundary a
            // half or a third of a ragged fixture tends to land near.
            let starts = [1usize, 3, 5].map(|seventh| data.len() * seventh / 7);

            for window_bytes in [64usize, 256, 4096] {
                let whole = records_within(
                    &data[starts[0]..],
                    window_bytes,
                    StreamStart::FirstBoundary(format),
                    None,
                )
                .unwrap_or_else(|error| panic!("{shape:?}, {window_bytes}-byte window: {error}"));
                assert!(!whole.is_empty(), "{shape:?}: the fixture holds no records");

                let mut tiled = Vec::new();
                for (index, &start) in starts.iter().enumerate() {
                    let end = starts.get(index + 1).copied().unwrap_or(data.len());
                    tiled.extend(
                        records_within(
                            &data[start..],
                            window_bytes,
                            StreamStart::FirstBoundary(format),
                            Some(end - start),
                        )
                        .unwrap(),
                    );
                }
                assert_eq!(
                    tiled,
                    whole,
                    "{shape:?}, {window_bytes}-byte window, windows at {starts:?} of {}",
                    data.len()
                );
            }
        }
    }

    /// Headers of every record in a buffer, as the tier-independent answer to compare against.
    fn headers_in(data: &[u8]) -> Vec<Vec<u8>> {
        let mut headers = Vec::new();
        RandomAccess::new(data)
            .for_each_record(|record| {
                headers.push(record.header.to_vec());
                Ok(())
            })
            .unwrap();
        headers
    }

    /// Writes `bytes` under `directory` and names the file.
    fn written(directory: &tempfile::TempDir, name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let path = directory.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    }

    /// Header and sequence of every record [`InputFile::open`] yields for `path`.
    fn records_at(path: &std::path::Path) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut records = Vec::new();
        InputFile::open(Some(path.to_str().unwrap()))?.for_each_record(|record| {
            records.push((record.header.to_vec(), record.sequence.to_vec()));
            Ok(())
        })?;
        Ok(records)
    }

    #[test]
    fn a_plain_file_opens_as_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let plain = fixtures::records(fixtures::WRAPPED_FASTA, 8);
        let path = written(&directory, "plain.fasta", &plain);
        assert!(matches!(
            InputFile::open(Some(path.to_str().unwrap())).unwrap(),
            InputFile::Bytes(_)
        ));
        assert_eq!(records_at(&path).unwrap().len(), 8);
    }

    #[test]
    fn a_missing_path_is_an_error() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("absent.fasta");
        match InputFile::open(Some(path.to_str().unwrap())) {
            Ok(_) => panic!("a path that names nothing must not open"),
            Err(error) => assert_eq!(error.kind(), io::ErrorKind::NotFound),
        }
    }

    /// A named pipe is not a regular file, so the `fstat` must route it to the stream tier
    /// rather than let `mmap` fail on it. This is what a process substitution looks like
    /// from the inside.
    #[cfg(unix)]
    #[test]
    fn a_named_pipe_opens_as_a_stream() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pipe.fasta");
        let created = std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .unwrap();
        assert!(created.success(), "mkfifo failed");

        let plain = fixtures::records(fixtures::WRAPPED_FASTA, 4);
        let expected = headers_in(&plain);
        // Both ends block until the other arrives, so the writer runs alongside the open.
        let written_path = path.clone();
        let writer = std::thread::spawn(move || std::fs::write(&written_path, &plain).unwrap());

        let mut input = InputFile::open(Some(path.to_str().unwrap())).unwrap();
        assert!(matches!(input, InputFile::Stream(_)));
        let mut headers = Vec::new();
        input
            .for_each_record(|record| {
                headers.push(record.header.to_vec());
                Ok(())
            })
            .unwrap();
        writer.join().unwrap();
        assert_eq!(headers, expected);
    }

    /// BGZF on a regular file stays block-addressable, which is why the tier exists.
    #[cfg(feature = "gzip")]
    #[test]
    fn a_bgzip_file_opens_as_blocks() {
        let directory = tempfile::tempdir().unwrap();
        let plain = fixtures::records(fixtures::WRAPPED_FASTA, 8);
        let path = written(
            &directory,
            "blocked.fasta.gz",
            &fixtures::bgzf_bytes(&plain, 512),
        );
        assert!(matches!(
            InputFile::open(Some(path.to_str().unwrap())).unwrap(),
            InputFile::Blocks(_)
        ));
        assert_eq!(records_at(&path).unwrap().len(), 8);
    }

    /// Plain gzip has no block layout, so it reads forward only however it is stored.
    #[cfg(feature = "gzip")]
    #[test]
    fn a_plain_gzip_file_opens_as_stream() {
        let directory = tempfile::tempdir().unwrap();
        let plain = fixtures::records(fixtures::WRAPPED_FASTA, 8);
        let path = written(
            &directory,
            "streamed.fasta.gz",
            &fixtures::gzip_bytes(&plain),
        );
        assert!(matches!(
            InputFile::open(Some(path.to_str().unwrap())).unwrap(),
            InputFile::Stream(_)
        ));
        assert_eq!(records_at(&path).unwrap().len(), 8);
    }

    /// The tier is an implementation detail, so all three shapes of the same file must yield
    /// exactly the same records — at every point of the input space.
    #[cfg(feature = "gzip")]
    #[test]
    fn every_input_shape_yields_the_same_records() {
        for shape in fixtures::ALL {
            let count = fixtures::count_within(shape, 8 << 10);
            let plain = fixtures::records(shape, count);
            let directory = tempfile::tempdir().unwrap();
            let expected = records_at(&written(&directory, "plain", &plain)).unwrap();
            assert_eq!(expected.len(), count, "{shape:?}");

            let blocked = written(&directory, "blocked.gz", &fixtures::bgzf_bytes(&plain, 512));
            assert_eq!(
                records_at(&blocked).unwrap(),
                expected,
                "{shape:?}: blocked diverged"
            );

            let streamed = written(&directory, "streamed.gz", &fixtures::gzip_bytes(&plain));
            assert_eq!(
                records_at(&streamed).unwrap(),
                expected,
                "{shape:?}: streamed diverged"
            );
        }
    }

    /// One record written under `rendering`, as the bytes a sink would receive.
    fn rendered(rendering: Rendering, sequence: &[u8], quality: &[u8]) -> Vec<u8> {
        let mut writer = RecordWriter::with_rendering(Vec::new(), rendering, SequenceFormat::Fastq);
        writer
            .write_parts(b"read0 sample", sequence, quality)
            .unwrap();
        writer.into_inner()
    }

    /// Every escape sequence dropped, leaving the bytes a pipe would have received.
    fn without_escapes(rendered: &[u8]) -> Vec<u8> {
        let mut plain = Vec::with_capacity(rendered.len());
        let mut rest = rendered;
        while let Some(escape) = rest.iter().position(|&byte| byte == 0x1b) {
            plain.extend_from_slice(&rest[..escape]);
            let end = rest[escape..]
                .iter()
                .position(|&byte| byte == b'm')
                .expect("an escape sequence ends in 'm'");
            rest = &rest[escape + end + 1..];
        }
        plain.extend_from_slice(rest);
        plain
    }

    /// A terminal gets the sequence wrapped and coloured, and colour is purely additive.
    ///
    /// The escape-stripped bytes must be the wrapped-but-uncoloured rendering exactly, so a
    /// colour run can neither swallow a base nor emit one twice, and quality carries no colour
    /// at all because its bytes are scores rather than bases.
    #[test]
    fn a_terminal_gets_a_wrapped_and_coloured_record() {
        assert_eq!(
            Rendering::TERMINAL,
            Rendering {
                line_width: 60,
                color: true
            }
        );
        assert_eq!(
            Rendering::PLAIN,
            Rendering {
                line_width: 0,
                color: false
            }
        );

        // 155 bases, so the wrap lands three lines deep with a short last one, and every
        // colour the table knows appears alongside a base it leaves uncoloured.
        let sequence: Vec<u8> = b"ACGTNX".iter().copied().cycle().take(155).collect();
        let quality = vec![b'I'; 155];

        let terminal = rendered(Rendering::TERMINAL, &sequence, &quality);
        let uncoloured = rendered(
            Rendering {
                line_width: 60,
                color: false,
            },
            &sequence,
            &quality,
        );
        assert_eq!(
            without_escapes(&terminal),
            uncoloured,
            "colour must add escapes and change nothing else"
        );

        // Green, blue, yellow, red and grey, one per base the table names.
        for escape in [
            &b"\x1b[32m"[..],
            b"\x1b[34m",
            b"\x1b[33m",
            b"\x1b[31m",
            b"\x1b[90m",
        ] {
            assert!(
                terminal.windows(escape.len()).any(|found| found == escape),
                "missing the colour {escape:?}"
            );
        }

        // Header, three sequence lines, the separator, three quality lines.
        let lines: Vec<&[u8]> = uncoloured.split(|&byte| byte == b'\n').collect();
        assert_eq!(lines.len(), 9, "trailing empty piece included");
        assert_eq!(lines[0], b"@read0 sample");
        assert_eq!(
            [lines[1].len(), lines[2].len(), lines[3].len()],
            [60, 60, 35]
        );
        assert_eq!(lines[4], b"+");
        assert_eq!(
            [lines[5].len(), lines[6].len(), lines[7].len()],
            [60, 60, 35]
        );

        // Quality is scores, so it wraps with the sequence and is never coloured.
        let separator = terminal
            .windows(3)
            .position(|found| found == b"\n+\n")
            .expect("a FASTQ record carries a separator");
        assert!(
            !terminal[separator..].contains(&0x1b),
            "quality must carry no colour"
        );

        // A pipe gets one line per sequence and no escape at all.
        let plain = rendered(Rendering::PLAIN, &sequence, &quality);
        assert!(!plain.contains(&0x1b));
        assert_eq!(plain.split(|&byte| byte == b'\n').count(), 5);
    }

    /// An in-place run writes beside its target and only swaps once the file is whole.
    #[test]
    fn in_place_swaps_only_after_the_records_are_written() {
        let directory = tempfile::tempdir().unwrap();
        let input = written(&directory, "reads.fa", b">a\nACGT\n");

        let mut per_input = Destination::InPlace
            .writer_for(input.to_str().unwrap(), Rendering::PLAIN)
            .ok()
            .flatten()
            .expect("an in-place run writes one file per input");
        per_input
            .writer_mut()
            .write_rendered(b">b\nTGCA\n")
            .unwrap();

        // Still the original bytes: the replacement exists, but beside the input, not over it.
        assert_eq!(std::fs::read(&input).unwrap(), b">a\nACGT\n");
        per_input.finish().unwrap();
        assert_eq!(std::fs::read(&input).unwrap(), b">b\nTGCA\n");

        // And the temporary is gone, having become the input.
        let siblings = std::fs::read_dir(directory.path()).unwrap().count();
        assert_eq!(siblings, 1, "the temporary outlived the rename");
    }

    /// One output stream carries one format, so the first record settles it and a later
    /// disagreement is an error rather than a silently mixed file.
    #[test]
    fn adopting_a_second_format_is_refused() {
        let mut writer = RecordWriter::new(Vec::new(), SequenceFormat::Fasta);
        writer.adopt(SequenceFormat::Fastq).unwrap();
        // Idempotent, so a tool calls it per record rather than needing a hook before them.
        writer.adopt(SequenceFormat::Fastq).unwrap();

        let error = writer.adopt(SequenceFormat::Fasta).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let message = error.to_string();
        assert!(
            message.contains("Fasta") && message.contains("Fastq"),
            "{message}"
        );

        // The refusal leaves the settled format in place rather than half-applying the second.
        writer.write_parts(b"read0", b"ACGT", b"IIII").unwrap();
        assert_eq!(writer.into_inner(), b"@read0\nACGT\n+\nIIII\n");
    }

    #[test]
    fn an_output_directory_keeps_each_input_name() {
        let directory = tempfile::tempdir().unwrap();
        let destination = Destination::Directory(directory.path().display().to_string());
        assert!(destination
            .writer_for("/corpus/reads.fastq", Rendering::PLAIN)
            .is_ok());
        assert!(directory.path().join("reads.fastq").exists());
    }

    #[test]
    fn an_output_directory_will_not_overwrite_its_input() {
        let destination = Destination::Directory(".".to_string());
        assert!(destination
            .writer_for("./reads.fastq", Rendering::PLAIN)
            .is_err());
    }

    #[test]
    fn validation_rejects_input_aliases_without_writing() {
        let directory = tempfile::tempdir().unwrap();
        let input = written(&directory, "reads.fa", b">a\nACGT\n");
        let hard_link = directory.path().join("hard.fa");
        std::fs::hard_link(&input, &hard_link).unwrap();
        let aliases = [
            input.clone(),
            directory.path().join(".").join("reads.fa"),
            hard_link,
        ];
        let inputs = vec![input.display().to_string()];
        for alias in aliases {
            let destination = Destination::Stream(Some(alias.display().to_string()));
            assert_eq!(
                destination.validate_inputs(&inputs).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(std::fs::read(&input).unwrap(), b">a\nACGT\n");
        }
        #[cfg(unix)]
        {
            let alias = directory.path().join("symbolic.fa");
            std::os::unix::fs::symlink(&input, &alias).unwrap();
            assert!(Destination::Stream(Some(alias.display().to_string()))
                .validate_inputs(&inputs)
                .is_err());
        }
    }

    #[test]
    fn validation_rejects_duplicate_output_names_and_later_input_aliases() {
        let first_directory = tempfile::tempdir().unwrap();
        let second_directory = tempfile::tempdir().unwrap();
        let first = written(&first_directory, "reads.fa", b">a\nACGT\n");
        let second = written(&second_directory, "reads.fa", b">b\nTGCA\n");
        let output = tempfile::tempdir().unwrap();
        let destination = Destination::Directory(output.path().display().to_string());
        assert!(destination
            .validate_inputs(&[first.display().to_string(), second.display().to_string()])
            .is_err());
        assert_eq!(std::fs::read_dir(output.path()).unwrap().count(), 0);

        let later = written(&second_directory, "later.fa", b">c\nAAAA\n");
        std::fs::hard_link(&later, output.path().join("reads.fa")).unwrap();
        assert!(destination
            .validate_inputs(&[first.display().to_string(), later.display().to_string()])
            .is_err());
        assert_eq!(std::fs::read(&later).unwrap(), b">c\nAAAA\n");

        let output = tempfile::tempdir().unwrap();
        let existing = written(&output, "reads.fa", b"keep");
        std::fs::hard_link(&existing, output.path().join("later.fa")).unwrap();
        let destination = Destination::Directory(output.path().display().to_string());
        assert!(destination
            .validate_inputs(&[first.display().to_string(), later.display().to_string()])
            .is_err());
        assert_eq!(std::fs::read(existing).unwrap(), b"keep");
    }

    #[cfg(unix)]
    #[test]
    fn validation_accepts_a_write_only_output() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let input = written(&directory, "reads.fa", b">a\nACGT\n");
        let output = written(&directory, "output.fa", b"old");
        std::fs::set_permissions(&output, std::fs::Permissions::from_mode(0o200)).unwrap();
        let destination = Destination::Stream(Some(output.display().to_string()));
        destination
            .validate_inputs(&[input.display().to_string()])
            .unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&output)
            .unwrap();
    }

    #[test]
    fn validation_accepts_distinct_outputs_and_explicit_in_place() {
        let directory = tempfile::tempdir().unwrap();
        let input = written(&directory, "reads.fa", b">a\nACGT\n");
        let inputs = vec![input.display().to_string()];
        for destination in [
            Destination::Stream(None),
            Destination::Stream(Some("-".to_string())),
            Destination::Discard,
            Destination::InPlace,
            Destination::Stream(Some(
                directory.path().join("result.fa").display().to_string(),
            )),
        ] {
            destination.validate_inputs(&inputs).unwrap();
        }
    }

    #[test]
    fn a_per_input_destination_refuses_standard_input() {
        // Nothing is opened: standard input has no name to give an output file.
        for destination in [
            Destination::Directory("elsewhere".to_string()),
            Destination::InPlace,
        ] {
            assert!(destination.writer_for("-", Rendering::PLAIN).is_err());
        }
    }

    #[test]
    fn a_shared_destination_needs_no_per_input_writer() {
        for destination in [Destination::Stream(None), Destination::Discard] {
            assert!(destination
                .writer_for("reads.fq", Rendering::PLAIN)
                .unwrap()
                .is_none());
            assert!(destination
                .shared_writer(Rendering::PLAIN)
                .unwrap()
                .is_some());
        }
    }

    #[test]
    fn only_a_stream_can_reach_a_terminal() {
        assert_eq!(
            Destination::Stream(Some("out.fa".to_string())).terminal_path(),
            Some("out.fa")
        );
        assert_eq!(Destination::Stream(None).terminal_path(), None);
        // Everything else answers as a named file would, so nothing writes an escape sequence
        // into a directory of outputs or over an input.
        for destination in [
            Destination::Directory("out".to_string()),
            Destination::InPlace,
            Destination::Discard,
        ] {
            assert_eq!(destination.terminal_path(), Some(""));
        }
    }

    #[test]
    fn an_explicit_rendering_overrides_the_detected_one() {
        let presentation = Presentation {
            line_width: Some(60),
            color: ColorWhen::Always,
            summary: false,
        };
        let rendering = presentation.rendering(Some("out.fa"));
        assert_eq!(rendering.line_width, 60);
        assert!(rendering.color);
    }

    #[test]
    fn a_discarding_run_is_never_coloured_by_detection() {
        let presentation = Presentation {
            line_width: None,
            color: ColorWhen::Auto,
            summary: false,
        };
        let rendering = presentation.rendering(Destination::Discard.terminal_path());
        assert_eq!(rendering, Rendering::PLAIN);
    }
}
