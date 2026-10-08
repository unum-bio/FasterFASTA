//! Sequence statistics and quality control.
//!
//! One row per input, so a globbed dataset is summarized file by file, with `--total` for a
//! combined row. Every column is an associative reduction, so rows merge and the totals do
//! not depend on the order the inputs were given.
//!
//! __Memory__: length counts, one entry per distinct sequence length. Reads share a handful
//! of lengths; assemblies have at most one per contig.
//! __Streaming__: yes, for files and pipes alike.
//!
//! # Examples
//!
//! ```bash
//! fastq-stats reads.fastq                       # one row
//! fastq-stats sample_*.fastq --total            # a row each, plus a combined row
//! fastq-stats reads.fastq --histogram           # length distribution as well
//! cat reads.fastq | fastq-stats
//! ```
//!
//! Exit: 0 summarized an input, 1 ran and summarized none, 2 could not run.

use std::collections::HashMap;
use std::io::{self, Write};

use clap::Parser;

use fasterfasta::files::{finish_or_exit, open_output, InputFile, RunOutcome};
use fasterfasta::records::{phred33_sum, Record, SequenceFormat};
use fasterfasta::scheduling::{for_each_record_in_input, Parallelism, Workers};

/// Totals over a set of records.
///
/// Every field is associative and commutative, so [`Summary::merge`] combines two summaries
/// and the result is independent of the order the inputs arrived in.
#[derive(Default)]
struct Summary {
    format: Option<SequenceFormat>,
    records: u64,
    bases: u64,
    minimum_length: usize,
    maximum_length: usize,
    quality_sum: u64,
    gc_count: u64,
    n_count: u64,
    /// Exact count per distinct sequence length, which is what lets the histogram pick its
    /// bins from the data rather than from whichever record happened to arrive first.
    length_counts: HashMap<usize, u64>,
}

impl Summary {
    fn new() -> Self {
        Self {
            minimum_length: usize::MAX,
            ..Default::default()
        }
    }

    fn add(&mut self, record: &Record<'_>) {
        self.format.get_or_insert(record.format());
        let length = record.sequence.len();
        self.records += 1;
        self.bases += length as u64;
        self.minimum_length = self.minimum_length.min(length);
        self.maximum_length = self.maximum_length.max(length);
        *self.length_counts.entry(length).or_insert(0) += 1;

        self.quality_sum += phred33_sum(record.quality);
        // Base composition stays scalar: counting members of a byteset has no primitive yet.
        for &base in record.sequence {
            match base {
                b'G' | b'C' | b'g' | b'c' => self.gc_count += 1,
                b'N' | b'n' => self.n_count += 1,
                _ => {}
            }
        }
    }

    fn merge(&mut self, other: &Summary) {
        // A total row over mixed formats has no single format to report.
        self.format = match (self.format, other.format) {
            (None, found) | (found, None) => found,
            (first, second) if first == second => first,
            _ => None,
        };
        self.records += other.records;
        self.bases += other.bases;
        self.minimum_length = self.minimum_length.min(other.minimum_length);
        self.maximum_length = self.maximum_length.max(other.maximum_length);
        self.quality_sum += other.quality_sum;
        self.gc_count += other.gc_count;
        self.n_count += other.n_count;
        for (length, count) in &other.length_counts {
            *self.length_counts.entry(*length).or_insert(0) += count;
        }
    }

    fn minimum(&self) -> usize {
        if self.records == 0 {
            0
        } else {
            self.minimum_length
        }
    }

    /// Flooring the divisor at one is exact here: no records means no bases either.
    fn mean_length(&self) -> f64 {
        self.bases as f64 / self.records.max(1) as f64
    }

    /// Mean Phred score, or `None` for FASTA, which carries no quality to average.
    fn mean_quality(&self) -> Option<f64> {
        match self.format {
            Some(SequenceFormat::Fastq) if self.bases > 0 => {
                Some(self.quality_sum as f64 / self.bases as f64)
            }
            _ => None,
        }
    }

    /// Flooring the divisor at one is exact here: no bases means nothing was counted either.
    fn percentage(&self, count: u64) -> f64 {
        100.0 * count as f64 / self.bases.max(1) as f64
    }

    fn format_name(&self) -> &'static str {
        match self.format {
            Some(SequenceFormat::Fasta) => "FASTA",
            Some(SequenceFormat::Fastq) => "FASTQ",
            None => "-",
        }
    }
}

const HISTOGRAM_BINS: usize = 10;

/// Render the length distribution, with bins spanning the observed range.
///
/// Bin width comes from the smallest and largest length actually seen, so the same records
/// produce the same histogram whatever order they arrived in, and a file of uniform-length
/// reads reports one honest bar rather than one meaningless one.
fn write_histogram(output: &mut impl Write, summary: &Summary) -> io::Result<()> {
    if summary.records == 0 {
        return Ok(());
    }

    let low = summary.minimum();
    let high = summary.maximum_length;
    let span = high.saturating_sub(low) + 1;
    let width = span.div_ceil(HISTOGRAM_BINS).max(1);

    let mut bins = [0u64; HISTOGRAM_BINS];
    for (length, count) in &summary.length_counts {
        bins[((length - low) / width).min(HISTOGRAM_BINS - 1)] += count;
    }
    let peak = bins.iter().copied().max().unwrap_or(1).max(1);

    writeln!(output)?;
    writeln!(output, "Length Distribution")?;
    for (index, &count) in bins.iter().enumerate().filter(|(_, &count)| count > 0) {
        let start = low + index * width;
        let end = (start + width - 1).min(high);
        let bar = "█".repeat((40 * count / peak) as usize);
        writeln!(output, "{start:>9}-{end:<9} {count:>10}  {bar}")?;
    }
    Ok(())
}

fn write_header(output: &mut impl Write) -> io::Result<()> {
    writeln!(
        output,
        "{:<28} {:<7} {:>10} {:>14} {:>9} {:>11} {:>9} {:>8} {:>7} {:>7}",
        "file",
        "format",
        "num_seqs",
        "sum_len",
        "min_len",
        "avg_len",
        "max_len",
        "mean_q",
        "gc_pct",
        "n_pct"
    )
}

fn write_row(
    output: &mut impl Write,
    format: Format,
    label: &str,
    summary: &Summary,
) -> io::Result<()> {
    let quality = match summary.mean_quality() {
        Some(mean) => format!("{mean:.2}"),
        None => "-".to_string(),
    };
    match format {
        Format::Table => writeln!(
            output,
            "{:<28} {:<7} {:>10} {:>14} {:>9} {:>11.2} {:>9} {:>8} {:>7.2} {:>7.2}",
            label,
            summary.format_name(),
            summary.records,
            summary.bases,
            summary.minimum(),
            summary.mean_length(),
            summary.maximum_length,
            quality,
            summary.percentage(summary.gc_count),
            summary.percentage(summary.n_count),
        ),
        Format::Plain => writeln!(
            output,
            "{}\t{}\t{}\t{}\t{}\t{:.2}\t{}\t{}\t{:.2}\t{:.2}",
            label,
            summary.format_name(),
            summary.records,
            summary.bases,
            summary.minimum(),
            summary.mean_length(),
            summary.maximum_length,
            quality,
            summary.percentage(summary.gc_count),
            summary.percentage(summary.n_count),
        ),
        // Quality is null rather than "-" for a FASTA input, so a consumer branches on the
        // type instead of on a sentinel it has to know about.
        Format::Json => {
            output.write_all(b"{\"file\":")?;
            write_json_string(output, label)?;
            writeln!(
                output,
                concat!(
                    r#","format":"{}","num_seqs":{},"sum_len":{},"min_len":{},"#,
                    r#""avg_len":{:.2},"max_len":{},"mean_q":{},"gc_pct":{:.2},"n_pct":{:.2}}}"#
                ),
                summary.format_name(),
                summary.records,
                summary.bases,
                summary.minimum(),
                summary.mean_length(),
                summary.maximum_length,
                match summary.mean_quality() {
                    Some(mean) => format!("{mean:.2}"),
                    None => "null".to_string(),
                },
                summary.percentage(summary.gc_count),
                summary.percentage(summary.n_count),
            )
        }
    }
}

/// Write a JSON string, leaving Unicode intact and escaping every ASCII control byte.
fn write_json_string(output: &mut impl Write, value: &str) -> io::Result<()> {
    output.write_all(b"\"")?;
    let mut copied = 0;
    for (index, byte) in value.bytes().enumerate() {
        let escaped: &[u8] = match byte {
            b'"' => b"\\\"",
            b'\\' => b"\\\\",
            0..=0x1f => {
                output.write_all(&value.as_bytes()[copied..index])?;
                write!(output, "\\u{byte:04x}")?;
                copied = index + 1;
                continue;
            }
            _ => continue,
        };
        output.write_all(&value.as_bytes()[copied..index])?;
        output.write_all(escaped)?;
        copied = index + 1;
    }
    output.write_all(&value.as_bytes()[copied..])?;
    output.write_all(b"\"")
}

/// Summarize one input, which may hold no records at all.
///
/// Every column is associative and commutative, so the workers fold into one summary each and
/// the merge at the end gives the answer a single pass would have.
fn summarize(path: &str, workers: &mut Workers) -> io::Result<Summary> {
    let mut states = workers.states(Summary::new);

    let path = if path == "-" { None } else { Some(path) };
    let mut input = InputFile::open(path)?;
    for_each_record_in_input(
        &mut input,
        workers,
        &mut states,
        |state: &mut Summary, record: Record<'_>| {
            state.add(&record);
            Ok(())
        },
        |_| Ok(()),
    )?;

    let mut summary = Summary::new();
    for state in &states {
        summary.merge(state);
    }
    Ok(summary)
}

/// Summarize sequence files
#[derive(Parser)]
#[command(name = "fastq-stats")]
#[command(version, about = "Summarize sequence files, one row per input")]
#[command(
    long_about = "Summarize FASTA or FASTQ inputs, one row each.\nPass --total for a combined row, and --histogram for the length distribution.\nEvery column is an associative reduction, so totals do not depend on input order."
)]
struct Args {
    /// Input files, FASTA or FASTQ; '-' or omitted reads standard input
    #[arg(default_value = "-")]
    inputs: Vec<String>,

    /// Append a row combining every input
    #[arg(long, help_heading = "Columns")]
    total: bool,

    /// Show the length distribution across all inputs
    #[arg(long, help_heading = "Columns")]
    histogram: bool,

    /// How rows are rendered
    #[arg(
        long,
        value_enum,
        value_name = "FORMAT",
        default_value_t = Format::Table,
        help_heading = "Output Formats"
    )]
    format: Format,

    /// Output file; '-' or omitted writes standard output
    #[arg(
        long,
        value_name = "FILE",
        conflicts_with_all = ["dry_run", "quiet"],
        help_heading = "Output"
    )]
    output: Option<String>,

    /// Report what would be written, on standard error, without writing it
    #[arg(long, conflicts_with = "quiet", help_heading = "Output")]
    dry_run: bool,

    /// Suppress all output; the exit code carries the answer
    #[arg(long, help_heading = "Output")]
    quiet: bool,

    /// Print one line about the whole run on standard error
    #[arg(long, help_heading = "Output Formats")]
    summary: bool,

    #[command(flatten)]
    parallelism: Parallelism,
}

/// How one summary row is rendered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Format {
    /// Aligned columns under a header, which is what a terminal reads best.
    Table,
    /// One tab-separated row per input, with no header, for `cut` and `awk`.
    Plain,
    /// JSON Lines, one object per input, for a pipeline that parses rather than reads.
    Json,
}

fn run(args: &Args) -> io::Result<RunOutcome> {
    let mut output: Box<dyn Write> = match args.quiet || args.dry_run {
        true => Box::new(io::sink()),
        false => open_output(args.output.as_deref())?,
    };
    let mut workers = args.parallelism.unordered()?;
    let mut combined = Summary::new();
    let mut rows = 0usize;

    if args.format == Format::Table {
        write_header(&mut output)?;
    }
    for path in &args.inputs {
        let summary = summarize(path, &mut workers)?;
        let label = if path == "-" {
            "(stdin)"
        } else {
            path.as_str()
        };
        write_row(&mut output, args.format, label, &summary)?;
        combined.merge(&summary);
        rows += 1;
    }

    if args.total && args.inputs.len() > 1 {
        write_row(&mut output, args.format, "(total)", &combined)?;
    }
    if args.histogram {
        write_histogram(&mut output, &combined)?;
    }
    output.flush()?;

    if args.dry_run {
        eprintln!(
            "would summarize {rows} inputs, {} records, {} bases; nothing was written",
            combined.records, combined.bases
        );
    } else if args.summary {
        eprintln!(
            "summarized {rows} inputs, {} records, {} bases",
            combined.records, combined.bases
        );
    }
    // Records rather than rows, so an empty input reports empty the way every other tool does.
    Ok(RunOutcome::of(combined.records as usize))
}

fn main() {
    finish_or_exit("Error computing statistics", run(&Args::parse()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use fasterfasta::scheduling;

    fn summary_of(data: &[u8], format: SequenceFormat) -> Summary {
        let mut summary = Summary::new();
        summary.format = Some(format);
        scheduling::for_each_record_in_bytes(data, &mut summary, |state: &mut Summary, record| {
            state.add(&record);
            Ok(())
        })
        .unwrap();
        summary
    }

    fn row_of(data: &[u8], format: SequenceFormat) -> String {
        let summary = summary_of(data, format);
        let mut out = Vec::new();
        write_row(&mut out, Format::Table, "x", &summary).unwrap();
        String::from_utf8(out).unwrap()
    }

    fn histogram_of(data: &[u8], format: SequenceFormat) -> String {
        let summary = summary_of(data, format);
        let mut out = Vec::new();
        write_histogram(&mut out, &summary).unwrap();
        String::from_utf8(out).unwrap()
    }

    fn record(name: &str, length: usize) -> Vec<u8> {
        format!(">{name}\n{}\n", "A".repeat(length)).into_bytes()
    }

    #[test]
    fn counts_records_and_bases() {
        let data = b"@a\nACGT\n+\nIIII\n@b\nACGTACGT\n+\nIIIIIIII\n";
        let summary = summary_of(data, SequenceFormat::Fastq);
        assert_eq!(summary.records, 2);
        assert_eq!(summary.bases, 12);
        assert_eq!(summary.minimum(), 4);
        assert_eq!(summary.maximum_length, 8);
        assert_eq!(summary.mean_length(), 6.0);
    }

    #[test]
    fn reports_composition() {
        let summary = summary_of(b"@a\nGGCCATNA\n+\nIIIIIIII\n", SequenceFormat::Fastq);
        assert_eq!(summary.percentage(summary.gc_count), 50.0);
        assert_eq!(summary.percentage(summary.n_count), 12.5);
    }

    /// FASTA carries no quality, so reporting a mean of zero would look measured.
    #[test]
    fn fasta_reports_no_quality_rather_than_zero() {
        assert_eq!(
            summary_of(b">a\nACGT\n", SequenceFormat::Fasta).mean_quality(),
            None
        );
        let row = row_of(b">a\nACGT\n", SequenceFormat::Fasta);
        assert!(row.contains(" - "), "{row}");
    }

    #[test]
    fn fastq_reports_mean_quality() {
        // 'I' is Q40 and '!' is Q0, so the mean over one of each is 20.
        let summary = summary_of(b"@a\nAC\n+\nI!\n", SequenceFormat::Fastq);
        assert_eq!(summary.mean_quality(), Some(20.0));
    }

    /// The histogram is derived from the observed range, so the same records in any order
    /// give the same bins. Calibrating on the first record made this fail.
    #[test]
    fn histogram_is_independent_of_record_order() {
        let forward = [record("a", 10), record("b", 1000)].concat();
        let backward = [record("b", 1000), record("a", 10)].concat();
        assert_eq!(
            histogram_of(&forward, SequenceFormat::Fasta),
            histogram_of(&backward, SequenceFormat::Fasta)
        );
    }

    /// Reads that are all one length must land in a bar whose range actually contains them.
    #[test]
    fn uniform_lengths_are_reported_honestly() {
        let mut data = Vec::new();
        for index in 0..5 {
            data.extend_from_slice(&record(&format!("r{index}"), 150));
        }
        let histogram = histogram_of(&data, SequenceFormat::Fasta);
        assert!(histogram.contains("150-150"), "{histogram}");
    }

    /// Every long record must fall inside a bin whose label contains it.
    #[test]
    fn the_widest_record_lands_in_a_bin_that_names_it() {
        let data = [record("small", 10), record("large", 1000)].concat();
        let histogram = histogram_of(&data, SequenceFormat::Fasta);
        assert!(histogram.contains("-1000"), "{histogram}");
    }

    /// Merging two summaries must equal summarizing their concatenation.
    #[test]
    fn merging_matches_a_single_pass() {
        let head = [record("a", 10), record("b", 20)].concat();
        let tail = [record("c", 30), record("d", 40)].concat();
        let whole = [head.clone(), tail.clone()].concat();

        let mut merged = summary_of(&head, SequenceFormat::Fasta);
        merged.merge(&summary_of(&tail, SequenceFormat::Fasta));
        let single = summary_of(&whole, SequenceFormat::Fasta);

        assert_eq!(merged.records, single.records);
        assert_eq!(merged.bases, single.bases);
        assert_eq!(merged.minimum(), single.minimum());
        assert_eq!(merged.maximum_length, single.maximum_length);
        assert_eq!(merged.length_counts, single.length_counts);
    }

    /// A total row over a FASTA and a FASTQ input has no single format to claim.
    #[test]
    fn merging_mixed_formats_reports_no_format() {
        let mut merged = summary_of(b">a\nACGT\n", SequenceFormat::Fasta);
        merged.merge(&summary_of(b"@b\nACGT\n+\nIIII\n", SequenceFormat::Fastq));
        assert_eq!(merged.format_name(), "-");
    }

    #[test]
    fn empty_input_reports_zeroes() {
        let summary = Summary::new();
        let row = row_of(b"", SequenceFormat::Fasta);
        assert_eq!(summary.minimum(), 0);
        assert!(row.contains(" 0 "), "{row}");
        assert!(histogram_of(b"", SequenceFormat::Fasta).is_empty());
    }

    /// Every flag is spelled out, so a call site says what it does and nothing is remembered
    /// by letter. `-h` and `-V` are clap's own and stay.
    #[test]
    fn json_strings_escape_controls_and_preserve_unicode() {
        let mut output = Vec::new();
        write_json_string(
            &mut output,
            "quote\" slash\\ newline\n tab\t \u{1} \u{85} é",
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            "\"quote\\\" slash\\\\ newline\\u000a tab\\u0009 \\u0001 \u{85} é\""
        );
        for byte in 0..=0x1f {
            let mut output = Vec::new();
            write_json_string(&mut output, &char::from(byte).to_string()).unwrap();
            assert_eq!(
                String::from_utf8(output).unwrap(),
                format!("\"\\u{byte:04x}\"")
            );
        }
    }

    #[test]
    fn declares_no_short_flags() {
        // Built first, because `-h` and `-V` are only added then and they are the exemption.
        let mut command = Args::command();
        command.build();
        assert!(command
            .get_arguments()
            .all(|argument| argument.get_short().is_none()
                || matches!(argument.get_short(), Some('h') | Some('V'))));
    }

    /// Pins the surface, so adding, renaming, or reordering a flag is a deliberate edit here
    /// rather than a drift nobody reviews.
    #[test]
    fn declares_the_expected_flags() {
        let mut command = Args::command();
        command.build();
        let longs: Vec<_> = command
            .get_arguments()
            .filter_map(|argument| argument.get_long())
            .collect();
        assert_eq!(
            longs,
            [
                "total",
                "histogram",
                "format",
                "output",
                "dry-run",
                "quiet",
                "summary",
                "threads",
                "help",
                "version"
            ]
        );
    }
}
