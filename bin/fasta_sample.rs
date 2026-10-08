//! Random subsampling of sequence records.
//!
//! Two modes with different memory profiles. `--fraction` decides each record independently
//! and holds nothing; `--count` keeps a uniform reservoir of exactly N records.
//!
//! __Memory__: constant for `--fraction`, N records for `--count`.
//! __Streaming__: yes, for files and pipes alike.
//!
//! # Examples
//!
//! ```bash
//! fasta-sample sequences.fasta --count 1000 --output sample.fasta
//! fasta-sample reads.fastq --fraction 0.1 --output tenth.fastq
//! cat reads.fastq | fasta-sample --fraction 0.01 --seed 42 > sample.fastq
//! ```
//!
//! Exit: 0 sampled a record, 1 ran and sampled none, 2 could not run.

use std::collections::BinaryHeap;
use std::io::{self, Write};

use clap::Parser;

use fasterfasta::files::{
    finish_or_exit, open_output, Destination, Presentation, RecordWriter, Rendering, RunOutcome,
};
use fasterfasta::records::{digest, Record, RecordKey, SequenceFormat};
use fasterfasta::scheduling::{for_each_record_in_inputs, Parallelism};

/// Whether a record with this key survives a `--fraction` pass.
fn keeps(key: u64, probability: f64) -> bool {
    if probability <= 0.0 {
        return false;
    }
    if probability >= 1.0 {
        return true;
    }
    (key as f64) < probability * (u64::MAX as f64)
}

#[derive(Debug, Clone, Copy)]
enum Mode {
    /// Keep each record independently with the given probability.
    Fraction(f64),
    /// Keep a uniform sample of exactly this many records.
    Count(usize),
}

/// One record kept by `Mode::Count`, ordered by digest so the largest is discarded first.
///
/// `arrival` is carried so the kept records can be emitted in input order rather than in
/// whatever order the heap holds them.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Kept {
    /// Leads the struct so the derived ordering is by key, which is what the heap needs.
    key: u64,
    /// A globally monotone index. Each state counts from zero over the byte range it took,
    /// and [`Sampler::absorb`] rebases that count onto the records already absorbed, so two
    /// ranges cannot both claim arrival zero and interleave in the sort.
    arrival: u64,
    rendered: Vec<u8>,
}

struct Sampler<W: Write> {
    writer: RecordWriter<W>,
    mode: Mode,
    seed: u64,
    /// Records seen so far, used only to stamp arrival order.
    seen: u64,
    /// Records held by `Mode::Count`; always empty under `Mode::Fraction`.
    kept: BinaryHeap<Kept>,
    /// Records written out, counted where they leave rather than where they are chosen.
    retained: u64,
}

impl<W: Write> Sampler<W> {
    fn new(writer: W, mode: Mode, seed: u64, rendering: Rendering) -> Self {
        Self {
            writer: RecordWriter::with_rendering(writer, rendering, SequenceFormat::Fasta),
            mode,
            seed,
            seen: 0,
            // Grown as records are kept, so `--count` cannot ask for an allocation up front.
            kept: BinaryHeap::new(),
            retained: 0,
        }
    }

    fn push(&mut self, record: Record<'_>) -> io::Result<()> {
        self.writer.adopt(record.format())?;
        // A record's fate is decided by its identity alone, so it does not depend on the
        // record's position, on how the inputs were ordered, or on how many workers ran.
        let key = digest(&record, RecordKey::Identifier, self.seed);

        match self.mode {
            Mode::Fraction(probability) => {
                if keeps(key, probability) {
                    self.writer.write_record(record)?;
                    self.retained += 1;
                }
            }
            Mode::Count(count) => {
                // The `count` smallest keys form a uniform sample, and a max-heap discards
                // the current largest, so this merges across inputs by construction.
                let worst = self.kept.peek().map_or(u64::MAX, |top| top.key);
                if self.kept.len() < count || key < worst {
                    let mut rendered = Vec::new();
                    self.writer.render_into(record, &mut rendered);
                    self.kept.push(Kept {
                        key,
                        arrival: self.seen,
                        rendered,
                    });
                    if self.kept.len() > count {
                        self.kept.pop();
                    }
                }
            }
        }
        self.seen += 1;
        Ok(())
    }

    /// Fold one state's work into this sampler, which the driver calls in input order.
    ///
    /// The reservoir merges by construction, since the `count` smallest keys of the union are
    /// the `count` smallest of the merged heaps.
    fn absorb(&mut self, state: &mut Sampler<Vec<u8>>) -> io::Result<()> {
        // A `--fraction` state writes as it goes, so its buffer is already rendered records.
        let buffered = state.writer.inner_mut();
        self.writer.write_rendered(buffered)?;
        buffered.clear();
        self.retained += state.retained;
        state.retained = 0;

        let capacity = match self.mode {
            Mode::Count(count) => count,
            Mode::Fraction(_) => 0,
        };
        for mut entry in state.kept.drain() {
            entry.arrival += self.seen;
            self.kept.push(entry);
            if self.kept.len() > capacity {
                self.kept.pop();
            }
        }
        self.seen += state.seen;
        state.seen = 0;
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        // Selection is order-independent, but the output is not: records leave in the order
        // they arrived, so a coordinate-sorted input stays sorted.
        let mut kept: Vec<Kept> = std::mem::take(&mut self.kept).into_vec();
        kept.sort_unstable_by_key(|entry| entry.arrival);
        for entry in kept {
            self.writer.write_rendered(&entry.rendered)?;
            self.retained += 1;
        }
        self.writer.flush()
    }
}

/// Randomly subsample sequence records
#[derive(Parser)]
#[command(name = "fasta-sample")]
#[command(version, about = "Randomly subsample sequence records")]
#[command(
    long_about = "Subsample FASTA or FASTQ records.\n--fraction decides each record independently in constant memory.\n--count keeps a uniform reservoir of exactly N records.\nPass --seed to reproduce a sample exactly."
)]
// Exactly one way of deciding what to keep.
#[command(group(clap::ArgGroup::new("selector").required(true)))]
struct Args {
    /// Input files; '-' or omitted reads standard input
    #[arg(default_value = "-")]
    inputs: Vec<String>,

    /// Keep exactly this many records, sampled uniformly
    #[arg(long, value_name = "N", group = "selector", help_heading = "Sampling")]
    count: Option<usize>,

    /// Keep each record with this probability, between 0.0 and 1.0
    #[arg(long, value_name = "P", group = "selector", help_heading = "Sampling")]
    fraction: Option<f64>,

    /// Seed for reproducible sampling
    #[arg(
        long,
        value_name = "N",
        default_value_t = 42,
        help_heading = "Sampling"
    )]
    seed: u64,

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

    #[command(flatten)]
    presentation: Presentation,

    #[command(flatten)]
    parallelism: Parallelism,
}

/// The mode the flags ask for, or why they cannot be honoured.
///
/// Reported as an error rather than printed here, so a bad flag leaves through the same
/// epilogue every other tool uses and carries the same prefix.
fn mode_of(count: Option<usize>, fraction: Option<f64>) -> io::Result<Mode> {
    let rejected = |reason: String| io::Error::new(io::ErrorKind::InvalidInput, reason);
    match (count, fraction) {
        (Some(0), _) => Err(rejected("--count must be at least 1".to_string())),
        (Some(count), _) => Ok(Mode::Count(count)),
        (_, Some(fraction)) if (0.0..=1.0).contains(&fraction) => Ok(Mode::Fraction(fraction)),
        (_, Some(fraction)) => Err(rejected(format!(
            "--fraction must be between 0.0 and 1.0, got {fraction}"
        ))),
        // Unreachable through the command line, where the `selector` group requires one.
        (None, None) => Err(rejected("pass either --count or --fraction".to_string())),
    }
}

fn run(args: &Args) -> io::Result<RunOutcome> {
    let mode = mode_of(args.count, args.fraction)?;

    // A reservoir spans every input, so there is no per-input file to write and the only
    // two destinations are one stream or none at all.
    let destination = match args.quiet || args.dry_run {
        true => Destination::Discard,
        false => Destination::Stream(args.output.clone()),
    };
    let rendering = args.presentation.rendering(destination.terminal_path());
    destination.validate_inputs(&args.inputs)?;
    let output: Box<dyn Write> = match &destination {
        Destination::Stream(path) => open_output(path.as_deref())?,
        _ => Box::new(io::sink()),
    };
    let mut workers = args.parallelism.ordered()?;
    // One sampler per unit of work in flight, each folded into `combined` in input order.
    let mut states = workers.states(|| Sampler::new(Vec::new(), mode, args.seed, rendering));
    let mut combined = Sampler::new(output, mode, args.seed, rendering);

    for_each_record_in_inputs(
        &args.inputs,
        &mut workers,
        &mut states,
        Sampler::push,
        |state| combined.absorb(state),
    )?;
    combined.finish()?;
    if args.dry_run {
        eprintln!(
            "would retain {} of {} records; nothing was written",
            combined.retained, combined.seen
        );
    } else if args.presentation.summary {
        eprintln!(
            "examined {} records, retained {}",
            combined.seen, combined.retained
        );
    }
    Ok(RunOutcome::of(combined.retained as usize))
}

fn main() {
    finish_or_exit("Error sampling records", run(&Args::parse()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use fasterfasta::scheduling::for_each_record_in_bytes;

    fn sample(data: &[u8], mode: Mode, seed: u64) -> String {
        let mut sampler = Sampler::new(Vec::new(), mode, seed, Rendering::PLAIN);
        for_each_record_in_bytes(data, &mut sampler, Sampler::push).unwrap();
        sampler.finish().unwrap();
        String::from_utf8(sampler.writer.into_inner()).unwrap()
    }

    fn fasta(records: usize) -> Vec<u8> {
        fasta_range(0, records)
    }

    fn fasta_range(first: usize, last: usize) -> Vec<u8> {
        let mut data = Vec::new();
        for index in first..last {
            data.extend_from_slice(format!(">seq{index}\nACGT\n").as_bytes());
        }
        data
    }

    #[test]
    fn count_keeps_exactly_that_many() {
        let data = fasta(100);
        let out = sample(&data, Mode::Count(10), 42);
        assert_eq!(out.matches('>').count(), 10);
    }

    #[test]
    fn count_larger_than_input_keeps_everything() {
        let data = fasta(5);
        let out = sample(&data, Mode::Count(50), 42);
        assert_eq!(out.matches('>').count(), 5);
    }

    #[test]
    fn same_seed_reproduces_the_sample() {
        let data = fasta(100);
        let first = sample(&data, Mode::Count(10), 7);
        let second = sample(&data, Mode::Count(10), 7);
        assert_eq!(first, second);
    }

    #[test]
    fn different_seeds_give_different_samples() {
        let data = fasta(1000);
        let first = sample(&data, Mode::Count(10), 1);
        let second = sample(&data, Mode::Count(10), 2);
        assert_ne!(first, second);
    }

    #[test]
    fn fraction_zero_keeps_nothing_and_one_keeps_all() {
        let data = fasta(50);
        assert_eq!(sample(&data, Mode::Fraction(0.0), 42), "");
        assert_eq!(
            sample(&data, Mode::Fraction(1.0), 42).matches('>').count(),
            50
        );
    }

    #[test]
    fn fraction_lands_near_the_requested_share() {
        let data = fasta(10_000);
        let kept = sample(&data, Mode::Fraction(0.1), 42).matches('>').count();
        assert!((900..1100).contains(&kept), "kept {kept} of 10000 at p=0.1");
    }

    /// Quality must survive the reservoir round trip alongside the sequence.
    #[test]
    fn fastq_records_keep_their_quality() {
        let data = b"@read0\nACGT\n+\nIIII\n@read1\nTGCA\n+\nHHHH\n";
        let out = sample(data, Mode::Count(2), 42);
        assert!(out.contains("IIII"), "{out}");
        assert!(out.contains("HHHH"), "{out}");
    }

    /// The point of a seed: the same records selected however the input is divided.
    /// A position-driven RNG could not hold this, because every record's draw depended on
    /// how many records preceded it.
    #[test]
    fn selection_survives_splitting_the_input() {
        let head = fasta_range(0, 100);
        let tail = fasta_range(100, 200);
        let whole = [head.clone(), tail.clone()].concat();

        let from_whole = sample(&whole, Mode::Fraction(0.2), 7);
        let mut from_halves = sample(&head, Mode::Fraction(0.2), 7);
        from_halves.push_str(&sample(&tail, Mode::Fraction(0.2), 7));

        assert_eq!(from_halves, from_whole);
    }

    /// Paired mates carry the same identifier, so both files select the same read pairs
    /// with no coordination beyond the shared seed.
    #[test]
    fn paired_mates_select_matching_pairs() {
        let mut first = Vec::new();
        let mut second = Vec::new();
        for index in 0..200 {
            first.extend_from_slice(format!("@read{index}/1\nACGT\n+\nIIII\n").as_bytes());
            second.extend_from_slice(format!("@read{index}/2\nTGCA\n+\nHHHH\n").as_bytes());
        }

        let strip = |text: String, suffix: &str| -> Vec<String> {
            text.lines()
                .filter(|line| line.starts_with('@'))
                .map(|line| line.trim_end_matches(suffix).to_string())
                .collect()
        };

        let kept_first = strip(sample(&first, Mode::Fraction(0.25), 3), "/1");
        let kept_second = strip(sample(&second, Mode::Fraction(0.25), 3), "/2");

        assert!(!kept_first.is_empty(), "sampled nothing");
        assert_eq!(kept_first, kept_second);
    }

    /// Two byte ranges each stamp arrival from zero, so absorbing has to rebase them or the
    /// sort in `finish` interleaves the halves.
    #[test]
    fn absorbing_two_states_keeps_input_order() {
        let head = fasta_range(0, 100);
        let tail = fasta_range(100, 200);
        let whole = [head.clone(), tail.clone()].concat();

        let mut combined = Sampler::new(Vec::new(), Mode::Count(20), 7, Rendering::PLAIN);
        for part in [&head, &tail] {
            let mut state = Sampler::new(Vec::new(), Mode::Count(20), 7, Rendering::PLAIN);
            for_each_record_in_bytes(part, &mut state, Sampler::push).unwrap();
            combined.absorb(&mut state).unwrap();
        }
        combined.finish().unwrap();

        let from_states = String::from_utf8(combined.writer.into_inner()).unwrap();
        assert_eq!(from_states, sample(&whole, Mode::Count(20), 7));
    }

    /// Selection is order-independent, but output is not: records leave as they arrived.
    #[test]
    fn output_preserves_input_order() {
        let data = fasta(100);
        let out = sample(&data, Mode::Count(10), 42);
        let order: Vec<usize> = out
            .lines()
            .filter(|line| line.starts_with('>'))
            .map(|line| line.trim_start_matches(">seq").parse().unwrap())
            .collect();
        let mut sorted = order.clone();
        sorted.sort_unstable();
        assert_eq!(order, sorted, "reservoir order leaked into the output");
    }

    /// A huge count must not ask for the allocation up front.
    #[test]
    fn absurd_count_does_not_preallocate() {
        let data = fasta(5);
        let out = sample(&data, Mode::Count(1_000_000_000), 42);
        assert_eq!(out.matches('>').count(), 5);
    }

    #[test]
    fn empty_input_produces_nothing() {
        assert_eq!(sample(b"", Mode::Count(10), 42), "");
        assert_eq!(sample(b"", Mode::Fraction(0.5), 42), "");
    }

    /// Both modes write from a different place, so both are counted here.
    #[test]
    fn counts_are_tracked() {
        let data = fasta(100);

        let mut reservoir = Sampler::new(Vec::new(), Mode::Count(10), 42, Rendering::PLAIN);
        for_each_record_in_bytes(&data, &mut reservoir, Sampler::push).unwrap();
        reservoir.finish().unwrap();
        assert_eq!(reservoir.seen, 100);
        assert_eq!(reservoir.retained, 10);

        let mut fraction = Sampler::new(Vec::new(), Mode::Fraction(1.0), 42, Rendering::PLAIN);
        for_each_record_in_bytes(&data, &mut fraction, Sampler::push).unwrap();
        fraction.finish().unwrap();
        assert_eq!(fraction.seen, 100);
        assert_eq!(fraction.retained, 100);
    }

    #[test]
    fn output_alias_is_rejected_before_truncating_the_input() {
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("reads.fa");
        std::fs::write(&input, b">a\nACGT\n").unwrap();
        let path = input.to_str().unwrap();
        let args = Args::try_parse_from(["tool", "--count", "1", path, "--output", path]).unwrap();
        assert_eq!(run(&args).unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(std::fs::read(&input).unwrap(), b">a\nACGT\n");
    }

    /// Every flag is spelled out, so a call site says what it does and nothing is remembered
    /// by letter. `-h` and `-V` are clap's own and stay.
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
                "count",
                "fraction",
                "seed",
                "output",
                "dry-run",
                "quiet",
                "line-width",
                "color",
                "summary",
                "threads",
                "help",
                "version"
            ]
        );
    }
}
