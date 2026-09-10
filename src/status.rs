//! Live status display.
//!
//! `render` is pure: it turns a `Snapshot` into lines that are guaranteed to
//! fit the given terminal width. The layout degrades in stages as the terminal
//! narrows, and every line is truncated as a final safety net, so the display
//! cannot corrupt a small terminal.

use std::time::Duration;

/// A file currently being written and uploaded.
#[derive(Debug, Clone)]
pub struct ActiveFile {
    pub name: String,
    pub bytes: u64,
}

/// One sampled view of generation and upload progress.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub elapsed: Duration,
    pub threads: usize,
    pub writers: usize,
    /// Batches waiting in the queue, and its capacity.
    pub queued: usize,
    pub queue_capacity: usize,
    pub rows: u64,
    pub target_rows: Option<u64>,
    pub bytes_generated: u64,
    pub bytes_uploaded: u64,
    pub target_bytes: Option<u64>,
    pub buffered_bytes: u64,
    pub files_completed: u64,
    pub active_files: Vec<ActiveFile>,
    pub rows_per_sec: f64,
    pub gen_bytes_per_sec: f64,
    pub upload_bytes_per_sec: f64,
    pub file_cap: Option<u64>,
    /// Generators have stopped; the workers are draining the queue.
    pub draining: bool,
    /// Set once no more rows will be produced, so the display can say so.
    pub finished: bool,
}

/// Below this the progress bar is dropped.
const BAR_MIN_WIDTH: usize = 68;
/// Below this the layout switches to the compact two-line form.
const COMPACT_MAX_WIDTH: usize = 52;

pub fn render(snapshot: &Snapshot, width: usize) -> Vec<String> {
    let width = width.clamp(20, 200);
    let lines = if width < COMPACT_MAX_WIDTH {
        render_compact(snapshot, width)
    } else {
        render_full(snapshot, width)
    };
    lines
        .into_iter()
        .map(|line| truncate(&line, width))
        .collect()
}

fn render_full(snapshot: &Snapshot, width: usize) -> Vec<String> {
    let mut lines = Vec::with_capacity(4);
    let short = width < 64;

    // Line 1: rows against target, with a bar and ETA when there is room.
    lines.push(progress_line(snapshot, width, short));

    // Line 2: generation side. There is no byte rate here on purpose: a row's
    // size is not known until its row group is encoded, so the only honest
    // byte rate belongs to the upload line below.
    let generating = if snapshot.draining {
        "draining".to_string()
    } else if snapshot.queued >= snapshot.queue_capacity && snapshot.queue_capacity > 0 {
        // Generators are blocked on a full queue, which is upload backpressure
        // rather than a stall. Say so, or a zero rate looks like a hang.
        format!("{} rows/s upload-bound", fmt_count(snapshot.rows_per_sec as u64))
    } else {
        format!("{} rows/s", fmt_count(snapshot.rows_per_sec as u64))
    };
    lines.push(pair_line(
        "gen",
        &generating,
        &[
            ("threads", snapshot.threads.to_string()),
            ("queue", format!("{}/{}", snapshot.queued, snapshot.queue_capacity)),
            ("buffered", fmt_bytes(snapshot.buffered_bytes)),
        ],
        width,
        short,
    ));

    // Line 3: upload side.
    let active = snapshot.active_files.len();
    let files = if active > 0 {
        format!("{} done, {} active", snapshot.files_completed, active)
    } else {
        format!("{} done, {} writers", snapshot.files_completed, snapshot.writers)
    };
    lines.push(pair_line(
        "up",
        &files,
        &[
            ("sent", fmt_bytes(snapshot.bytes_uploaded)),
            ("rate", format!("{}/s", fmt_bytes(snapshot.upload_bytes_per_sec as u64))),
        ],
        width,
        short,
    ));

    // Line 4: the files in flight, as many as fit.
    if !snapshot.active_files.is_empty() {
        lines.push(active_files_line(snapshot, width));
    }
    lines
}

fn render_compact(snapshot: &Snapshot, width: usize) -> Vec<String> {
    let mut lines = Vec::with_capacity(3);
    let rows = match snapshot.target_rows {
        Some(target) => format!(
            "{}/{} {}",
            fmt_count_short(snapshot.rows),
            fmt_count_short(target),
            fmt_percent(snapshot.rows, target)
        ),
        None => fmt_count_short(snapshot.rows),
    };
    lines.push(clip(&format!(" rows {}  {}", rows, tail_timing(snapshot)), width));
    lines.push(clip(
        &format!(
            " gen  {}/s  buf {}",
            fmt_count_short(snapshot.rows_per_sec as u64),
            fmt_bytes(snapshot.buffered_bytes)
        ),
        width,
    ));
    lines.push(clip(
        &format!(
            " up   {} f  {}  {}/s",
            snapshot.files_completed,
            fmt_bytes(snapshot.bytes_uploaded),
            fmt_bytes(snapshot.upload_bytes_per_sec as u64)
        ),
        width,
    ));
    lines
}

fn progress_line(snapshot: &Snapshot, width: usize, short: bool) -> String {
    let label = " rows ";
    let timing = tail_timing(snapshot);

    let Some((done, target)) = progress_target(snapshot) else {
        // No target: show the count and elapsed time only.
        let counts = fmt_count(snapshot.rows);
        return pad_between(&format!("{}{}", label, counts), &timing, width);
    };

    // Right-align the running count against the target so digits do not jitter.
    let target_text = if snapshot.target_rows.is_some() {
        fmt_count(target)
    } else {
        fmt_bytes(target)
    };
    let done_text = if snapshot.target_rows.is_some() {
        fmt_count(done)
    } else {
        fmt_bytes(done)
    };
    let counts = format!(
        "{:>width$} / {}",
        done_text,
        target_text,
        width = target_text.chars().count()
    );
    let percent = fmt_percent(done, target);

    let head = format!("{}{}  {:>4}", label, counts, percent);
    if short || width < BAR_MIN_WIDTH {
        return pad_between(&head, &timing, width);
    }

    // Give the bar whatever space is left, within sane bounds.
    let used = head.chars().count() + timing.chars().count() + 4;
    let bar_width = width.saturating_sub(used).clamp(0, 36);
    if bar_width < 8 {
        return pad_between(&head, &timing, width);
    }
    let bar = fmt_bar(done, target, bar_width);
    pad_between(&format!("{}  {}", head, bar), &timing, width)
}

/// `label value` on the left, `key value` pairs packed to the right.
fn pair_line(label: &str, value: &str, pairs: &[(&str, String)], width: usize, short: bool) -> String {
    let head = format!(" {:<4} {}", label, value);
    let mut tail = String::new();
    for (key, value) in pairs {
        let piece = if short {
            format!("{} {}", &key[..key.len().min(3)], value)
        } else {
            format!("{} {}", key, value)
        };
        let candidate = if tail.is_empty() {
            piece
        } else {
            format!("{}  {}", tail, piece)
        };
        // Only keep a pair if the whole line still fits.
        if head.chars().count() + 2 + candidate.chars().count() <= width {
            tail = candidate;
        }
    }
    pad_between(&head, &tail, width)
}

fn active_files_line(snapshot: &Snapshot, width: usize) -> String {
    let mut out = String::from(" file ");
    let mut first = true;
    for file in &snapshot.active_files {
        let piece = match snapshot.file_cap {
            Some(cap) => format!("{} {}/{}", file.name, fmt_bytes(file.bytes), fmt_bytes(cap)),
            None => format!("{} {}", file.name, fmt_bytes(file.bytes)),
        };
        let sep = if first { "" } else { "  " };
        if out.chars().count() + sep.chars().count() + piece.chars().count() > width {
            // Signal that more files are in flight than fit on the line.
            let more = format!("{}+{}", sep, snapshot.active_files.len() - count_shown(&out));
            if out.chars().count() + more.chars().count() <= width {
                out.push_str(&more);
            }
            break;
        }
        out.push_str(sep);
        out.push_str(&piece);
        first = false;
    }
    out
}

fn count_shown(line: &str) -> usize {
    line.matches(".parquet").count().max(line.split("  ").count().saturating_sub(1))
}

fn progress_target(snapshot: &Snapshot) -> Option<(u64, u64)> {
    if let Some(target) = snapshot.target_rows {
        return Some((snapshot.rows.min(target), target));
    }
    snapshot
        .target_bytes
        .map(|target| (snapshot.bytes_generated.min(target), target))
}

/// ETA when a target and a rate are known, otherwise elapsed time.
fn tail_timing(snapshot: &Snapshot) -> String {
    if snapshot.finished {
        return format!("done in {}", fmt_duration(snapshot.elapsed));
    }
    if let Some((done, target)) = progress_target(snapshot) {
        let rate = if snapshot.target_rows.is_some() {
            snapshot.rows_per_sec
        } else {
            snapshot.gen_bytes_per_sec
        };
        if rate > 0.0 && target > done {
            let seconds = (target - done) as f64 / rate;
            if seconds.is_finite() && seconds < 86_400.0 * 30.0 {
                return format!("ETA {}", fmt_duration(Duration::from_secs(seconds as u64)));
            }
        }
    }
    fmt_duration(snapshot.elapsed)
}

fn fmt_bar(done: u64, target: u64, width: usize) -> String {
    if target == 0 || width == 0 {
        return String::new();
    }
    let filled = ((done as f64 / target as f64) * width as f64).round() as usize;
    let filled = filled.min(width);
    // Block characters are one column wide, so char counting stays accurate.
    let mut bar = String::with_capacity(width * 3);
    for _ in 0..filled {
        bar.push('\u{2588}');
    }
    for _ in filled..width {
        bar.push('\u{2591}');
    }
    bar
}

fn fmt_percent(done: u64, target: u64) -> String {
    if target == 0 {
        return "  0%".to_string();
    }
    let percent = ((done as f64 / target as f64) * 100.0).min(100.0);
    format!("{:.0}%", percent)
}

/// Group digits with commas: 12847392 -> "12,847,392".
fn fmt_count(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, ch) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

/// Compact counts for narrow terminals: 12847392 -> "12.8M".
fn fmt_count_short(value: u64) -> String {
    const UNITS: [(u64, &str); 4] = [
        (1_000_000_000_000, "T"),
        (1_000_000_000, "G"),
        (1_000_000, "M"),
        (1_000, "K"),
    ];
    for (scale, suffix) in UNITS {
        if value >= scale {
            let scaled = value as f64 / scale as f64;
            return if scaled < 10.0 {
                format!("{:.1}{}", scaled, suffix)
            } else {
                format!("{:.0}{}", scaled, suffix)
            };
        }
    }
    value.to_string()
}

fn fmt_bytes(value: u64) -> String {
    const UNITS: [(u64, &str); 4] = [
        (1u64 << 40, "TiB"),
        (1u64 << 30, "GiB"),
        (1u64 << 20, "MiB"),
        (1u64 << 10, "KiB"),
    ];
    for (scale, suffix) in UNITS {
        if value >= scale {
            let scaled = value as f64 / scale as f64;
            return if scaled < 10.0 {
                format!("{:.1} {}", scaled, suffix)
            } else {
                format!("{:.0} {}", scaled, suffix)
            };
        }
    }
    format!("{} B", value)
}

fn fmt_duration(duration: Duration) -> String {
    let total = duration.as_secs();
    let (hours, minutes, seconds) = (total / 3600, (total % 3600) / 60, total % 60);
    if hours > 0 {
        format!("{}h{:02}m", hours, minutes)
    } else if minutes > 0 {
        format!("{}m{:02}s", minutes, seconds)
    } else {
        format!("{}s", seconds)
    }
}

/// Push `right` to the right edge, keeping at least one space of separation.
fn pad_between(left: &str, right: &str, width: usize) -> String {
    if right.is_empty() {
        return clip(left, width);
    }
    let left_len = left.chars().count();
    let right_len = right.chars().count();
    if left_len + right_len + 1 > width {
        // No room for both: the left side carries the more important numbers.
        return clip(left, width);
    }
    let gap = width - left_len - right_len - 1;
    format!("{}{}{} ", left, " ".repeat(gap), right)
}

fn clip(text: &str, width: usize) -> String {
    truncate(text, width)
}

fn truncate(text: &str, width: usize) -> String {
    if text.chars().count() <= width {
        return text.to_string();
    }
    text.chars().take(width).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Snapshot {
        Snapshot {
            elapsed: Duration::from_secs(94),
            threads: 4,
            writers: 8,
            queued: 12,
            queue_capacity: 30,
            rows: 12_847_392,
            target_rows: Some(50_000_000),
            bytes_generated: 17_800_000_000,
            bytes_uploaded: 15_781_000_000,
            target_bytes: None,
            buffered_bytes: 192 << 20,
            files_completed: 6,
            active_files: vec![
                ActiveFile { name: "part-000007.parquet".into(), bytes: 1_288_490_188 },
                ActiveFile { name: "part-000008.parquet".into(), bytes: 429_496_729 },
            ],
            rows_per_sec: 421_003.0,
            gen_bytes_per_sec: 61_000_000.0,
            upload_bytes_per_sec: 54_600_000.0,
            file_cap: Some(2 << 30),
            draining: false,
            finished: false,
        }
    }

    #[test]
    fn never_exceeds_the_terminal_width() {
        let mut snapshot = sample();
        for width in 20..=160 {
            for finished in [false, true] {
                for target in [Some(50_000_000), None] {
                    snapshot.finished = finished;
                    snapshot.target_rows = target;
                    // The wordiest state: a full queue adds "upload-bound".
                    snapshot.queued = snapshot.queue_capacity;
                    for line in render(&snapshot, width) {
                        assert!(
                            line.chars().count() <= width,
                            "width {} overflowed with line {:?} ({} chars)",
                            width,
                            line,
                            line.chars().count()
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn line_count_is_stable_so_redraw_does_not_drift() {
        let snapshot = sample();
        for width in [52, 60, 80, 100, 120] {
            let first = render(&snapshot, width).len();
            let mut later = snapshot.clone();
            later.rows = 49_999_999;
            later.bytes_uploaded = 40_000_000_000;
            assert_eq!(first, render(&later, width).len(), "line count moved at width {}", width);
        }
    }

    #[test]
    fn handles_zero_and_missing_values() {
        let snapshot = Snapshot { threads: 1, queue_capacity: 30, ..Default::default() };
        let lines = render(&snapshot, 80);
        assert!(!lines.is_empty());
        for line in &lines {
            assert!(line.chars().count() <= 80);
        }
        // A zero target must not divide by zero or claim progress.
        let zero_target = Snapshot { target_rows: Some(0), ..Default::default() };
        assert!(render(&zero_target, 80).iter().any(|line| line.contains("0%")));
    }

    #[test]
    fn formats_numbers_readably() {
        assert_eq!(fmt_count(12_847_392), "12,847,392");
        assert_eq!(fmt_count(0), "0");
        assert_eq!(fmt_count_short(12_847_392), "13M");
        assert_eq!(fmt_count_short(1_500), "1.5K");
        assert_eq!(fmt_bytes(1_288_490_188), "1.2 GiB");
        assert_eq!(fmt_bytes(512), "512 B");
        assert_eq!(fmt_duration(Duration::from_secs(94)), "1m34s");
        assert_eq!(fmt_duration(Duration::from_secs(3_930)), "1h05m");
    }

    /// Not an assertion: prints the layout so it can be eyeballed.
    #[test]
    fn demo() {
        let mut snapshot = sample();
        for width in [100usize, 80, 64, 48] {
            println!("\n--- {} columns {}", width, "-".repeat(width.saturating_sub(16)));
            for line in render(&snapshot, width) {
                println!("{}", line);
            }
        }

        println!("\n--- 80 columns, no row target (unbounded stream) ---");
        snapshot.target_rows = None;
        for line in render(&snapshot, 80) {
            println!("{}", line);
        }

        println!("\n--- 80 columns, size target instead of rows ---");
        snapshot.target_bytes = Some(50 << 30);
        for line in render(&snapshot, 80) {
            println!("{}", line);
        }

        println!("\n--- 80 columns, finished ---");
        snapshot.finished = true;
        snapshot.active_files.clear();
        snapshot.files_completed = 24;
        snapshot.buffered_bytes = 0;
        for line in render(&snapshot, 80) {
            println!("{}", line);
        }
    }

    #[test]
    fn a_full_queue_is_labelled_upload_bound_not_a_stall() {
        let mut snapshot = sample();
        snapshot.queued = snapshot.queue_capacity;
        snapshot.rows_per_sec = 0.0;
        let text = render(&snapshot, 120).join("\n");
        assert!(text.contains("upload-bound"), "{}", text);

        snapshot.draining = true;
        let draining = render(&snapshot, 120).join("\n");
        assert!(draining.contains("draining"), "{}", draining);
        assert!(!draining.contains("upload-bound"), "draining wins: {}", draining);

        snapshot.draining = false;
        snapshot.queued = 3;
        let running = render(&snapshot, 120).join("\n");
        assert!(!running.contains("upload-bound"), "{}", running);
    }
}
