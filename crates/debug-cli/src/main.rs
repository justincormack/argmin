use rusqlite::{Connection, OpenFlags};
use std::path::{Path, PathBuf};
use std::{fmt::Write as _, fs, process};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        usage();
    }
    let data_dir = Path::new(&args[1]);
    if !data_dir.is_dir() {
        eprintln!("error: {} is not a directory", data_dir.display());
        process::exit(1);
    }
    let command = args[2].as_str();
    match command {
        "buckets" => cmd_buckets(data_dir),
        "objects" => {
            let mut pg_filter = None;
            let mut bucket_filter = None;
            let mut prefix_filter = None;
            let mut i = 3;
            while i < args.len() {
                match args[i].as_str() {
                    "--pg" => {
                        i += 1;
                        pg_filter = Some(parse_u32_arg(&args, i, "--pg"));
                    }
                    "--bucket" => {
                        i += 1;
                        bucket_filter = Some(require_arg(&args, i, "--bucket").to_string());
                    }
                    "--prefix" => {
                        i += 1;
                        prefix_filter = Some(require_arg(&args, i, "--prefix").to_string());
                    }
                    other => {
                        eprintln!("error: unknown flag '{other}' for objects command");
                        process::exit(1);
                    }
                }
                i += 1;
            }
            cmd_objects(data_dir, pg_filter, bucket_filter, prefix_filter);
        }
        "shards" => {
            let mut pg_filter = None;
            let mut i = 3;
            while i < args.len() {
                match args[i].as_str() {
                    "--pg" => {
                        i += 1;
                        pg_filter = Some(parse_u32_arg(&args, i, "--pg"));
                    }
                    other => {
                        eprintln!("error: unknown flag '{other}' for shards command");
                        process::exit(1);
                    }
                }
                i += 1;
            }
            cmd_shards(data_dir, pg_filter);
        }
        "summary" => cmd_summary(data_dir),
        other => {
            eprintln!("error: unknown command '{other}'");
            usage();
        }
    }
}

fn usage() -> ! {
    eprintln!("Usage: argmin-debug <data-dir> <command> [options]");
    eprintln!();
    eprintln!("Commands:");
    eprintln!("  buckets                          Dump bucket database");
    eprintln!("  objects [--pg N] [--bucket NAME] [--prefix PFX]");
    eprintln!("                                   Dump objects from PG metadata databases");
    eprintln!("  shards [--pg N]                  Dump shards from PG metadata databases");
    eprintln!("  summary                          Show overview counts and sizes");
    process::exit(1);
}

fn require_arg<'a>(args: &'a [String], i: usize, flag: &str) -> &'a str {
    if i >= args.len() {
        eprintln!("error: {flag} requires a value");
        process::exit(1);
    }
    &args[i]
}

fn parse_u32_arg(args: &[String], i: usize, flag: &str) -> u32 {
    let s = require_arg(args, i, flag);
    s.parse::<u32>().unwrap_or_else(|_| {
        eprintln!("error: {flag} value '{s}' is not a valid number");
        process::exit(1);
    })
}

// --- SQLite helpers ---

fn open_readonly(path: &Path) -> Result<Connection, rusqlite::Error> {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
}

fn discover_pgs(data_dir: &Path) -> Vec<(u32, PathBuf)> {
    let mut pgs = Vec::new();
    let entries = match fs::read_dir(data_dir) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("error: cannot read {}: {e}", data_dir.display());
            process::exit(1);
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(id_str) = name.strip_prefix("pg-") {
            if let Ok(id) = id_str.parse::<u32>() {
                let meta_path = entry.path().join("metadata.db");
                if meta_path.exists() {
                    pgs.push((id, meta_path));
                }
            }
        }
    }
    pgs.sort_unstable_by_key(|(id, _)| *id);
    pgs
}

// --- Formatting helpers ---

/// Convert days since Unix epoch to (year, month, day) using Hinnant's algorithm.
fn days_to_date(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Format a unix millisecond timestamp as ISO 8601.
fn format_timestamp_millis(millis: u64) -> String {
    let secs = millis / 1000;
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;
    let (year, month, day) = days_to_date(days_since_epoch as i64);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.000Z",
        year, month, day, hours, minutes, seconds
    )
}

/// Format a unix second timestamp as ISO 8601.
fn format_timestamp_secs(secs: i64) -> String {
    let days_since_epoch = secs.div_euclid(86400);
    let time_of_day = secs.rem_euclid(86400) as u64;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;
    let (year, month, day) = days_to_date(days_since_epoch);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

/// Format 8-byte big-endian etag blob as quoted hex string.
fn format_etag(blob: &[u8]) -> String {
    let mut s = String::with_capacity(18);
    s.push('"');
    for b in blob {
        write!(s, "{:02x}", b).unwrap();
    }
    s.push('"');
    s
}

/// Format byte count as human-readable size.
fn format_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * 1024 * 1024;
    const TIB: u64 = 1024 * 1024 * 1024 * 1024;
    if bytes >= TIB {
        format!("{:.1} TiB", bytes as f64 / TIB as f64)
    } else if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format object status integer.
fn format_object_status(status: i32) -> &'static str {
    match status {
        0 => "Live",
        1 => "DeleteMarker",
        2 => "PendingDelete",
        _ => "Unknown",
    }
}

/// Format shard status integer.
fn format_shard_status(status: i32) -> &'static str {
    match status {
        0 => "Live",
        1 => "Deleting",
        2 => "Quarantined",
        _ => "Unknown",
    }
}

/// Format versioning integer.
fn format_versioning(v: i32) -> &'static str {
    match v {
        0 => "Disabled",
        1 => "Enabled",
        2 => "Suspended",
        _ => "Unknown",
    }
}

// --- Column-aligned printing ---

fn print_table(headers: &[&str], rows: &[Vec<String>]) {
    if rows.is_empty() {
        println!("(no rows)");
        return;
    }
    let ncols = headers.len();
    let mut widths = vec![0usize; ncols];
    for (i, h) in headers.iter().enumerate() {
        widths[i] = h.len();
    }
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i < ncols {
                widths[i] = widths[i].max(val.len());
            }
        }
    }
    // Print header
    for (i, h) in headers.iter().enumerate() {
        if i > 0 {
            print!("  ");
        }
        print!("{:width$}", h, width = widths[i]);
    }
    println!();
    // Print separator
    for (i, w) in widths.iter().enumerate() {
        if i > 0 {
            print!("  ");
        }
        print!("{}", "-".repeat(*w));
    }
    println!();
    // Print rows
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i >= ncols {
                break;
            }
            if i > 0 {
                print!("  ");
            }
            print!("{:width$}", val, width = widths[i]);
        }
        println!();
    }
}

// --- Subcommands ---

fn cmd_buckets(data_dir: &Path) {
    let pgs = discover_pgs(data_dir);
    if pgs.is_empty() {
        println!("(no PG databases found)");
        return;
    }

    let mut rows: Vec<Vec<String>> = Vec::new();

    for (pg_id, meta_path) in pgs {
        let conn = match open_readonly(&meta_path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("warning: cannot open {}: {e}", meta_path.display());
                continue;
            }
        };

        let mut stmt = match conn.prepare(
            "SELECT name, owner_principal, created_at, region, versioning \
             FROM buckets ORDER BY name",
        ) {
            Ok(stmt) => stmt,
            Err(e) => {
                eprintln!("warning: query failed on pg-{pg_id}: {e}");
                continue;
            }
        };

        let pg_rows = match stmt.query_map([], |row| {
            let name: String = row.get(0)?;
            let owner_principal: String = row.get(1)?;
            let created_at: u64 = row.get(2)?;
            let region: i32 = row.get(3)?;
            let versioning: i32 = row.get(4)?;
            Ok(vec![
                pg_id.to_string(),
                name,
                owner_principal,
                format_timestamp_millis(created_at),
                region.to_string(),
                format_versioning(versioning).to_string(),
            ])
        }) {
            Ok(rows) => rows,
            Err(e) => {
                eprintln!("warning: query failed on pg-{pg_id}: {e}");
                continue;
            }
        };

        rows.extend(pg_rows.flatten());
    }

    rows.sort_by(|a, b| a[1].cmp(&b[1]).then_with(|| a[0].cmp(&b[0])));
    print_table(
        &["PG", "NAME", "OWNER", "CREATED", "REGION", "VERSIONING"],
        &rows,
    );
}

fn cmd_objects(
    data_dir: &Path,
    pg_filter: Option<u32>,
    bucket_filter: Option<String>,
    prefix_filter: Option<String>,
) {
    let pgs = match pg_filter {
        Some(id) => {
            let meta_path = data_dir.join(format!("pg-{id}")).join("metadata.db");
            if !meta_path.exists() {
                eprintln!("error: {} not found", meta_path.display());
                process::exit(1);
            }
            vec![(id, meta_path)]
        }
        None => discover_pgs(data_dir),
    };

    if pgs.is_empty() {
        println!("(no PG databases found)");
        return;
    }

    let mut all_rows = Vec::new();
    for (pg_id, meta_path) in &pgs {
        let conn = match open_readonly(meta_path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("warning: cannot open {}: {e}", meta_path.display());
                continue;
            }
        };

        let mut sql = String::from(
            "SELECT bucket, key, version_id, size, total_size, etag, etag_kind, \
             last_modified, ec_k, ec_m, status \
             FROM objects WHERE 1=1",
        );
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(ref bucket) = bucket_filter {
            sql.push_str(" AND bucket = ?");
            params.push(Box::new(bucket.clone()));
        }
        if let Some(ref prefix) = prefix_filter {
            sql.push_str(" AND key LIKE ? || '%'");
            params.push(Box::new(prefix.clone()));
        }
        sql.push_str(" ORDER BY bucket, key");

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();

        let mut stmt = match conn.prepare(&sql) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("warning: query failed on pg-{pg_id}: {e}");
                continue;
            }
        };

        let rows = match stmt.query_map(param_refs.as_slice(), |row| {
            let bucket: String = row.get(0)?;
            let key: String = row.get(1)?;
            let version_id: String = row.get(2)?;
            let size: u64 = row.get(3)?;
            let total_size: u64 = row.get(4)?;
            let etag: Vec<u8> = row.get(5)?;
            let _etag_kind: i32 = row.get(6)?;
            let last_modified: u64 = row.get(7)?;
            let ec_k: u32 = row.get(8)?;
            let ec_m: u32 = row.get(9)?;
            let status: i32 = row.get(10)?;
            Ok(vec![
                pg_id.to_string(),
                bucket,
                key,
                version_id,
                format_size(size),
                format_size(total_size),
                format_etag(&etag),
                format!("{}+{}", ec_k, ec_m),
                format_timestamp_millis(last_modified),
                format_object_status(status).to_string(),
            ])
        }) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("warning: query failed on pg-{pg_id}: {e}");
                continue;
            }
        };

        for row in rows.flatten() {
            all_rows.push(row);
        }
    }

    print_table(
        &[
            "PG",
            "BUCKET",
            "KEY",
            "VERSION",
            "SIZE",
            "TOTAL_SIZE",
            "ETAG",
            "EC",
            "LAST_MODIFIED",
            "STATUS",
        ],
        &all_rows,
    );
}

fn cmd_shards(data_dir: &Path, pg_filter: Option<u32>) {
    let pgs = match pg_filter {
        Some(id) => {
            let meta_path = data_dir.join(format!("pg-{id}")).join("metadata.db");
            if !meta_path.exists() {
                eprintln!("error: {} not found", meta_path.display());
                process::exit(1);
            }
            vec![(id, meta_path)]
        }
        None => discover_pgs(data_dir),
    };

    if pgs.is_empty() {
        println!("(no PG databases found)");
        return;
    }

    let mut all_rows = Vec::new();
    for (pg_id, meta_path) in &pgs {
        let conn = match open_readonly(meta_path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("warning: cannot open {}: {e}", meta_path.display());
                continue;
            }
        };

        let mut stmt = match conn.prepare(
            "SELECT shard_key, data_size, crc64_nvme, created_at, last_verified, status \
             FROM shards ORDER BY shard_key",
        ) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("warning: query failed on pg-{pg_id}: {e}");
                continue;
            }
        };

        let rows = match stmt.query_map([], |row| {
            let shard_key: Vec<u8> = row.get(0)?;
            let data_size: u64 = row.get(1)?;
            let crc64: i64 = row.get(2)?;
            let created_at: i64 = row.get(3)?;
            let last_verified: Option<i64> = row.get(4)?;
            let status: i32 = row.get(5)?;

            let shard_hex =
                shard_key
                    .iter()
                    .fold(String::with_capacity(shard_key.len() * 2), |mut s, b| {
                        write!(s, "{:02x}", b).unwrap();
                        s
                    });

            Ok(vec![
                pg_id.to_string(),
                shard_hex,
                format_size(data_size),
                format!("{:016x}", crc64 as u64),
                format_timestamp_secs(created_at),
                last_verified
                    .map(format_timestamp_secs)
                    .unwrap_or_else(|| "-".to_string()),
                format_shard_status(status).to_string(),
            ])
        }) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("warning: query failed on pg-{pg_id}: {e}");
                continue;
            }
        };

        for row in rows.flatten() {
            all_rows.push(row);
        }
    }

    print_table(
        &[
            "PG",
            "SHARD_KEY",
            "SIZE",
            "CRC64",
            "CREATED",
            "LAST_VERIFIED",
            "STATUS",
        ],
        &all_rows,
    );
}

fn cmd_summary(data_dir: &Path) {
    // Per-PG stats
    let pgs = discover_pgs(data_dir);
    if pgs.is_empty() {
        println!("Buckets: 0");
        println!();
        println!("(no PG databases found)");
        return;
    }

    let mut total_buckets: u64 = 0;
    let mut total_objects: u64 = 0;
    let mut total_shards: u64 = 0;
    let mut total_data_size: u64 = 0;

    let mut pg_rows = Vec::new();
    for (pg_id, meta_path) in &pgs {
        let conn = match open_readonly(meta_path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("warning: cannot open {}: {e}", meta_path.display());
                continue;
            }
        };

        let obj_count: u64 = conn
            .query_row("SELECT COUNT(*) FROM objects", [], |row| row.get(0))
            .unwrap_or(0);
        let bucket_count: u64 = conn
            .query_row("SELECT COUNT(*) FROM buckets", [], |row| row.get(0))
            .unwrap_or(0);
        let shard_count: u64 = conn
            .query_row("SELECT COUNT(*) FROM shards", [], |row| row.get(0))
            .unwrap_or(0);
        let data_size: u64 = conn
            .query_row(
                "SELECT COALESCE(SUM(data_size), 0) FROM shards",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);

        total_buckets += bucket_count;
        total_objects += obj_count;
        total_shards += shard_count;
        total_data_size += data_size;

        pg_rows.push(vec![
            pg_id.to_string(),
            obj_count.to_string(),
            shard_count.to_string(),
            format_size(data_size),
        ]);
    }

    println!("Buckets: {total_buckets}");
    println!();

    print_table(&["PG", "OBJECTS", "SHARDS", "DATA_SIZE"], &pg_rows);

    println!();
    println!(
        "Total: {} objects, {} shards, {}",
        total_objects,
        total_shards,
        format_size(total_data_size)
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_days_to_date_epoch() {
        assert_eq!(days_to_date(0), (1970, 1, 1));
    }

    #[test]
    fn test_days_to_date_known() {
        // 2024-01-15 = day 19737
        assert_eq!(days_to_date(19737), (2024, 1, 15));
    }

    #[test]
    fn test_format_timestamp_millis_epoch() {
        assert_eq!(format_timestamp_millis(0), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn test_format_timestamp_millis_known() {
        // 2024-01-15T11:30:45.000Z = 1705318245000 ms
        assert_eq!(
            format_timestamp_millis(1705318245000),
            "2024-01-15T11:30:45.000Z"
        );
    }

    #[test]
    fn test_format_timestamp_secs_epoch() {
        assert_eq!(format_timestamp_secs(0), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn test_format_timestamp_secs_known() {
        assert_eq!(format_timestamp_secs(1705318245), "2024-01-15T11:30:45Z");
    }

    #[test]
    fn test_format_etag_8_bytes() {
        let blob = [0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90];
        assert_eq!(format_etag(&blob), "\"abcdef1234567890\"");
    }

    #[test]
    fn test_format_etag_empty() {
        assert_eq!(format_etag(&[]), "\"\"");
    }

    #[test]
    fn test_format_size_bytes() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(512), "512 B");
        assert_eq!(format_size(1023), "1023 B");
    }

    #[test]
    fn test_format_size_kib() {
        assert_eq!(format_size(1024), "1.0 KiB");
        assert_eq!(format_size(1536), "1.5 KiB");
    }

    #[test]
    fn test_format_size_mib() {
        assert_eq!(format_size(1048576), "1.0 MiB");
        assert_eq!(format_size(10 * 1048576), "10.0 MiB");
    }

    #[test]
    fn test_format_size_gib() {
        assert_eq!(format_size(1073741824), "1.0 GiB");
    }

    #[test]
    fn test_format_size_tib() {
        assert_eq!(format_size(1099511627776), "1.0 TiB");
    }

    #[test]
    fn test_format_object_status() {
        assert_eq!(format_object_status(0), "Live");
        assert_eq!(format_object_status(1), "DeleteMarker");
        assert_eq!(format_object_status(2), "PendingDelete");
        assert_eq!(format_object_status(99), "Unknown");
    }

    #[test]
    fn test_format_shard_status() {
        assert_eq!(format_shard_status(0), "Live");
        assert_eq!(format_shard_status(1), "Deleting");
        assert_eq!(format_shard_status(2), "Quarantined");
        assert_eq!(format_shard_status(99), "Unknown");
    }

    #[test]
    fn test_format_versioning() {
        assert_eq!(format_versioning(0), "Disabled");
        assert_eq!(format_versioning(1), "Enabled");
        assert_eq!(format_versioning(2), "Suspended");
        assert_eq!(format_versioning(99), "Unknown");
    }
}
