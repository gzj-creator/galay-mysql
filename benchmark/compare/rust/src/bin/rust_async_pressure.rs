use std::sync::Arc;
use std::time::Instant;

use mysql_async::prelude::Queryable;
use mysql_async::Pool;
use tokio::task::JoinSet;

#[derive(Clone)]
struct Config {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    clients: usize,
    queries_per_client: usize,
    warmup_queries: usize,
    sql: String,
}

#[derive(Default)]
struct WorkerStats {
    success: u64,
    failed: u64,
    latency_ns_total: u64,
    latencies_ns: Vec<u64>,
    first_error: Option<String>,
}

fn env_or_default(keys: &[&str], default: &str) -> String {
    for key in keys {
        if let Ok(v) = std::env::var(key) {
            return v;
        }
    }
    default.to_string()
}

fn env_usize_or_default(keys: &[&str], default: usize) -> usize {
    for key in keys {
        if let Ok(v) = std::env::var(key) {
            if let Ok(parsed) = v.parse::<usize>() {
                if parsed > 0 {
                    return parsed;
                }
            }
        }
    }
    default
}

fn env_u16_or_default(keys: &[&str], default: u16) -> u16 {
    for key in keys {
        if let Ok(v) = std::env::var(key) {
            if let Ok(parsed) = v.parse::<u16>() {
                if parsed > 0 {
                    return parsed;
                }
            }
        }
    }
    default
}

fn parse_args(mut cfg: Config) -> Result<Config, String> {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--clients" => {
                i += 1;
                let v = args.get(i).ok_or("missing --clients value")?;
                cfg.clients = v.parse::<usize>().map_err(|_| "invalid --clients value")?;
            }
            "--queries" => {
                i += 1;
                let v = args.get(i).ok_or("missing --queries value")?;
                cfg.queries_per_client = v.parse::<usize>().map_err(|_| "invalid --queries value")?;
            }
            "--warmup" => {
                i += 1;
                let v = args.get(i).ok_or("missing --warmup value")?;
                cfg.warmup_queries = v.parse::<usize>().map_err(|_| "invalid --warmup value")?;
            }
            "--timeout-sec" => {
                i += 1;
                if args.get(i).is_none() {
                    return Err("missing --timeout-sec value".to_string());
                }
            }
            "--sql" => {
                i += 1;
                let v = args.get(i).ok_or("missing --sql value")?;
                cfg.sql = v.clone();
            }
            "--mode" | "--batch-size" | "--buffer-size" => {
                i += 1;
                if args.get(i).is_none() {
                    return Err(format!("missing {} value", args[i - 1]));
                }
            }
            "--alloc-stats" => {}
            unknown => return Err(format!("unknown argument: {unknown}")),
        }
        i += 1;
    }
    Ok(cfg)
}

fn percentile_ms(samples_ns: &mut [u64], p: f64) -> f64 {
    if samples_ns.is_empty() {
        return 0.0;
    }
    samples_ns.sort_unstable();
    let clamped = p.clamp(0.0, 1.0);
    let idx = (clamped * (samples_ns.len().saturating_sub(1) as f64)) as usize;
    (samples_ns[idx] as f64) / 1e6
}

async fn run_worker(cfg: Arc<Config>) -> WorkerStats {
    let mut stats = WorkerStats::default();
    let url = format!(
        "mysql://{}:{}@{}:{}/{}",
        cfg.user, cfg.password, cfg.host, cfg.port, cfg.database
    );
    let pool = Pool::new(url.as_str());
    let mut conn = match pool.get_conn().await {
        Ok(c) => c,
        Err(e) => {
            stats.failed = cfg.queries_per_client as u64;
            stats.first_error = Some(format!("connect failed: {e}"));
            let _ = pool.disconnect().await;
            return stats;
        }
    };

    for _ in 0..cfg.warmup_queries {
        let _ = conn.query_drop(cfg.sql.as_str()).await;
    }

    for _ in 0..cfg.queries_per_client {
        let started = Instant::now();
        match conn.query_drop(cfg.sql.as_str()).await {
            Ok(_) => {
                let elapsed = started.elapsed().as_nanos() as u64;
                stats.success += 1;
                stats.latency_ns_total += elapsed;
                stats.latencies_ns.push(elapsed);
            }
            Err(e) => {
                stats.failed += 1;
                if stats.first_error.is_none() {
                    stats.first_error = Some(format!("query failed: {e}"));
                }
            }
        }
    }

    let _ = conn.disconnect().await;
    let _ = pool.disconnect().await;
    stats
}

fn main() {
    let cfg = Config {
        host: env_or_default(&["GALAY_MYSQL_HOST", "MYSQL_HOST"], "127.0.0.1"),
        port: env_u16_or_default(&["GALAY_MYSQL_PORT", "MYSQL_PORT"], 3306),
        user: env_or_default(&["GALAY_MYSQL_USER", "MYSQL_USER"], "root"),
        password: env_or_default(&["GALAY_MYSQL_PASSWORD", "MYSQL_PASSWORD"], "password"),
        database: env_or_default(&["GALAY_MYSQL_DB", "MYSQL_DATABASE"], "test"),
        clients: env_usize_or_default(&["GALAY_MYSQL_BENCH_CLIENTS", "MYSQL_BENCH_CLIENTS"], 64),
        queries_per_client: env_usize_or_default(&["GALAY_MYSQL_BENCH_QUERIES", "MYSQL_BENCH_QUERIES"], 200),
        warmup_queries: env_usize_or_default(&["GALAY_MYSQL_BENCH_WARMUP", "MYSQL_BENCH_WARMUP"], 10),
        sql: env_or_default(&["GALAY_MYSQL_BENCH_SQL", "MYSQL_BENCH_SQL"], "SELECT 1"),
    };
    let cfg = match parse_args(cfg) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };

    println!(
        "MySQL config: host={}, port={}, user={}, db={}",
        cfg.host, cfg.port, cfg.user, cfg.database
    );
    println!(
        "Benchmark config: clients={}, queries_per_client={}, warmup={}, mode=async",
        cfg.clients, cfg.queries_per_client, cfg.warmup_queries
    );
    println!("SQL: {}", cfg.sql);
    println!("Running rust async pressure benchmark...");

    let worker_threads = cfg.clients.clamp(1, 16);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    let cfg = Arc::new(cfg);
    let started = Instant::now();
    let (success, failed, latency_ns_total, mut all_latencies, first_error) = rt.block_on(async {
        let mut set = JoinSet::new();
        for _ in 0..cfg.clients {
            let worker_cfg = Arc::clone(&cfg);
            set.spawn(run_worker(worker_cfg));
        }

        let mut success = 0u64;
        let mut failed = 0u64;
        let mut latency_ns_total = 0u64;
        let mut all_latencies = Vec::with_capacity(cfg.clients * cfg.queries_per_client);
        let mut first_error: Option<String> = None;
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok(worker) => {
                    success += worker.success;
                    failed += worker.failed;
                    latency_ns_total += worker.latency_ns_total;
                    all_latencies.extend(worker.latencies_ns);
                    if first_error.is_none() {
                        first_error = worker.first_error;
                    }
                }
                Err(e) => {
                    failed += cfg.queries_per_client as u64;
                    if first_error.is_none() {
                        first_error = Some(format!("worker task failed: {e}"));
                    }
                }
            }
        }
        (success, failed, latency_ns_total, all_latencies, first_error)
    });

    let elapsed_sec = started.elapsed().as_secs_f64();
    let total = success + failed;
    let qps = if elapsed_sec > 0.0 {
        success as f64 / elapsed_sec
    } else {
        0.0
    };
    let avg_latency_ms = if total > 0 {
        (latency_ns_total as f64) / (total as f64) / 1e6
    } else {
        0.0
    };
    let p50 = percentile_ms(&mut all_latencies, 0.50);
    let p95 = percentile_ms(&mut all_latencies, 0.95);
    let p99 = percentile_ms(&mut all_latencies, 0.99);
    let max = percentile_ms(&mut all_latencies, 1.0);

    println!("\n=== Rust B2 Async Pressure Summary ===");
    println!("clients: {}", cfg.clients);
    println!("queries_per_client: {}", cfg.queries_per_client);
    println!("total_queries: {}", total);
    println!("success: {}", success);
    println!("failed: {}", failed);
    println!("elapsed_sec: {}", elapsed_sec);
    println!("qps: {}", qps);
    println!("avg_latency_ms: {}", avg_latency_ms);
    println!("p50_latency_ms: {}", p50);
    println!("p95_latency_ms: {}", p95);
    println!("p99_latency_ms: {}", p99);
    println!("max_latency_ms: {}", max);
    if let Some(err) = first_error {
        println!("first_error: {}", err);
    }

    if failed > 0 {
        std::process::exit(1);
    }
}
