mod benchmark;
mod config;

use anyhow::Result;
use mimalloc::MiMalloc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const MIN_WORKER_THREADS: usize = 10;
const DEFAULT_PARALLELISM: usize = 4;

fn main() -> Result<()> {
    let cfg = config::Config::from_file("config.toml")?;

    init_tracing(&cfg)?;

    let worker_threads = calculate_worker_threads(cfg.runtime_threads);

    // Setup graceful shutdown signal
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);
    
    ctrlc::set_handler(move || {
        tracing::warn!("shutdown_signal_received");
        shutdown_clone.store(true, Ordering::SeqCst);
    })?;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()?;

    rt.block_on(async move {
        tracing::info!(
            database_url = cfg.database.url,
            worker_threads,
            "starting_cockroachdb_benchmark"
        );

        if let Err(e) = benchmark::run_single_insert_suite(&cfg, shutdown).await {
            tracing::error!(error = %e, "benchmark_suite_failed");
            std::process::exit(1);
        }
    });

    Ok(())
}

fn init_tracing(_cfg: &config::Config) -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .flatten_event(true)
                .with_file(false)
                .with_line_number(false)
                .with_level(false)
                .with_target(false),
        )
        .with(EnvFilter::from_default_env().add_directive("cockroach_test=info".parse()?))
        .init();

    Ok(())
}

fn calculate_worker_threads(config_threads: Option<usize>) -> usize {
    let available = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(DEFAULT_PARALLELISM);

    let threads = config_threads
        .unwrap_or(available)
        .max(available)
        .max(MIN_WORKER_THREADS);

    if threads < MIN_WORKER_THREADS {
        tracing::warn!(
            threads,
            min = MIN_WORKER_THREADS,
            "worker_threads_below_minimum"
        );
    }

    threads
}
