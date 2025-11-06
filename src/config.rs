use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SingleInsertTest {
    pub enabled: bool,
    pub workers: usize,
    pub batch_size: u32,
    pub duration_secs: u64,
    pub metrics_interval_secs: u64,
    pub timeout_in_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SingleInsertSuiteConfig {
    pub tests: Vec<SingleInsertTest>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub database: DatabaseConfig,
    pub single_insert: SingleInsertSuiteConfig,
    pub runtime_threads: Option<usize>,
}

impl Config {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)?;
        let mut cfg: Config = toml::from_str(&s)?;

        // Override with RUN_TEST environment variable if present
        // Example: RUN_TEST='{ enabled = true, workers = 5, batch_size = 1, duration_secs = 5, metrics_interval_secs = 1, timeout_in_ms = 250 }'
        if let Ok(run_test) = std::env::var("RUN_TEST") {
            cfg = Self::apply_run_test_override(cfg, &run_test)?;
        }

        Ok(cfg)
    }

    fn apply_run_test_override(mut cfg: Config, run_test_str: &str) -> anyhow::Result<Self> {
        // Parse the RUN_TEST as TOML key-value pairs (newline or comma-separated)
        // Normalize: replace commas with newlines for TOML parsing
        let normalized = run_test_str.replace(", ", "\n").replace(",", "\n");

        let test: SingleInsertTest = toml::from_str(&normalized)
            .map_err(|e| anyhow::anyhow!("Failed to parse RUN_TEST environment variable: {}. Expected format: 'enabled = true, workers = 5, batch_size = 1, duration_secs = 5, metrics_interval_secs = 1, timeout_in_ms = 250, warm_up_secs = 10'", e))?;

        // Use eprintln since tracing not initialized yet
        eprintln!("🔧 RUN_TEST environment variable detected - overriding config.toml tests");
        eprintln!(
            "   Workers: {}, Batch Size: {}, Duration: {}s",
            test.workers, test.batch_size, test.duration_secs
        );

        // Replace all tests with this single test from environment
        cfg.single_insert.tests = vec![test];

        Ok(cfg)
    }
}
