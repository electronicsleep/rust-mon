// Author: https://github.com/electronicsleep
// Purpose: RustMon - monitor servers using prometheus metrics
// Released under the MIT License

use anyhow::{Context, Result};
use chrono::Local;
use clap::Parser;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    net::TcpListener,
    sync::{Arc},
    thread,
    time::{Duration, Instant},
};
use gethostname::gethostname;


#[derive(Parser, Debug)]
#[command(name = "rustmon", about = "Server monitoring tool")]
struct Cli {
    /// Enable verbose output
    #[arg(short = 'v', long, default_value_t = false)]
    verbose: bool,

    /// Run a single check then exit
    #[arg(short = 's', long, default_value_t = false)]
    single: bool,

    /// Response-time threshold in milliseconds before an alert is raised
    #[arg(short = 't', long, default_value_t = 500)]
    threshold: u64,

    /// Start the built-in web server (logs + metrics endpoints)
    #[arg(short = 'w', long, default_value_t = false)]
    webserver: bool,

    /// Update the default sleep interval
    #[arg(short = 'i', long, default_value_t = 60)]
    sleep_interval: u64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Config {
    #[serde(default)]
    slack_url: String,
    #[serde(default)]
    slack_msg: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    servers: Vec<String>,
}

impl Config {
    fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {path}"))?;
        let config: Config =
            serde_yaml::from_str(&content).context("Failed to parse config.yaml")?;
        Ok(config)
    }
}

#[derive(Debug, Default, Clone)]
struct State {
    error_num: u32,
    run_num: u32,
}

impl State {
    fn new() -> Self {
        State {
            error_num: 0,
            run_num: 0,
        }
    }

    fn increment_error(&mut self) {
        self.error_num += 1;
        info!("State error_num: {}", self.error_num);
    }
}

const LOG_FILE: &str = "rustmon.log";
const METRICS_FILE: &str = "rustmon_metrics.log";

fn log_output(prefix: &str, msg: &str) {
    for line in msg.lines() {
        info!("{} {}", prefix, line);
    }
}

fn write_metrics(msg: &str, append: bool) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(!append)
        .append(append)
        .open(METRICS_FILE)
        .context("Failed to open metrics file")?;
    writeln!(file, "{msg}").context("Failed to write metrics")?;
    Ok(())
}

fn read_last_lines(path: &str, max: usize) -> Result<Vec<String>> {
    let file = File::open(path).with_context(|| format!("Cannot open {path}"))?;
    let reader = BufReader::new(file);
    let mut lines: Vec<String> = Vec::new();
    for line in reader.lines() {
        lines.push(line?);
        if lines.len() > max {
            lines.remove(0);
        }
    }
    Ok(lines)
}

fn is_connected(client: &reqwest::blocking::Client) -> bool {
    client
        .get("http://clients3.google.com/generate_204")
        .timeout(Duration::from_secs(5))
        .send()
        .is_ok()
}

fn post_slack(client: &reqwest::blocking::Client, config: &Config, message: &str) {
    if config.slack_url.is_empty() {
        info!("Slack URL not configured: no message sent");
        return;
    }

    let send_text = format!("{}: {}", message, config.slack_msg);

    let escaped = send_text.replace('\\', "\\\\").replace('"', "\\\"");
    let body = format!("{{\"text\": \"{escaped}\"}}");

    if !is_connected(client) {
        error!("No network connection — cannot post to Slack");
        return;
    }

    match client
        .post(&config.slack_url)
        .header("Content-Type", "application/json; charset=UTF-8")
        .body(body)
        .send()
    {
        Ok(resp) => info!("Slack response: {}", resp.status()),
        Err(e) => error!("Slack post failed: {e}"),
    }
}

fn post_message(client: &reqwest::blocking::Client, config: &Config, message: &str) {
    info!("Posting message: {message}");
    post_slack(client, config, message);
}

const CHECK_NUM: usize = 3;
const ERROR_NUM_ALERT: u32 = 2;

fn check_sites(
    client: &reqwest::blocking::Client,
    config: &Config,
    mut state: State,
    threshold_ms: u64,
    sleep_interval: u64,
    verbose: bool,
) -> State {
    let urls = &config.servers;

    println!("DEBUG: check_sites");
    if verbose {
        info!("URLs to check: {:?}", urls);
    }

    info!("state.error_num: {}", state.error_num);
    info!("state.run_num:   {}", state.run_num);

    let mut threshold_reached_num: u32 = 0;

    for i in 0..CHECK_NUM {
        println!("DEBUG: check_num");
        info!("--- Check pass {} of {} ---", i + 1, CHECK_NUM);

        for (n, url) in urls.iter().enumerate() {
            let ts_start = Local::now().format("%Y.%m.%d-%H.%M.%S%.3f").to_string();
            let wall_start = Instant::now();

            info!("Checking URL #{n}: {url}");

            match client.get(url).send() {
                Err(e) => {
                    state.increment_error();
                    log_output("ERROR:", &format!("{url} error: {e}"));
                    println!("DEBUG: check_sites: error");
                    error!(
                        "error_num: {} alert_threshold: {} err: {e}",
                        state.error_num, ERROR_NUM_ALERT
                    );
                    if state.error_num >= ERROR_NUM_ALERT {
                        post_message(
                            client,
                            config,
                            &format!(
                                "ALERT: error with site over {} errors: {} count: {} Date: {}",
                                ERROR_NUM_ALERT, url, state.error_num, ts_start
                            ),
                        );
                    }
                }
                Ok(resp) => {
                    println!("DEBUG: check_sites ok");
                    let status = resp.status();
                    let _ = resp.bytes();
                    log_output("INFO: client ok", &format!("{url} status={status}"));
                    info!("client ok  url={url}  status={status}");
                }
            }

            let duration_ms = wall_start.elapsed().as_millis() as u64;
            let ts_end = Local::now().format("%Y.%m.%d-%H.%M.%S%.3f").to_string();

            info!(
                "url={url}  duration={}ms  threshold={}ms",
                duration_ms, threshold_ms
            );

            if duration_ms > threshold_ms {
                threshold_reached_num += 1;
                warn!(
                    "OVER threshold  duration={}ms  threshold_reached={}",
                    duration_ms, threshold_reached_num
                );
                if threshold_reached_num > 1 {
                    post_message(
                        client,
                        config,
                        &format!(
                            "ALERT: site over threshold: {url} duration={}ms Date: {ts_start}",
                            duration_ms
                        ),
                    );
                }
            }

            let metric_key = url
                .replace(':', "_")
                .replace('/', "_")
                .replace('.', "_");
            let metric_line = format!("{metric_key} {duration_ms}");

            if n == 0 {
                // First URL in this pass: overwrite (reset metrics file)
                if let Err(e) = write_metrics("", false) {
                    error!("Failed to reset metrics file: {e}");
                }
            }
            if let Err(e) = write_metrics(&metric_line, true) {
                error!("Failed to append metric: {e}");
            }

            info!("time_start: {ts_start}");
            info!("time_end:   {ts_end}");
        }

        if i + 1 < CHECK_NUM {
            let sleep_interval_duration: Duration = Duration::from_secs(sleep_interval);

            info!("Sleeping {}s before next pass: ", sleep_interval);
            println!("DEBUG: Sleeping {}s: ", sleep_interval);
            thread::sleep(sleep_interval_duration);
            println!("DEBUG: Sleeping finished");
        }
    }

    state.run_num += 1;
    state
}

fn handle_connection(mut stream: std::net::TcpStream) {
    use std::io::Read;

    let mut buf = [0u8; 4096];
    let mut content = "plain";
    let _ = stream.read(&mut buf);
    let request = String::from_utf8_lossy(&buf);
    let first_line = request.lines().next().unwrap_or("");

    let (status, body) = if first_line.contains("GET /metrics") {
        match read_last_lines(METRICS_FILE, 20) {
            Ok(lines) => ("200 OK", lines.join("\n")),
            Err(e) => ("500 Internal Server Error", e.to_string()),
        }
    } else if first_line.contains("GET /logs") {
        match read_last_lines(LOG_FILE, 20) {
            Ok(lines) => ("200 OK", lines.join("\n")),
            Err(e) => ("500 Internal Server Error", e.to_string()),
        }
    } else {
        content = "html";
        (
            "200 OK",
            "<html><body>\
             <h2>rustmon</h2>\
             <ul>\
             <li><a href='/logs'>/logs</a></li>\
             <li><a href='/metrics'>/metrics</a></li>\
             </ul>\
             </body></html>"
                .to_string(),
        )
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Type: text/{content}\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
}

fn run_webserver(addr: &str) {
    let listener = TcpListener::bind(addr).expect("Failed to bind web server");
    info!("Web server listening on http://{addr}/logs and http://{addr}/metrics");
    println!("INFO: Web server listening on http://{addr}/logs and http://{addr}/metrics");
    for stream in listener.incoming().flatten() {
        handle_connection(stream);
    }
}

fn run_monitor(
    client: Arc<reqwest::blocking::Client>,
    config: Arc<Config>,
    threshold_ms: u64,
    sleep_interval :u64,
    verbose: bool,
    single: bool,
) {
    let mut state = State::new();
    println!("DEBUG: Starting in run_montior");

    if single {
        println!("DEBUG: Single run mode");
        info!("Single run mode");
        check_sites(&client, &config, state, threshold_ms, sleep_interval, verbose);
        info!("Single run complete: exiting");
        std::process::exit(0)
    } else {
        println!("DEBUG: run_monitor loop run mode");
        info!("Loop run mode");
        let mut loop_count: u64 = 0;
        loop {
            loop_count += 1;
            info!("--- Monitor loop #{loop_count} ---");
            println!("DEBUG: check_sites");
            state = check_sites(&client, &config, state, threshold_ms, sleep_interval, verbose);
            println!("DEBUG: LOOP: Sleeping");
            info!("Sleeping {}s until next loop", sleep_interval);

            let sleep_interval_duration: Duration = Duration::from_secs(sleep_interval);

            thread::sleep(sleep_interval_duration);
        }
    }
}

fn main() -> Result<()> {
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(LOG_FILE)
        .context("Cannot open rustmon.log")?;

    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .target(env_logger::Target::Pipe(Box::new(log_file)))
        .init();

    let cli = Cli::parse();

    let config = Arc::new(Config::load("config.yaml")?);
    let client = Arc::new(
        reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("Failed to build HTTP client")?,
    );

    let hostname_osstring = gethostname();
    let os_hostname = hostname_osstring.into_string();
    println!("Hostname: {:?}", os_hostname);
    println!("INFO: Starting rustmon hostname={:?}", os_hostname);
    info!("Starting rustmon hostname={:?}", os_hostname);

    post_message(&client, &config, &format!("INFO: Starting rustmon hostname: {:?}", os_hostname));

    if cli.verbose {
        println!("verbose:   {}", cli.verbose);
        println!("single:    {}", cli.single);
        println!("threshold: {}ms", cli.threshold);
        println!("webserver: {}", cli.webserver);
    }

    if cli.webserver {
        let client2 = Arc::clone(&client);
        let config2 = Arc::clone(&config);
        let threshold = cli.threshold;
        let sleep_interval = cli.sleep_interval;
        let verbose = cli.verbose;
        let single = cli.single;

        thread::spawn(move || {
            run_monitor(client2, config2, threshold, sleep_interval, verbose, single);
        });

        println!("INFO: Starting run webserver");
        run_webserver("0.0.0.0:8080");
    } else {
        println!("INFO: Starting run run_montior");
        run_monitor(client, config, cli.threshold, cli.sleep_interval, cli.verbose, cli.single);
    }

    Ok(())
}
