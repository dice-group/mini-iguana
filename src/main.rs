use anyhow::Context;
use bytes::Bytes;
use clap::Parser;
use reqwest::{blocking::Client, Url};
use std::{
    io::{BufRead, BufWriter, Write},
    path::PathBuf,
    time::{Duration, Instant},
};

#[derive(Parser)]
struct Opts {
    endpoint: Url,
    update_query_file: PathBuf,

    #[clap(long)]
    timeout_secs: Option<u64>,
}

fn main() -> anyhow::Result<()> {
    let Opts { endpoint, update_query_file, timeout_secs } = Opts::parse();

    tracing_subscriber::fmt().with_writer(std::io::stderr).init();

    let mut builder = Client::builder().tcp_nodelay(true);

    if let Some(timeout_secs) = timeout_secs {
        builder = builder.timeout(Duration::from_secs(timeout_secs));
    }

    let client = builder.build().unwrap();

    let mut f =
        std::io::BufReader::new(std::fs::File::open(update_query_file).context("Unable to open query file")?).lines();

    let mut stdout = BufWriter::new(std::io::stdout().lock());
    writeln!(stdout, "query_id,runtime_secs").context("Unable to write to stdout")?;

    let mut id = 0;
    while let Some(Ok(query)) = f.next() {
        let start_time = Instant::now();

        match run_query(&client, &endpoint, query) {
            Ok(resp) => {
                std::hint::black_box(resp);

                let end_time = Instant::now();
                let runtime_secs = end_time.duration_since(start_time).as_secs_f64();

                writeln!(stdout, "{id},{runtime_secs}").context("Unable to write to stdout")?;
            },
            Err(e) => {
                tracing::warn!("HTTP request failed: {e:#?}");
                writeln!(stdout, "{id},{}", f64::INFINITY).context("Unable to write to stdout")?;
            },
        }

        id += 1;
    }

    Ok(())
}

fn run_query(client: &Client, endpoint: &Url, query: String) -> anyhow::Result<Bytes> {
    client
        .post(endpoint.clone())
        .header("Content-Type", "application/sparql-update")
        .body(query)
        .send()
        .context("Error sending HTTP request")?
        .error_for_status()
        .context("Received error HTTP response")?
        .bytes()
        .context("Unable to receive HTTP response body")
}
