use anyhow::Context;
use clap::Parser;
use reqwest::{Client, Url};
use std::{
    io::Write,
    path::PathBuf,
    time::{Duration, Instant},
};
use bytes::Bytes;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
};
use tokio::io::BufWriter;

#[derive(Parser)]
struct Opts {
    endpoint: Url,
    update_query_file: PathBuf,

    #[clap(long)]
    timeout_secs: Option<u64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Opts { endpoint, update_query_file, timeout_secs } = Opts::parse();

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .finish();

    let mut builder = Client::builder().tcp_nodelay(true);

    if let Some(timeout_secs) = timeout_secs {
        builder = builder.timeout(Duration::from_secs(timeout_secs));
    }

    let client = builder.build().unwrap();

    let mut f = BufReader::new(
        File::open(update_query_file)
            .await
            .context("Unable to open query file")?,
    )
    .lines();

    let mut stdout = BufWriter::new(tokio::io::stdout());
    stdout
        .write_all(b"query_id,runtime_secs")
        .await
        .context("Cannot write to stdout")?;

    let mut output_buf = Vec::new();

    let mut id = 0;
    while let Some(query) = f.next_line().await.context("Unable to read query file")? {
        let start_time = Instant::now();

        match run_query(&client, &endpoint, query).await {
            Ok(resp) => {
                std::hint::black_box(resp);

                let end_time = Instant::now();
                let runtime_secs = end_time.duration_since(start_time).as_secs_f64();

                output_buf.clear();
                writeln!(output_buf, "{id},{runtime_secs}").unwrap();
            },
            Err(e) => {
                tracing::warn!("HTTP request failed: {e:#?}");

                output_buf.clear();
                writeln!(output_buf, "{id},{}", f64::INFINITY).unwrap();
            }
        }

        stdout.write_all(&output_buf).await.context("Cannot write to stdout")?;
        id += 1;
    }

    Ok(())
}

async fn run_query(client: &Client, endpoint: &Url, query: String) -> anyhow::Result<Bytes> {
    client
        .post(endpoint.clone())
        .header("Content-Type", "application/sparql-update")
        .body(query)
        .send()
        .await
        .context("Error sending HTTP request")?
        .error_for_status()
        .context("Received error HTTP response")?
        .bytes()
        .await
        .context("Unable to receive HTTP response body")
}
