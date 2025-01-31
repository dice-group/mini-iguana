use anyhow::Context;
use clap::Parser;
use reqwest::{blocking::Client, Url};
use std::{
    fmt::Display,
    fs::File,
    io,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    time::{Duration, Instant},
};

#[derive(Parser)]
struct Opts {
    endpoint: Url,
    query_file: PathBuf,

    #[clap(long)]
    timeout_secs: Option<u64>,

    #[clap(long)]
    qlever_access_token: Option<String>,

    #[clap(subcommand)]
    mode: Mode,
}

#[derive(Parser)]
enum Mode {
    Warmup,
    Update,
}

fn main() -> anyhow::Result<()> {
    let Opts { endpoint, query_file, timeout_secs, qlever_access_token, mode } = Opts::parse();

    tracing_subscriber::fmt().with_writer(std::io::stderr).init();

    let client = {
        let mut builder = Client::builder().tcp_nodelay(true);

        if let Some(timeout_secs) = timeout_secs {
            builder = builder.timeout(Duration::from_secs(timeout_secs));
        } else {
            builder = builder.timeout(None);
        }

        builder.build().unwrap()
    };

    let input = BufReader::new(File::open(query_file).context("Unable to open query file")?);

    match mode {
        Mode::Warmup => warmup(client, endpoint, input),
        Mode::Update => update(client, endpoint, input, qlever_access_token),
    }
}

fn warmup(client: Client, endpoint: Url, input: impl BufRead) -> anyhow::Result<()> {
    for (id, query) in input.lines().enumerate() {
        let query = query.context("Unable to read from query file")?;

        if let Err(e) = run_query(&client, &endpoint, query) {
            tracing::warn!("query {id} failed: {e:#?}");
        }
    }

    Ok(())
}

fn update(client: Client, mut endpoint: Url, input: impl BufRead, access_token: Option<String>) -> anyhow::Result<()> {
    let mut output = csv::Writer::from_writer(io::stdout().lock());
    let mut output_buf = Vec::new();

    output
        .write_record(&["query_id", "runtime_secs", "error"])
        .context("Unable to write to stdout")?;

    if let Some(token) = access_token {
        endpoint.query_pairs_mut().append_pair("access-token", &token);
    }

    for (id, query) in input.lines().enumerate() {
        let query = query.context("Unable to read from query file")?;

        let start_time = Instant::now();

        match run_update(&client, &endpoint, query) {
            Ok(_) => {
                let end_time = Instant::now();
                let runtime_secs = end_time.duration_since(start_time).as_secs_f64();

                serialize_field(&mut output_buf, &mut output, id)?;
                serialize_field(&mut output_buf, &mut output, runtime_secs)?;
                output.write_field("")?;
                output.write_record(None::<&[u8]>)?;
            },
            Err(e) => {
                tracing::warn!("query {id} failed: {e:#?}");

                serialize_field(&mut output_buf, &mut output, id)?;
                serialize_field(&mut output_buf, &mut output, f64::INFINITY)?;
                alt_serialize_field(&mut output_buf, &mut output, e)?;
                output.write_record(None::<&[u8]>)?;
            }
        }
    }

    Ok(())
}

struct NullWriter;

impl Write for NullWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(std::hint::black_box(buf).len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn run_query(client: &Client, endpoint: &Url, query: String) -> anyhow::Result<()> {
    client
        .get(endpoint.clone())
        .header("Content-Type", "application/sparql-query")
        .query(&[("query", query)])
        .send()
        .context("Error sending HTTP request")?
        .error_for_status()
        .context("Received error HTTP response")?
        .copy_to(&mut NullWriter)
        .context("Unable to receive HTTP response body")?;

    Ok(())
}

fn run_update(client: &Client, endpoint: &Url, query: String) -> anyhow::Result<()> {
    let resp = client
        .post(endpoint.clone())
        .header("Content-Type", "application/sparql-update")
        .body(query)
        .send()
        .context("Error sending HTTP request")?
        .error_for_status()
        .context("Received error HTTP response")?
        .bytes()
        .context("Unable to receive HTTP response body")?;

    std::hint::black_box(resp);
    Ok(())
}

fn serialize_field<T: Display, W: Write>(buf: &mut Vec<u8>, w: &mut csv::Writer<W>, item: T) -> io::Result<()> {
    buf.clear();
    write!(buf, "{item}")?;
    w.write_field(&buf)?;
    Ok(())
}

fn alt_serialize_field<T: Display, W: Write>(buf: &mut Vec<u8>, w: &mut csv::Writer<W>, item: T) -> io::Result<()> {
    buf.clear();
    write!(buf, "{item:#}")?;
    w.write_field(&buf)?;
    Ok(())
}
