use anyhow::Result;
use clap::Parser;
use surge::Downloader;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// URL to download
    url: String,

    /// Output file path
    #[arg(short, long)]
    output: Option<String>,

    /// Number of concurrent segments
    #[arg(short, long, default_value_t = 8)]
    concurrency: usize,

    /// Accept invalid/expired certificates (DANGEROUS)
    #[arg(long)]
    insecure: bool,

    /// Custom HTTP headers (e.g. -H "Authorization: Bearer token")
    #[arg(short = 'H', long)]
    headers: Vec<String>,

    /// Custom cookies (e.g. -b "session=123")
    #[arg(short = 'b', long)]
    cookie: Option<String>,

    /// Custom referer URL
    #[arg(short = 'e', long)]
    referer: Option<String>,

    /// Path to browser cookies file
    #[arg(long)]
    cookies: Option<String>,

    /// Video format selection
    #[arg(short = 'f', long)]
    format: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let downloader = Downloader::new(
        args.url,
        args.output,
        args.concurrency,
        args.insecure,
        args.headers,
        args.cookie,
        args.referer,
        args.format,
        args.cookies,
    )?;
    downloader.run().await?;
    Ok(())
}
