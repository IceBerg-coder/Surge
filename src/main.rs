use anyhow::Result;
use clap::Parser;
use surge::Downloader;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// URL to download (optional if input-file is provided)
    url: Option<String>,

    /// Path to a text file containing URLs to download
    #[arg(short = 'i', long)]
    input_file: Option<String>,

    /// Output file path (ignored in batch mode)
    #[arg(short, long)]
    output: Option<String>,

    /// Number of concurrent segments
    #[arg(short, long, default_value_t = 8)]
    concurrency: usize,

    /// Automatically find the best concurrency (Smart Scaling)
    #[arg(long)]
    auto: bool,

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

    /// Expected SHA256 checksum for verification
    #[arg(long)]
    sha256: Option<String>,

    /// Expected MD5 checksum for verification
    #[arg(long)]
    md5: Option<String>,

    /// Limit download speed (e.g. 10M, 500K)
    #[arg(long)]
    limit_rate: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let mut urls = Vec::new();
    if let Some(url) = args.url {
        urls.push(url);
    }

    if let Some(path) = args.input_file {
        let content = std::fs::read_to_string(path)?;
        for line in content.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                urls.push(trimmed.to_string());
            }
        }
    }

    if urls.is_empty() {
        println!("❌ Error: No URLs provided. Use 'surge <URL>' or 'surge -i links.txt'");
        std::process::exit(1);
    }

    for (idx, url) in urls.iter().enumerate() {
        if urls.len() > 1 {
            println!("\n📦 Batch Download {}/{}: {}", idx + 1, urls.len(), url);
        }

        let downloader = Downloader::new(
            url.clone(),
            args.output.clone(),
            args.concurrency,
            args.insecure,
            args.headers.clone(),
            args.cookie.clone(),
            args.referer.clone(),
            args.format.clone(),
            args.cookies.clone(),
            args.sha256.clone(),
            args.md5.clone(),
            args.limit_rate.clone(),
            args.auto,
        )?;
        downloader.run().await?;
    }
    Ok(())
}
