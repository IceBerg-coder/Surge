use anyhow::{anyhow, Context, Result};
use futures_util::StreamExt;
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::Client;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use url::Url;

pub struct Downloader {
    url: String,
    targets: Vec<DownloadTarget>,
    concurrency: usize,
    client: Client,
    headers: reqwest::header::HeaderMap,
    final_output: PathBuf,
    handled_by_fallback: bool,
    sha256: Option<String>,
    md5: Option<String>,
    limiter: Option<Arc<DefaultDirectRateLimiter>>,
    auto_concurrency: bool,
}

struct DownloadTarget {
    url: String,
    path: PathBuf,
}

struct Chunk {
    start: u64,
    end: u64,
    target_idx: usize,
}

impl Downloader {
    pub fn new(
        url: String,
        output: Option<String>,
        concurrency: usize,
        insecure: bool,
        custom_headers: Vec<String>,
        cookie: Option<String>,
        referer: Option<String>,
        format_spec: Option<String>,
        cookie_path: Option<String>,
        sha256: Option<String>,
        md5: Option<String>,
        limit_rate: Option<String>,
        auto_concurrency: bool,
    ) -> Result<Self> {
        let is_video_platform = url.contains("youtube.com")
            || url.contains("youtu.be")
            || url.contains("facebook.com")
            || url.contains("instagram.com")
            || url.contains("tiktok.com");
        let parsed_url = Url::parse(&url).context("Failed to parse URL")?;
        let detected_filename = parsed_url
            .path_segments()
            .and_then(|s| s.last())
            .filter(|s| !s.is_empty())
            .unwrap_or("download");
        let final_output_path =
            PathBuf::from(output.unwrap_or_else(|| detected_filename.to_string()));
        let mut targets = Vec::new();

        if is_video_platform {
            println!("🔍 Video platform detected. Using native yt-dlp for reliable download...");
            let mut cmd = std::process::Command::new("yt-dlp");
            cmd.arg("-f")
                .arg(format_spec.unwrap_or_else(|| "bestvideo+bestaudio/best".to_string()))
                .arg("-o")
                .arg(&final_output_path)
                .arg(&url);
            if let Some(path) = cookie_path {
                cmd.arg("--cookies").arg(path);
            } else {
                cmd.arg("--cookies-from-browser").arg("chrome");
            }
            if cmd.status()?.success() {
                println!("\n✅ Download complete via yt-dlp: {:?}", final_output_path);
                return Ok(Self {
                    url,
                    targets,
                    concurrency,
                    client: Client::new(),
                    headers: reqwest::header::HeaderMap::new(),
                    final_output: final_output_path,
                    handled_by_fallback: true,
                    sha256,
                    md5,
                    limiter: None,
                    auto_concurrency: false,
                });
            }
        }

        targets.push(DownloadTarget {
            url: url.clone(),
            path: final_output_path.clone(),
        });
        let mut headers = reqwest::header::HeaderMap::new();
        for h in custom_headers {
            let parts: Vec<&str> = h.splitn(2, ':').collect();
            if parts.len() == 2 {
                let name = reqwest::header::HeaderName::from_bytes(parts[0].trim().as_bytes())?;
                let value = reqwest::header::HeaderValue::from_str(parts[1].trim())?;
                headers.insert(name, value);
            }
        }
        if let Some(c) = cookie {
            headers.insert(
                reqwest::header::COOKIE,
                reqwest::header::HeaderValue::from_str(&c)?,
            );
        }
        if let Some(r) = referer {
            headers.insert(
                reqwest::header::REFERER,
                reqwest::header::HeaderValue::from_str(&r)?,
            );
        }

        let pool_size = if auto_concurrency {
            128
        } else {
            concurrency.max(32)
        };
        let client = Client::builder()
            .tcp_nodelay(true)
            .pool_max_idle_per_host(pool_size)
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
            .danger_accept_invalid_certs(insecure)
            .default_headers(headers.clone())
            .build()?;

        let limiter = if let Some(rate_str) = limit_rate {
            let rate = parse_rate(&rate_str)?;
            println!("⏳ Bandwidth limited to {}/s", rate_str);
            Some(Arc::new(RateLimiter::direct(Quota::per_second(
                NonZeroU32::new(rate as u32).unwrap(),
            ))))
        } else {
            None
        };

        Ok(Self {
            url,
            targets,
            concurrency,
            client,
            headers,
            final_output: final_output_path,
            handled_by_fallback: false,
            sha256,
            md5,
            limiter,
            auto_concurrency,
        })
    }

    pub async fn run(&self) -> Result<()> {
        if self.handled_by_fallback {
            if self.sha256.is_some() || self.md5.is_some() {
                self.verify_integrity().await?;
            }
            return Ok(());
        }

        println!(
            "🚀 Surge: Starting download of {} target(s)",
            self.targets.len()
        );
        let mut total_size = 0;
        let mut all_chunks = Vec::new();

        for (idx, target) in self.targets.iter().enumerate() {
            println!(
                "🚀 Surge: Inspecting target {}/{}...",
                idx + 1,
                self.targets.len()
            );
            let (size, accept_ranges) = self.inspect_url(&target.url).await?;
            total_size += size;
            println!("📦 Size: {} bytes", size);
            self.preallocate_path(&target.path, size).await?;
            let chunks = if accept_ranges {
                self.partition_for_target(size, idx)
            } else {
                vec![Chunk {
                    start: 0,
                    end: size.saturating_sub(1),
                    target_idx: idx,
                }]
            };
            all_chunks.extend(chunks);
        }

        if all_chunks.is_empty() {
            return Err(anyhow!("Nothing to download."));
        }

        match self.download_all_chunks(all_chunks, total_size).await {
            Ok(_) => {}
            Err(e) if e.to_string() == "401" => {
                println!("\n⚠️ 401 Unauthorized detected. Falling back to native yt-dlp...");
                let status = std::process::Command::new("yt-dlp")
                    .arg("-o")
                    .arg(self.final_output.as_path())
                    .arg(self.url.as_str())
                    .status()?;
                if !status.success() {
                    return Err(anyhow!("yt-dlp fallback failed"));
                }
                println!("\n✅ Download complete via yt-dlp.");
                if self.sha256.is_some() || self.md5.is_some() {
                    self.verify_integrity().await?;
                }
                return Ok(());
            }
            Err(e) => return Err(e),
        }

        if self.targets.len() > 1 {
            self.merge_streams().await?;
        }
        println!("\n✅ Download complete: {:?}", self.final_output);
        if self.sha256.is_some() || self.md5.is_some() {
            self.verify_integrity().await?;
        }
        Ok(())
    }

    async fn verify_integrity(&self) -> Result<()> {
        println!("🛡️ Verifying file integrity...");
        use sha2::{Digest, Sha256};
        use std::io::Read;
        let mut file = std::fs::File::open(&self.final_output)?;
        let mut buffer = vec![0u8; 1024 * 1024];
        let mut hasher_sha = Sha256::new();
        let mut hasher_md5 = md5::Context::new();
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            if self.sha256.is_some() {
                hasher_sha.update(&buffer[..n]);
            }
            if self.md5.is_some() {
                hasher_md5.consume(&buffer[..n]);
            }
        }
        if let Some(ref expected) = self.sha256 {
            let res = hex::encode(hasher_sha.finalize());
            if res.to_lowercase() == expected.to_lowercase() {
                println!("   ✅ SHA256 Match: {}", res);
            } else {
                println!("   ❌ SHA256 Mismatch!");
                return Err(anyhow!("Integrity check failed: SHA256 mismatch"));
            }
        }
        if let Some(ref expected) = self.md5 {
            let res = format!("{:x}", hasher_md5.compute());
            if res.to_lowercase() == expected.to_lowercase() {
                println!("   ✅ MD5 Match:    {}", res);
            } else {
                println!("   ❌ MD5 Mismatch!");
                return Err(anyhow!("Integrity check failed: MD5 mismatch"));
            }
        }
        Ok(())
    }

    async fn inspect_url(&self, url: &str) -> Result<(u64, bool)> {
        let res = match self.client.head(url).send().await {
            Ok(r) if r.status().is_success() => r,
            _ => {
                self.client
                    .get(url)
                    .header(reqwest::header::RANGE, "bytes=0-0")
                    .send()
                    .await?
            }
        };
        let size = if res.status() == reqwest::StatusCode::PARTIAL_CONTENT {
            res.headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.split('/').last())
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or_else(|| anyhow!("Could not determine size"))?
        } else {
            res.headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or_else(|| anyhow!("Could not determine size"))?
        };
        let accept_ranges = res
            .headers()
            .get(reqwest::header::ACCEPT_RANGES)
            .map(|v| v == "bytes")
            .unwrap_or_else(|| {
                res.status() == reqwest::StatusCode::PARTIAL_CONTENT
                    || res.headers().contains_key(reqwest::header::CONTENT_RANGE)
            });
        Ok((size, accept_ranges))
    }

    async fn preallocate_path(&self, path: &PathBuf, size: u64) -> Result<()> {
        let file = std::fs::File::create(path)?;
        file.set_len(size)?;
        // Only use fallocate for massive files (>1GB) to avoid slow initialization on smaller files
        #[cfg(target_os = "linux")]
        if size > 1024 * 1024 * 1024 {
            use std::os::unix::io::AsRawFd;
            unsafe {
                libc::posix_fallocate(file.as_raw_fd(), 0, size as libc::off_t);
            }
        }
        Ok(())
    }

    fn partition_for_target(&self, size: u64, target_idx: usize) -> Vec<Chunk> {
        let num_chunks = 1024; // Massive chunk count for extreme work-stealing
        let mut chunks = Vec::new();
        let chunk_size = size / num_chunks as u64;
        for i in 0..num_chunks {
            let start = i as u64 * chunk_size;
            let end = if i == num_chunks - 1 {
                size - 1
            } else {
                (i as u64 + 1) * chunk_size - 1
            };
            chunks.push(Chunk {
                start,
                end,
                target_idx,
            });
        }
        chunks
    }

    async fn download_all_chunks(&self, chunks: Vec<Chunk>, total_size: u64) -> Result<()> {
        let start_time = std::time::Instant::now();
        let m = MultiProgress::new();
        let main_pb = m.add(ProgressBar::new(total_size));
        main_pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")?.progress_chars("#>-"));

        let chunks = Arc::new(tokio::sync::Mutex::new(chunks));
        let client = Arc::new(self.client.clone());
        let targets = Arc::new(
            self.targets
                .iter()
                .map(|t| (t.url.clone(), t.path.clone()))
                .collect::<Vec<_>>(),
        );
        let headers = Arc::new(self.headers.clone());
        let limiter = self.limiter.clone();

        let initial_concurrency = if self.auto_concurrency {
            16
        } else {
            self.concurrency
        };
        let active_workers = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let spawn_worker =
            |id: usize,
             chunks: Arc<tokio::sync::Mutex<Vec<Chunk>>>,
             client: Arc<Client>,
             targets: Arc<Vec<(String, PathBuf)>>,
             main_pb: ProgressBar,
             headers: Arc<reqwest::header::HeaderMap>,
             limiter: Option<Arc<DefaultDirectRateLimiter>>| {
                tokio::spawn(async move {
                    loop {
                        let chunk = {
                            let mut q = chunks.lock().await;
                            if q.is_empty() {
                                break;
                            }
                            q.remove(0)
                        };
                        let (url, path) = &targets[chunk.target_idx];
                        let mut pos = chunk.start;
                        let mut retries = 3;
                        while pos <= chunk.end && retries > 0 {
                            match client
                                .get(url)
                                .headers((*headers).clone())
                                .header(
                                    reqwest::header::RANGE,
                                    format!("bytes={}-{}", pos, chunk.end),
                                )
                                .send()
                                .await
                            {
                                Ok(res) if res.status().is_success() => {
                                    let mut stream = res.bytes_stream();
                                    let file = OpenOptions::new().write(true).open(path).await?;
                                    let mut buf =
                                        tokio::io::BufWriter::with_capacity(1024 * 1024, file);
                                    buf.seek(std::io::SeekFrom::Start(pos)).await?;
                                    let mut batch = 0u64;
                                    while let Some(item) = stream.next().await {
                                        let data = item?;
                                        if let Some(ref l) = limiter {
                                            let _ = l
                                                .until_n_ready(
                                                    NonZeroU32::new(data.len() as u32).unwrap(),
                                                )
                                                .await;
                                        }
                                        buf.write_all(&data).await?;
                                        pos += data.len() as u64;
                                        batch += data.len() as u64;
                                        if batch >= 2 * 1024 * 1024 {
                                            main_pb.inc(batch);
                                            batch = 0;
                                        }
                                    }
                                    main_pb.inc(batch);
                                    buf.flush().await?;
                                    if pos > chunk.end {
                                        break;
                                    }
                                }
                                Ok(res) if res.status() == reqwest::StatusCode::UNAUTHORIZED => {
                                    return Err(anyhow!("401"))
                                }
                                _ => {
                                    retries -= 1;
                                    if retries == 0 {
                                        return Err(anyhow!("Worker {} failed", id));
                                    }
                                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                                }
                            }
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                })
            };

        {
            let mut w_list = active_workers.lock().await;
            for i in 0..initial_concurrency {
                w_list.push(spawn_worker(
                    i,
                    chunks.clone(),
                    client.clone(),
                    targets.clone(),
                    main_pb.clone(),
                    headers.clone(),
                    limiter.clone(),
                ));
            }
        }

        if self.auto_concurrency {
            let (c, cl, t, pb, h, l, w) = (
                chunks.clone(),
                client.clone(),
                targets.clone(),
                main_pb.clone(),
                headers.clone(),
                limiter.clone(),
                active_workers.clone(),
            );
            tokio::spawn(async move {
                let mut last_speed = 0.0;
                let mut current_concurrency = initial_concurrency;
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    if c.lock().await.is_empty() {
                        break;
                    }
                    let speed = pb.per_sec();
                    if speed > last_speed * 1.02 && current_concurrency < 128 {
                        let to_add = if speed > last_speed * 1.2 { 16 } else { 4 };
                        let mut w_list = w.lock().await;
                        for _ in 0..to_add {
                            if current_concurrency >= 128 {
                                break;
                            }
                            current_concurrency += 1;
                            w_list.push(spawn_worker(
                                current_concurrency,
                                c.clone(),
                                cl.clone(),
                                t.clone(),
                                pb.clone(),
                                h.clone(),
                                l.clone(),
                            ));
                        }
                    } else if speed > 0.0 && speed < last_speed * 0.85 {
                        break;
                    }
                    last_speed = speed;
                }
            });
        }

        loop {
            let mut finished = false;
            {
                let mut w_list = active_workers.lock().await;
                if w_list.is_empty() {
                    finished = true;
                } else {
                    let mut i = 0;
                    while i < w_list.len() {
                        if w_list[i].is_finished() {
                            let h = w_list.remove(i);
                            h.await??;
                        } else {
                            i += 1;
                        }
                    }
                }
            }
            if finished {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let duration = start_time.elapsed();
        main_pb.finish_and_clear();
        println!(
            "\n🏁 Download Summary:\n   ⏱️  Total time:  {:.2?}\n   🚀 Average speed: {:.2} MiB/s",
            duration,
            (total_size as f64 / 1024.0 / 1024.0) / duration.as_secs_f64()
        );
        Ok(())
    }

    async fn merge_streams(&self) -> Result<()> {
        println!("🎬 Merging streams via ffmpeg...");
        if std::process::Command::new("ffmpeg")
            .arg("-y")
            .arg("-i")
            .arg(&self.targets[0].path)
            .arg("-i")
            .arg(&self.targets[1].path)
            .arg("-c")
            .arg("copy")
            .arg(&self.final_output)
            .status()?
            .success()
        {
            let _ = std::fs::remove_file(&self.targets[0].path);
            let _ = std::fs::remove_file(&self.targets[1].path);
        }
        Ok(())
    }
}

fn parse_rate(s: &str) -> Result<u64> {
    let s = s.trim().to_uppercase();
    if s.ends_with('K') {
        Ok(s[..s.len() - 1]
            .parse::<u64>()
            .context("Invalid rate number")?
            * 1024)
    } else if s.ends_with('M') {
        Ok(s[..s.len() - 1]
            .parse::<u64>()
            .context("Invalid rate number")?
            * 1024
            * 1024)
    } else if s.ends_with('G') {
        Ok(s[..s.len() - 1]
            .parse::<u64>()
            .context("Invalid rate number")?
            * 1024
            * 1024
            * 1024)
    } else {
        Ok(s.parse::<u64>().context("Invalid rate number")?)
    }
}
