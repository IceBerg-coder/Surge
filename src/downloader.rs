use anyhow::{anyhow, Context, Result};
use futures_util::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::Client;
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
}

struct DownloadTarget {
    url: String,
    path: PathBuf,
}

struct Chunk {
    id: usize,
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
    ) -> Result<Self> {
        let mut targets = Vec::new();
        let is_video_platform = url.contains("youtube.com")
            || url.contains("youtu.be")
            || url.contains("facebook.com")
            || url.contains("instagram.com")
            || url.contains("tiktok.com");

        let base_name = output.clone().unwrap_or_else(|| "download".to_string());
        let mut final_output_path = PathBuf::from(&base_name);

        if is_video_platform {
            println!("🔍 Video platform detected. Resolving best available streams...");
            let mut cmd = std::process::Command::new("yt-dlp");
            cmd.arg("-g")
                .arg("-f")
                .arg(format_spec.unwrap_or_else(|| "bestvideo+bestaudio/best".to_string()))
                .arg(&url);

            if let Some(ref path) = cookie_path {
                cmd.arg("--cookies").arg(path);
            }

            if let Ok(out) = cmd.output() {
                if out.status.success() {
                    let full_out = String::from_utf8(out.stdout)?;
                    let lines: Vec<&str> = full_out.lines().filter(|l| !l.is_empty()).collect();

                    if lines.len() >= 2 {
                        println!("💎 Dual-stream detected (Video + Audio) for maximum quality.");
                        targets.push(DownloadTarget {
                            url: lines[0].trim().to_string(),
                            path: PathBuf::from(format!("{}.video.tmp", base_name)),
                        });
                        targets.push(DownloadTarget {
                            url: lines[1].trim().to_string(),
                            path: PathBuf::from(format!("{}.audio.tmp", base_name)),
                        });
                    } else if lines.len() == 1 {
                        println!("✨ Combined stream resolved successfully!");
                        targets.push(DownloadTarget {
                            url: lines[0].trim().to_string(),
                            path: PathBuf::from(&base_name),
                        });
                    }
                }
            }
        }

        if targets.is_empty() {
            let parsed_url = Url::parse(&url).context("Failed to parse URL")?;
            let filename = output.clone().unwrap_or_else(|| {
                parsed_url
                    .path_segments()
                    .and_then(|s| s.last())
                    .filter(|s| !s.is_empty())
                    .unwrap_or("download")
                    .to_string()
            });
            final_output_path = PathBuf::from(&filename);
            targets.push(DownloadTarget {
                url: url.clone(),
                path: final_output_path.clone(),
            });
        }

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

        let client = Client::builder()
            .tcp_nodelay(true)
            .pool_max_idle_per_host(concurrency)
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            .danger_accept_invalid_certs(insecure)
            .default_headers(headers.clone())
            .build()?;

        Ok(Self {
            url: url.clone(),
            targets,
            concurrency,
            client,
            headers,
            final_output: final_output_path,
        })
    }

    pub async fn run(&self) -> Result<()> {
        if self.targets.is_empty() {
            println!("⚠️ Falling back to native yt-dlp download...");
            let status = std::process::Command::new("yt-dlp")
                .arg("-o")
                .arg(&self.final_output)
                .arg(&self.url)
                .status()?;

            if status.success() {
                println!("\n✅ Download complete via yt-dlp: {:?}", self.final_output);
                return Ok(());
            }
            return Err(anyhow!("yt-dlp fallback failed"));
        }

        println!(
            "🚀 Surge: Starting download of {} target(s)",
            self.targets.len()
        );
        // ... rest of the run method
        let mut total_size = 0;
        let mut all_chunks = Vec::new();

        for (idx, target) in self.targets.iter().enumerate() {
            println!(
                "🚀 Surge: Inspecting target {}/{}...",
                idx + 1,
                self.targets.len()
            );
            let (size, accept_ranges) = self.inspect_url(&target.url).await?;
            if size == 0 {
                continue;
            }

            total_size += size;
            println!("📦 Size: {} bytes", size);

            self.preallocate_path(&target.path, size).await?;

            let chunks = if accept_ranges {
                self.partition_for_target(size, idx)
            } else {
                vec![Chunk {
                    id: 0,
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

        self.download_all_chunks(all_chunks, total_size).await?;

        if self.targets.len() > 1 {
            self.merge_streams().await?;
        }

        println!("\n✅ Download complete: {:?}", self.final_output);
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
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            unsafe {
                libc::posix_fallocate(file.as_raw_fd(), 0, size as libc::off_t);
            }
        }
        Ok(())
    }

    fn partition_for_target(&self, size: u64, target_idx: usize) -> Vec<Chunk> {
        let num_chunks = (self.concurrency * 8).max(64);
        let chunk_size = size / num_chunks as u64;
        let mut chunks = Vec::new();
        for i in 0..num_chunks {
            let start = i as u64 * chunk_size;
            let end = if i == num_chunks - 1 {
                size - 1
            } else {
                (i as u64 + 1) * chunk_size - 1
            };
            chunks.push(Chunk {
                id: i,
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
        let mut workers = Vec::new();

        for worker_id in 0..self.concurrency {
            let (chunks, client, targets, main_pb, headers) = (
                Arc::clone(&chunks),
                Arc::clone(&client),
                Arc::clone(&targets),
                main_pb.clone(),
                Arc::clone(&headers),
            );
            workers.push(tokio::spawn(async move {
                loop {
                    let chunk = {
                        let mut q = chunks.lock().await;
                        if q.is_empty() {
                            break;
                        }
                        q.remove(0)
                    };
                    let (target_url, target_path) = &targets[chunk.target_idx];
                    let mut current_pos = chunk.start;
                    let mut retries = 3;
                    while current_pos <= chunk.end && retries > 0 {
                        let res_result = client
                            .get(target_url)
                            .headers((*headers).clone())
                            .header(
                                reqwest::header::RANGE,
                                format!("bytes={}-{}", current_pos, chunk.end),
                            )
                            .send()
                            .await;

                        match res_result {
                            Ok(res) if res.status().is_success() => {
                                let mut stream = res.bytes_stream();
                                let file = OpenOptions::new().write(true).open(target_path).await?;
                                let mut buf = tokio::io::BufWriter::with_capacity(512 * 1024, file);
                                buf.seek(std::io::SeekFrom::Start(current_pos)).await?;
                                let mut batch = 0u64;
                                while let Some(item) = stream.next().await {
                                    let data = item?;
                                    buf.write_all(&data).await?;
                                    current_pos += data.len() as u64;
                                    batch += data.len() as u64;
                                    if batch >= 2 * 1024 * 1024 {
                                        main_pb.inc(batch);
                                        batch = 0;
                                    }
                                }
                                main_pb.inc(batch);
                                buf.flush().await?;
                                if current_pos > chunk.end {
                                    break;
                                }
                            }
                            _ => {
                                retries -= 1;
                                if retries == 0 {
                                    let err_msg = match res_result {
                                        Err(e) => format!("{}", e),
                                        Ok(r) => format!("HTTP {}", r.status()),
                                    };
                                    return Err(anyhow!(
                                        "Worker {} failed: {}",
                                        worker_id,
                                        err_msg
                                    ));
                                }
                                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                            }
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            }));
        }
        for w in workers {
            w.await??;
        }
        let duration = start_time.elapsed();
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
