# Surge ⚡

Surge is a high-performance, concurrent CLI downloader written in Rust. It is designed to maximize network bandwidth by splitting files into multiple segments and downloading them simultaneously using HTTP Range requests.

## Key Features
- **Concurrent Downloading:** Spawns multiple worker threads to fetch file chunks in parallel.
- **Work-Stealing Queue:** Uses a dynamic queue of 256+ chunks, ensuring fast connections never sit idle.
- **Auto-Merging:** Automatically detects dual audio/video streams (4K/1080p) and stitches them using `ffmpeg`.
- **Intra-chunk Resumption:** If a connection drops, it resumes exactly where it left off, avoiding redundant downloads.
- **OS-Level Optimization:** Uses `fallocate` on Linux to pre-allocate disk space, preventing fragmentation and write speed drops.
- **Universal Compatibility:** Can use `yt-dlp` to resolve hidden stream links from YouTube, Facebook, and more.

## Installation

### Prerequisites
- [Rust & Cargo](https://rustup.rs/)
- [ffmpeg](https://ffmpeg.org/) (for video merging)
- [yt-dlp](https://github.com/yt-dlp/yt-dlp) (for video platform support)

### Build and Install
```bash
# Clone the repository and build
git clone <repo-url>
cd Surge
cargo build --release

# Install to system path
sudo cp target/release/surge /usr/local/bin/
```

### Bulk Download
```bash
surge -i links.txt -c 32
```

### Verified Download
```bash
surge <URL> --sha256 <HASH>
```

## Options
- `-c, --concurrency <N>`: Number of concurrent connections (default: 8).
- `-i, --input-file <PATH>`: Batch download URLs from a text file.
- `-o, --output <PATH>`: Specify output filename/path.
- `--sha256 <HASH>`: Verify file integrity with SHA256.
- `--md5 <HASH>`: Verify file integrity with MD5.
- `-H, --headers <KEY:VAL>`: Add custom HTTP headers.
- `-b, --cookie <COOKIE>`: Pass a session cookie string.
- `-e, --referer <URL>`: Set a custom referer URL.
- `-f, --format <FMT>`: Specify video format (passed to `yt-dlp`).
- `--insecure`: Allow invalid/expired SSL certificates.


## Performance Tuning
Surge is tuned for Gbps speeds. For the absolute maximum throughput on a 1Gbps connection, use higher concurrency:
```bash
surge <URL> -c 64
```

## License
MIT
