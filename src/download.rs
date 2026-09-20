use reqwest::{
    Client, StatusCode,
    header::{ACCEPT_RANGES, CONTENT_RANGE, ETAG, IF_RANGE, LAST_MODIFIED, RANGE},
};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::io::AsyncWriteExt;

const MAX_ATTEMPTS: usize = 4;
const SEGMENT_THRESHOLD: u64 = 32 * 1024 * 1024;
const MAX_SEGMENTS: usize = 8;
pub const DEFAULT_STREAM_NAME: &str = "MovieBox-Tui_Stream";

pub fn safe_file_stem(value: &str) -> String {
    let mut stem = value
        .chars()
        .take(120)
        .map(|character| {
            if character.is_control()
                || matches!(
                    character,
                    '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|'
                )
            {
                '_'
            } else {
                character
            }
        })
        .collect::<String>();
    stem = stem.trim_matches(['.', ' ', '_']).to_string();
    if stem.is_empty() {
        return DEFAULT_STREAM_NAME.into();
    }
    let upper = stem.to_ascii_uppercase();
    let reserved = matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || upper
            .strip_prefix("COM")
            .or_else(|| upper.strip_prefix("LPT"))
            .is_some_and(|number| {
                number.len() == 1 && number.bytes().all(|byte| matches!(byte, b'1'..=b'9'))
            });
    if reserved {
        stem.push('_');
    }
    stem
}

#[derive(Debug, Clone)]
pub struct DownloadProgress {
    pub downloaded: u64,
    pub total: Option<u64>,
    pub bytes_per_second: f64,
    pub attempt: usize,
    pub workers: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadOutcome {
    Completed { bytes: u64 },
    Paused { bytes: u64 },
}

#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("server returned HTTP {0}")]
    Http(StatusCode),
    #[error("network error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("file error: {0}")]
    File(#[from] std::io::Error),
    #[error("invalid partial response: {0}")]
    InvalidRange(String),
    #[error("download ended at {downloaded} of {expected} bytes")]
    Incomplete { downloaded: u64, expected: u64 },
    #[error("download paused")]
    Paused,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ResumeMetadata {
    etag: Option<String>,
    last_modified: Option<String>,
    total: Option<u64>,
    segments: Option<usize>,
}

pub async fn download<F>(
    client: &Client,
    url: &str,
    destination: &Path,
    cancel: Arc<AtomicBool>,
    mut report: F,
) -> Result<DownloadOutcome, DownloadError>
where
    F: FnMut(DownloadProgress),
{
    let partial = sidecar_path(destination, "part");
    let metadata_path = sidecar_path(destination, "part.json");
    let mut metadata = read_metadata(&metadata_path).await;
    let started = Instant::now();
    let mut last_report = Instant::now() - Duration::from_secs(1);
    let mut last_error = None;
    let mut segmented_disabled = false;

    for attempt in 1..=MAX_ATTEMPTS {
        if cancel.load(Ordering::Relaxed) {
            return Ok(DownloadOutcome::Paused {
                bytes: file_len(&partial).await,
            });
        }

        let mut offset = file_len(&partial).await;
        let mut request = client.get(url);
        if offset > 0 {
            request = request.header(RANGE, format!("bytes={offset}-"));
            if let Some(validator) = metadata.etag.as_ref().or(metadata.last_modified.as_ref()) {
                request = request.header(IF_RANGE, validator);
            }
        }

        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                last_error = Some(DownloadError::Network(error));
                retry_delay(attempt).await;
                continue;
            }
        };

        if response.status() == StatusCode::RANGE_NOT_SATISFIABLE && metadata.total == Some(offset)
        {
            finalize(&partial, &metadata_path, destination).await?;
            return Ok(DownloadOutcome::Completed { bytes: offset });
        }
        if !response.status().is_success() {
            last_error = Some(DownloadError::Http(response.status()));
            retry_delay(attempt).await;
            continue;
        }

        if !segmented_disabled
            && offset == 0
            && response.status() == StatusCode::OK
            && response
                .headers()
                .get(ACCEPT_RANGES)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.eq_ignore_ascii_case("bytes"))
            && response
                .content_length()
                .is_some_and(|total| total >= SEGMENT_THRESHOLD)
        {
            let total = response.content_length().unwrap_or_default();
            let segments = segment_count(total);
            let current_metadata = ResumeMetadata {
                etag: header_string(&response, ETAG),
                last_modified: header_string(&response, LAST_MODIFIED),
                total: Some(total),
                segments: Some(segments),
            };
            if !metadata_matches(&metadata, &current_metadata) {
                remove_segment_files(destination).await;
            }
            write_metadata(&metadata_path, &current_metadata).await?;
            drop(response);
            match download_segmented(
                client,
                url,
                destination,
                &metadata_path,
                current_metadata,
                cancel.clone(),
                &mut report,
            )
            .await
            {
                Err(DownloadError::InvalidRange(_)) => {
                    remove_segment_files(destination).await;
                    metadata = ResumeMetadata::default();
                    write_metadata(&metadata_path, &metadata).await?;
                    segmented_disabled = true;
                    continue;
                }
                result => return result,
            }
        }

        let response_total = if response.status() == StatusCode::PARTIAL_CONTENT {
            let content_range = response
                .headers()
                .get(CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| DownloadError::InvalidRange("Content-Range missing".into()))?;
            let (start, total) = parse_content_range(content_range)?;
            if start != offset {
                return Err(DownloadError::InvalidRange(format!(
                    "requested byte {offset}, received {start}"
                )));
            }
            total
        } else {
            if offset > 0 {
                truncate(&partial).await?;
                offset = 0;
            }
            response.content_length()
        };

        if offset > 0
            && let (Some(previous), Some(current)) = (metadata.total, response_total)
            && previous != current
        {
            truncate(&partial).await?;
            metadata = ResumeMetadata::default();
            write_metadata(&metadata_path, &metadata).await?;
            last_error = Some(DownloadError::InvalidRange(
                "remote file size changed; partial reset".into(),
            ));
            retry_delay(attempt).await;
            continue;
        }

        metadata.etag = header_string(&response, ETAG).or(metadata.etag);
        metadata.last_modified = header_string(&response, LAST_MODIFIED).or(metadata.last_modified);
        metadata.total = response_total.or(metadata.total);
        metadata.segments = None;
        write_metadata(&metadata_path, &metadata).await?;

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&partial)
            .await?;
        let mut response = response;
        let mut downloaded = offset;
        let transfer_started = Instant::now();

        loop {
            if cancel.load(Ordering::Relaxed) {
                file.flush().await?;
                return Ok(DownloadOutcome::Paused { bytes: downloaded });
            }

            match tokio::time::timeout(Duration::from_secs(30), response.chunk()).await {
                Ok(Ok(Some(chunk))) => {
                    file.write_all(&chunk).await?;
                    downloaded += chunk.len() as u64;
                    if last_report.elapsed() >= Duration::from_millis(200) {
                        let elapsed = started.elapsed().as_secs_f64();
                        report(DownloadProgress {
                            downloaded,
                            total: metadata.total,
                            bytes_per_second: if elapsed > 0.0 {
                                downloaded.saturating_sub(offset) as f64 / elapsed
                            } else {
                                0.0
                            },
                            attempt,
                            workers: 1,
                        });
                        last_report = Instant::now();
                    }
                }
                Ok(Ok(None)) => {
                    file.flush().await?;
                    file.sync_data().await?;
                    if let Some(expected) = metadata.total
                        && downloaded != expected
                    {
                        last_error = Some(DownloadError::Incomplete {
                            downloaded,
                            expected,
                        });
                        break;
                    }
                    report(DownloadProgress {
                        downloaded,
                        total: metadata.total.or(Some(downloaded)),
                        bytes_per_second: if transfer_started.elapsed().as_secs_f64() > 0.0 {
                            downloaded.saturating_sub(offset) as f64
                                / transfer_started.elapsed().as_secs_f64()
                        } else {
                            0.0
                        },
                        attempt,
                        workers: 1,
                    });
                    finalize(&partial, &metadata_path, destination).await?;
                    return Ok(DownloadOutcome::Completed { bytes: downloaded });
                }
                Ok(Err(error)) => {
                    file.flush().await?;
                    last_error = Some(DownloadError::Network(error));
                    break;
                }
                Err(_) => {
                    file.flush().await?;
                    last_error = Some(DownloadError::InvalidRange("read timeout".into()));
                    break;
                }
            }
        }

        retry_delay(attempt).await;
    }

    Err(last_error.unwrap_or(DownloadError::InvalidRange(
        "download failed without a response".into(),
    )))
}

async fn download_segmented<F>(
    client: &Client,
    url: &str,
    destination: &Path,
    metadata_path: &Path,
    metadata: ResumeMetadata,
    cancel: Arc<AtomicBool>,
    report: &mut F,
) -> Result<DownloadOutcome, DownloadError>
where
    F: FnMut(DownloadProgress),
{
    let total = metadata
        .total
        .ok_or_else(|| DownloadError::InvalidRange("segment total missing".into()))?;
    let segments = metadata.segments.unwrap_or_else(|| segment_count(total));
    let ranges = segment_ranges(total, segments);
    let mut initial = 0;

    for (index, (start, end)) in ranges.iter().copied().enumerate() {
        let path = segment_path(destination, index);
        let expected = end - start + 1;
        let length = file_len(&path).await;
        if length > expected {
            truncate(&path).await?;
        } else {
            initial += length;
        }
    }

    let downloaded = Arc::new(AtomicU64::new(initial));
    let (progress_sender, mut progress_receiver) = tokio::sync::mpsc::channel::<(u64, usize)>(64);
    let mut tasks = tokio::task::JoinSet::new();
    let validator = metadata.etag.clone().or(metadata.last_modified.clone());

    for (index, (start, end)) in ranges.iter().copied().enumerate() {
        let client = client.clone();
        let url = url.to_string();
        let path = segment_path(destination, index);
        let cancel = cancel.clone();
        let progress_sender = progress_sender.clone();
        let validator = validator.clone();
        tasks.spawn(async move {
            download_segment(
                &client,
                &url,
                &path,
                start,
                end,
                total,
                validator,
                cancel,
                progress_sender,
            )
            .await
        });
    }
    drop(progress_sender);

    let started = Instant::now();
    let mut last_report = Instant::now() - Duration::from_secs(1);
    let mut finished = 0;
    while finished < segments {
        tokio::select! {
            progress = progress_receiver.recv() => {
                if let Some((bytes, attempt)) = progress {
                    let current = downloaded.fetch_add(bytes, Ordering::Relaxed) + bytes;
                    if last_report.elapsed() >= Duration::from_millis(200) {
                        let elapsed = started.elapsed().as_secs_f64();
                        report(DownloadProgress {
                            downloaded: current,
                            total: Some(total),
                            bytes_per_second: if elapsed > 0.0 {
                                current.saturating_sub(initial) as f64 / elapsed
                            } else {
                                0.0
                            },
                            attempt,
                            workers: segments,
                        });
                        last_report = Instant::now();
                    }
                }
            }
            result = tasks.join_next() => {
                match result {
                    Some(Ok(Ok(()))) => finished += 1,
                    Some(Ok(Err(DownloadError::Paused))) => {
                        tasks.abort_all();
                        return Ok(DownloadOutcome::Paused {
                            bytes: downloaded.load(Ordering::Relaxed),
                        });
                    }
                    Some(Ok(Err(error))) => {
                        tasks.abort_all();
                        return Err(error);
                    }
                    Some(Err(error)) => {
                        tasks.abort_all();
                        return Err(DownloadError::InvalidRange(format!(
                            "download worker stopped: {error}"
                        )));
                    }
                    None => break,
                }
            }
        }
    }

    if cancel.load(Ordering::Relaxed) {
        return Ok(DownloadOutcome::Paused {
            bytes: downloaded.load(Ordering::Relaxed),
        });
    }

    let assembly = sidecar_path(destination, "assembling");
    let mut output = tokio::fs::File::create(&assembly).await?;
    let mut copy_buffer = vec![0u8; 256 * 1024];
    for index in 0..segments {
        let path = segment_path(destination, index);
        let mut part = tokio::fs::File::open(&path).await?;
        loop {
            let n = tokio::io::AsyncReadExt::read(&mut part, &mut copy_buffer).await?;
            if n == 0 {
                break;
            }
            tokio::io::AsyncWriteExt::write_all(&mut output, &copy_buffer[..n]).await?;
        }
    }
    output.flush().await?;
    output.sync_data().await?;
    drop(output);
    if file_len(&assembly).await != total {
        return Err(DownloadError::Incomplete {
            downloaded: file_len(&assembly).await,
            expected: total,
        });
    }
    if destination.exists() {
        let _ = tokio::fs::remove_file(destination).await;
    }
    if let Err(e) = tokio::fs::rename(&assembly, destination).await {
        let _ = tokio::fs::remove_file(destination).await;
        tokio::fs::rename(&assembly, destination)
            .await
            .map_err(|e2| {
                DownloadError::File(std::io::Error::other(format!(
                    "failed to move assembled file to destination: {e} ({e2})"
                )))
            })?;
    }
    for index in 0..segments {
        let _ = tokio::fs::remove_file(segment_path(destination, index)).await;
    }
    let _ = tokio::fs::remove_file(metadata_path).await;
    report(DownloadProgress {
        downloaded: total,
        total: Some(total),
        bytes_per_second: if started.elapsed().as_secs_f64() > 0.0 {
            total.saturating_sub(initial) as f64 / started.elapsed().as_secs_f64()
        } else {
            0.0
        },
        attempt: 1,
        workers: segments,
    });
    Ok(DownloadOutcome::Completed { bytes: total })
}

#[allow(clippy::too_many_arguments)]
async fn download_segment(
    client: &Client,
    url: &str,
    path: &Path,
    start: u64,
    end: u64,
    total: u64,
    validator: Option<String>,
    cancel: Arc<AtomicBool>,
    progress: tokio::sync::mpsc::Sender<(u64, usize)>,
) -> Result<(), DownloadError> {
    let expected = end - start + 1;
    let mut last_error = None;

    for attempt in 1..=MAX_ATTEMPTS {
        if cancel.load(Ordering::Relaxed) {
            return Err(DownloadError::Paused);
        }
        let existing = file_len(path).await.min(expected);
        if existing == expected {
            return Ok(());
        }
        let requested_start = start + existing;
        let mut request = client
            .get(url)
            .header(RANGE, format!("bytes={requested_start}-{end}"));
        if let Some(validator) = &validator {
            request = request.header(IF_RANGE, validator);
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                last_error = Some(DownloadError::Network(error));
                retry_delay(attempt).await;
                continue;
            }
        };
        if response.status() != StatusCode::PARTIAL_CONTENT {
            last_error = Some(DownloadError::InvalidRange(format!(
                "worker expected HTTP 206, received {}",
                response.status()
            )));
            retry_delay(attempt).await;
            continue;
        }
        let content_range = response
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| DownloadError::InvalidRange("Content-Range missing".into()))?;
        let (received_start, received_total) = parse_content_range(content_range)?;
        if received_start != requested_start || received_total != Some(total) {
            return Err(DownloadError::InvalidRange(format!(
                "worker requested {requested_start}-{end}/{total}, received {content_range}"
            )));
        }

        let raw_file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        let mut file = tokio::io::BufWriter::with_capacity(256 * 1024, raw_file);
        let mut response = response;
        let mut written = existing;
        let mut unbatched_bytes = 0u64;
        let mut last_progress_send = Instant::now();
        loop {
            if cancel.load(Ordering::Relaxed) {
                if unbatched_bytes > 0 {
                    let _ = progress.send((unbatched_bytes, attempt)).await;
                }
                file.flush().await?;
                return Err(DownloadError::Paused);
            }
            match tokio::time::timeout(Duration::from_secs(30), response.chunk()).await {
                Ok(Ok(Some(chunk))) => {
                    let remaining = expected - written;
                    let bytes = chunk.len().min(remaining as usize);
                    file.write_all(&chunk[..bytes]).await?;
                    written += bytes as u64;
                    unbatched_bytes += bytes as u64;
                    if unbatched_bytes >= 256 * 1024
                        || last_progress_send.elapsed() >= Duration::from_millis(100)
                    {
                        let _ = progress.send((unbatched_bytes, attempt)).await;
                        unbatched_bytes = 0;
                        last_progress_send = Instant::now();
                    }
                    if written == expected {
                        if unbatched_bytes > 0 {
                            let _ = progress.send((unbatched_bytes, attempt)).await;
                        }
                        file.flush().await?;
                        file.get_mut().sync_data().await?;
                        return Ok(());
                    }
                }
                Ok(Ok(None)) => {
                    if unbatched_bytes > 0 {
                        let _ = progress.send((unbatched_bytes, attempt)).await;
                    }
                    file.flush().await?;
                    last_error = Some(DownloadError::Incomplete {
                        downloaded: written,
                        expected,
                    });
                    break;
                }
                Ok(Err(error)) => {
                    if unbatched_bytes > 0 {
                        let _ = progress.send((unbatched_bytes, attempt)).await;
                    }
                    file.flush().await?;
                    last_error = Some(DownloadError::Network(error));
                    break;
                }
                Err(_) => {
                    if unbatched_bytes > 0 {
                        let _ = progress.send((unbatched_bytes, attempt)).await;
                    }
                    file.flush().await?;
                    last_error = Some(DownloadError::InvalidRange("read timeout".into()));
                    break;
                }
            }
        }
        retry_delay(attempt).await;
    }

    Err(last_error.unwrap_or(DownloadError::Incomplete {
        downloaded: file_len(path).await,
        expected,
    }))
}

fn segment_count(total: u64) -> usize {
    if total < 256 * 1024 * 1024 {
        2
    } else if total < 2 * 1024 * 1024 * 1024 {
        4
    } else {
        MAX_SEGMENTS
    }
}

fn segment_ranges(total: u64, segments: usize) -> Vec<(u64, u64)> {
    let size = total / segments as u64;
    (0..segments)
        .map(|index| {
            let start = index as u64 * size;
            let end = if index + 1 == segments {
                total - 1
            } else {
                start + size - 1
            };
            (start, end)
        })
        .collect()
}

fn segment_path(destination: &Path, index: usize) -> PathBuf {
    sidecar_path(destination, &format!("part.{index}"))
}

fn metadata_matches(previous: &ResumeMetadata, current: &ResumeMetadata) -> bool {
    if previous.total != current.total || previous.segments != current.segments {
        return false;
    }

    if let (Some(a), Some(b)) = (&previous.etag, &current.etag) {
        return a == b;
    }

    match (&previous.last_modified, &current.last_modified) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

async fn remove_segment_files(destination: &Path) {
    for index in 0..MAX_SEGMENTS {
        let _ = tokio::fs::remove_file(segment_path(destination, index)).await;
    }
    let _ = tokio::fs::remove_file(sidecar_path(destination, "assembling")).await;
}

fn parse_content_range(value: &str) -> Result<(u64, Option<u64>), DownloadError> {
    let value = value
        .strip_prefix("bytes ")
        .ok_or_else(|| DownloadError::InvalidRange(value.into()))?;
    let (range, total) = value
        .split_once('/')
        .ok_or_else(|| DownloadError::InvalidRange(value.into()))?;
    let (start, _) = range
        .split_once('-')
        .ok_or_else(|| DownloadError::InvalidRange(value.into()))?;
    let start = start
        .parse()
        .map_err(|_| DownloadError::InvalidRange(value.into()))?;
    let total = if total == "*" {
        None
    } else {
        Some(
            total
                .parse()
                .map_err(|_| DownloadError::InvalidRange(value.into()))?,
        )
    };
    Ok((start, total))
}

pub(crate) fn sidecar_path(destination: &Path, suffix: &str) -> PathBuf {
    let mut name = destination.as_os_str().to_os_string();
    name.push(".");
    name.push(suffix);
    PathBuf::from(name)
}

async fn file_len(path: &Path) -> u64 {
    tokio::fs::metadata(path)
        .await
        .map(|metadata| metadata.len())
        .unwrap_or_default()
}

async fn truncate(path: &Path) -> Result<(), std::io::Error> {
    tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .await
        .map(|_| ())
}

async fn finalize(
    partial: &Path,
    metadata: &Path,
    destination: &Path,
) -> Result<(), std::io::Error> {
    if tokio::fs::rename(partial, destination).await.is_err() {
        let _ = tokio::fs::remove_file(destination).await;
        tokio::fs::rename(partial, destination).await?;
    }
    let _ = tokio::fs::remove_file(metadata).await;
    Ok(())
}

async fn read_metadata(path: &Path) -> ResumeMetadata {
    let Ok(bytes) = tokio::fs::read(path).await else {
        return ResumeMetadata::default();
    };
    serde_json::from_slice(&bytes).unwrap_or_default()
}

async fn write_metadata(path: &Path, metadata: &ResumeMetadata) -> Result<(), std::io::Error> {
    let bytes = serde_json::to_vec(metadata).map_err(std::io::Error::other)?;
    tokio::fs::write(path, bytes).await
}

fn header_string(
    response: &reqwest::Response,
    name: reqwest::header::HeaderName,
) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

async fn retry_delay(attempt: usize) {
    if attempt < MAX_ATTEMPTS {
        tokio::time::sleep(Duration::from_secs(1 << (attempt - 1))).await;
    }
}


pub fn ffmpeg_missing_guidance() -> String {
    if crate::updater::artifact::is_termux_environment() {
        "FFmpeg is required to merge video and audio streams. Please install it on your device (e.g. 'pkg install ffmpeg') to download and merge these streams.".to_string()
    } else if cfg!(target_os = "macos") {
        "FFmpeg is required to merge video and audio streams. Please install it on your Mac (e.g. 'brew install ffmpeg') to download and merge these streams.".to_string()
    } else if cfg!(target_os = "windows") {
        "FFmpeg is required to merge video and audio streams. Please install it on your system (e.g. 'winget install Gyan.FFmpeg') to download and merge these streams.".to_string()
    } else if cfg!(target_os = "linux") {
        "FFmpeg is required to merge video and audio streams. Please install ffmpeg via your system package manager (e.g. 'sudo apt install ffmpeg') to download and merge these streams.".to_string()
    } else {
        "FFmpeg is required to merge video and audio streams. Please install ffmpeg on your system to download and merge these streams.".to_string()
    }
}

pub async fn probe_media_streams(file: &Path) -> (bool, bool) {
    if !file.is_file() {
        return (false, false);
    }

    if let Some(ffprobe_bin) = crate::player::find_ffprobe() {
        let mut cmd = tokio::process::Command::new(ffprobe_bin);
        cmd.args([
            "-v",
            "error",
            "-show_entries",
            "stream=codec_type",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
        ])
        .arg(file);
        #[cfg(target_os = "windows")]
        cmd.creation_flags(crate::player::CREATE_NO_WINDOW);
        if let Ok(output) = cmd.output().await {
            let text = String::from_utf8_lossy(&output.stdout);
            let has_video = text.lines().any(|l| l.trim().eq_ignore_ascii_case("video"));
            let has_audio = text.lines().any(|l| l.trim().eq_ignore_ascii_case("audio"));
            if has_video || has_audio {
                return (has_video, has_audio);
            }
        }
    }

    if let Some(ffmpeg_bin) = crate::player::find_ffmpeg() {
        let mut cmd = tokio::process::Command::new(ffmpeg_bin);
        cmd.args(["-hide_banner", "-i"]).arg(file);
        #[cfg(target_os = "windows")]
        cmd.creation_flags(crate::player::CREATE_NO_WINDOW);
        if let Ok(output) = cmd.output().await {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let has_video = stderr.contains("Video:") || stderr.contains(": Video");
            let has_audio = stderr.contains("Audio:") || stderr.contains(": Audio");
            if has_video || has_audio {
                return (has_video, has_audio);
            }
        }
    }

    let filename = file
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let is_video = filename.contains(".f0")
        || filename.contains("video")
        || filename.ends_with(".mp4")
        || filename.ends_with(".mkv")
        || filename.ends_with(".webm");
    let is_audio = filename.contains(".f3")
        || filename.contains("audio")
        || filename.ends_with(".m4a")
        || filename.ends_with(".aac")
        || filename.ends_with(".opus")
        || filename.ends_with(".mka");
    (is_video, is_audio)
}

pub fn find_split_stream_files(target_dir: &Path, base_name: &str) -> Vec<PathBuf> {
    let mut results = Vec::new();
    let Ok(entries) = std::fs::read_dir(target_dir) else {
        return results;
    };
    let base_lower = base_name.to_lowercase();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        let name_lower = filename.to_lowercase();
        if !name_lower.starts_with(&base_lower) {
            continue;
        }
        if name_lower.ends_with(".srt")
            || name_lower.ends_with(".vtt")
            || name_lower.ends_with(".ass")
            || name_lower.ends_with(".ssa")
            || name_lower.ends_with(".sub")
            || name_lower.ends_with(".json")
            || name_lower.ends_with(".metadata")
            || name_lower.ends_with(".assembling")
            || name_lower.ends_with(".merging.mp4")
        {
            continue;
        }
        results.push(path);
    }
    results
}

pub async fn merge_streams_with_ffmpeg(
    video_file: &Path,
    audio_file: &Path,
    destination: &Path,
) -> Result<(), String> {
    let Some(ffmpeg_bin) = crate::player::find_ffmpeg() else {
        return Err(ffmpeg_missing_guidance());
    };

    let temp_merged = sidecar_path(destination, "merging.mp4");
    if temp_merged.exists() {
        let _ = tokio::fs::remove_file(&temp_merged).await;
    }

    let mut cmd = tokio::process::Command::new(ffmpeg_bin);
    cmd.arg("-y")
        .arg("-i")
        .arg(video_file)
        .arg("-i")
        .arg(audio_file)
        .arg("-map")
        .arg("0:v:0")
        .arg("-map")
        .arg("1:a:0")
        .arg("-c")
        .arg("copy")
        .arg("-movflags")
        .arg("+faststart")
        .arg(&temp_merged);

    #[cfg(target_os = "windows")]
    cmd.creation_flags(crate::player::CREATE_NO_WINDOW);

    let output = cmd
        .output()
        .await
        .map_err(|e| format!("Failed to spawn FFmpeg: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let _ = tokio::fs::remove_file(&temp_merged).await;
        return Err(format!("FFmpeg merge failed: {stderr}"));
    }

    let size = file_len(&temp_merged).await;
    if size == 0 {
        let _ = tokio::fs::remove_file(&temp_merged).await;
        return Err("Merged file is empty (0 bytes)".to_string());
    }

    let (has_video, has_audio) = probe_media_streams(&temp_merged).await;
    if !has_video || !has_audio {
        let _ = tokio::fs::remove_file(&temp_merged).await;
        return Err(format!(
            "Merged file validation failed (has_video={has_video}, has_audio={has_audio})"
        ));
    }

    if tokio::fs::rename(&temp_merged, destination).await.is_err() {
        let _ = tokio::fs::remove_file(destination).await;
        tokio::fs::rename(&temp_merged, destination)
            .await
            .map_err(|e| format!("Failed to move merged file to destination: {e}"))?;
    }

    // Safely remove split files only after successful merge and validation
    let _ = tokio::fs::remove_file(video_file).await;
    let _ = tokio::fs::remove_file(audio_file).await;

    Ok(())
}

pub async fn post_process_download(
    target_dir: &Path,
    base_name: &str,
    destination: &Path,
) -> Result<PathBuf, String> {
    if destination.is_file() && file_len(destination).await > 0 {
        let (v, a) = probe_media_streams(destination).await;
        if v && a {
            for file in find_split_stream_files(target_dir, base_name) {
                if file != destination {
                    let _ = tokio::fs::remove_file(file).await;
                }
            }
            return Ok(destination.to_path_buf());
        }
    }

    let split_files = find_split_stream_files(target_dir, base_name);
    let mut video_candidates = Vec::new();
    let mut audio_candidates = Vec::new();

    for file in split_files {
        let (v, a) = probe_media_streams(&file).await;
        if v && a {
            if file != destination {
                if tokio::fs::rename(&file, destination).await.is_err() {
                    let _ = tokio::fs::remove_file(destination).await;
                    tokio::fs::rename(&file, destination)
                        .await
                        .map_err(|e| format!("Failed to rename valid stream to destination: {e}"))?;
                }
            }
            return Ok(destination.to_path_buf());
        }
        if v {
            video_candidates.push(file.clone());
        }
        if a {
            audio_candidates.push(file);
        }
    }

    if let (Some(video_file), Some(audio_file)) = (video_candidates.first(), audio_candidates.first()) {
        merge_streams_with_ffmpeg(video_file, audio_file, destination).await?;
        return Ok(destination.to_path_buf());
    }

    if destination.is_file() && file_len(destination).await > 0 {
        return Ok(destination.to_path_buf());
    }

    if let Some(video_file) = video_candidates.first() {
        if video_file != destination {
            if tokio::fs::rename(video_file, destination).await.is_err() {
                let _ = tokio::fs::remove_file(destination).await;
                tokio::fs::rename(video_file, destination)
                    .await
                    .map_err(|e| format!("Failed to rename stream file: {e}"))?;
            }
        }
        return Ok(destination.to_path_buf());
    }

    Err(format!(
        "Download output file was not found: {}",
        destination.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_content_range_valid() {
        let (start, total) = parse_content_range("bytes 0-1023/2048").unwrap();
        assert_eq!(start, 0);
        assert_eq!(total, Some(2048));

        let (start, total) = parse_content_range("bytes 500-999/*").unwrap();
        assert_eq!(start, 500);
        assert_eq!(total, None);
    }

    #[test]
    fn test_parse_content_range_invalid() {
        assert!(parse_content_range("invalid range").is_err());
        assert!(parse_content_range("bytes invalid").is_err());
    }

    #[test]
    fn test_segment_ranges_partitioning() {
        let total = 1000;
        let ranges = segment_ranges(total, 4);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0], (0, 249));
        assert_eq!(ranges[1], (250, 499));
        assert_eq!(ranges[2], (500, 749));
        assert_eq!(ranges[3], (750, 999));
    }

    #[test]
    fn test_safe_file_stem_empty_fallback() {
        assert_eq!(safe_file_stem(""), DEFAULT_STREAM_NAME);
        assert_eq!(safe_file_stem("   "), DEFAULT_STREAM_NAME);
        assert_eq!(safe_file_stem("..."), DEFAULT_STREAM_NAME);
        assert_eq!(safe_file_stem("___"), DEFAULT_STREAM_NAME);
    }

    #[test]
    fn test_safe_file_stem_sanitization_and_reserved() {
        assert_eq!(safe_file_stem("../../../etc/passwd"), "etc_passwd");
        assert_eq!(
            safe_file_stem("C:\\Windows\\System32\\calc.exe"),
            "C__Windows_System32_calc.exe"
        );
        assert_eq!(safe_file_stem("CON"), "CON_");
        assert_eq!(safe_file_stem("con"), "con_");
        assert_eq!(safe_file_stem("PRN"), "PRN_");
        assert_eq!(safe_file_stem("AUX"), "AUX_");
        assert_eq!(safe_file_stem("NUL"), "NUL_");
        assert_eq!(safe_file_stem("COM1"), "COM1_");
        assert_eq!(safe_file_stem("LPT9"), "LPT9_");
        assert_eq!(
            safe_file_stem("Normal Title: Special"),
            "Normal Title_ Special"
        );
        assert_eq!(
            safe_file_stem("Movie: The <Ultimate> Edition *?|"),
            "Movie_ The _Ultimate_ Edition"
        );
    }

    #[tokio::test]
    async fn test_finalize_replaces_existing_destination_cleanly() {
        let dir = std::env::temp_dir().join(format!(
            "mbx_test_finalize_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let partial = dir.join("video.mp4.part");
        let metadata = dir.join("video.mp4.metadata");
        let destination = dir.join("video.mp4");

        tokio::fs::write(&destination, b"old version")
            .await
            .unwrap();
        tokio::fs::write(&partial, b"new version").await.unwrap();
        tokio::fs::write(&metadata, b"{}").await.unwrap();

        let res = finalize(&partial, &metadata, &destination).await;
        assert!(res.is_ok());
        assert!(!partial.exists());
        assert!(!metadata.exists());
        assert!(destination.exists());
        assert_eq!(
            tokio::fs::read_to_string(&destination).await.unwrap(),
            "new version"
        );
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[test]
    fn test_ffmpeg_missing_guidance_contains_platform_hint() {
        let guidance = ffmpeg_missing_guidance();
        assert!(guidance.contains("FFmpeg is required"));
    }

    #[test]
    fn test_find_split_stream_files_filters_subtitles_and_metadata() {
        let dir = std::env::temp_dir().join(format!(
            "mbx_test_split_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let base_name = "India's Got Talent - S01E01";
        let vid_file = dir.join(format!("{base_name}.f0"));
        let aud_file = dir.join(format!("{base_name}.f3"));
        let srt_file = dir.join(format!("{base_name}.en.srt"));
        let json_file = dir.join(format!("{base_name}.mp4.part.json"));

        std::fs::write(&vid_file, b"video_data").unwrap();
        std::fs::write(&aud_file, b"audio_data").unwrap();
        std::fs::write(&srt_file, b"1\n00:00:00 --> 00:00:01\nHi").unwrap();
        std::fs::write(&json_file, b"{}").unwrap();

        let split = find_split_stream_files(&dir, base_name);
        assert_eq!(split.len(), 2);
        assert!(split.contains(&vid_file));
        assert!(split.contains(&aud_file));
        assert!(!split.contains(&srt_file));
        assert!(!split.contains(&json_file));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_ffmpeg_merge_streams_integration() {
        let Some(ffmpeg_bin) = crate::player::find_ffmpeg() else {
            return;
        };

        let dir = std::env::temp_dir().join(format!(
            "mbx_test_merge_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        tokio::fs::create_dir_all(&dir).await.unwrap();

        let base_name = "India's Got Talent - S01E01";
        let vid_file = dir.join(format!("{base_name}.f0.mp4"));
        let aud_file = dir.join(format!("{base_name}.f3.m4a"));
        let dest_file = dir.join(format!("{base_name}.mp4"));

        // Generate 1s test video stream
        let mut v_cmd = tokio::process::Command::new(&ffmpeg_bin);
        v_cmd.args(["-y", "-f", "lavfi", "-i", "testsrc=duration=1:size=320x240:rate=10", "-c:v", "libx264", "-pix_fmt", "yuv420p"])
            .arg(&vid_file);
        #[cfg(target_os = "windows")]
        v_cmd.creation_flags(crate::player::CREATE_NO_WINDOW);
        assert!(v_cmd.output().await.unwrap().status.success());

        // Generate 1s test audio stream
        let mut a_cmd = tokio::process::Command::new(&ffmpeg_bin);
        a_cmd.args(["-y", "-f", "lavfi", "-i", "sine=frequency=1000:duration=1", "-c:a", "aac"])
            .arg(&aud_file);
        #[cfg(target_os = "windows")]
        a_cmd.creation_flags(crate::player::CREATE_NO_WINDOW);
        assert!(a_cmd.output().await.unwrap().status.success());

        assert!(vid_file.exists());
        assert!(aud_file.exists());

        let res = post_process_download(&dir, base_name, &dest_file).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), dest_file);
        assert!(dest_file.exists());

        let (has_v, has_a) = probe_media_streams(&dest_file).await;
        assert!(has_v, "Merged file must have video stream");
        assert!(has_a, "Merged file must have audio stream");

        assert!(!vid_file.exists(), "Temporary video format file must be cleaned up");
        assert!(!aud_file.exists(), "Temporary audio format file must be cleaned up");

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
