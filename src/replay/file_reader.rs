use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc, Timelike};
use std::collections::BinaryHeap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use zstd::stream::read::Decoder;

use super::config::ReplayConfig;
use super::gcs_downloader::GcsDownloader;
use super::message_parser::LogMessage;

pub struct HistoricalDataReader {
    config: ReplayConfig,
}

impl HistoricalDataReader {
    pub fn new(config: ReplayConfig) -> Self {
        Self { config }
    }

    /// Discover all relevant data files for the replay time range
    pub fn discover_files(&self) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();

        // Check current log file
        let current_file = self.config.data_directory.join("current/log.jsonl.zst");
        if current_file.exists() {
            files.push(current_file);
        }

        // Discover historical files with timestamp-based names
        let data_dir = &self.config.data_directory;
        if data_dir.exists() {
            for entry in fs::read_dir(data_dir)? {
                let entry = entry?;
                let path = entry.path();
                
                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    // Match pattern: YYYY-MM-DD-HH.jsonl.zst
                    if file_name.ends_with(".jsonl.zst") && 
                       file_name.len() == "YYYY-MM-DD-HH.jsonl.zst".len() &&
                       file_name.chars().nth(4) == Some('-') &&
                       file_name.chars().nth(7) == Some('-') &&
                       file_name.chars().nth(10) == Some('-') {
                        
                        // Parse timestamp from filename to check if it's in range
                        if let Ok(file_time) = self.parse_filename_timestamp(file_name) {
                            // Include file if it could contain messages in our time range
                            // (file represents hour starting at file_time)
                            let file_end = file_time + chrono::Duration::hours(1);
                            if file_end > self.config.start_timestamp && 
                               file_time < self.config.end_timestamp {
                                files.push(path);
                            }
                        }
                    }
                }
            }
        }

        // Sort files by timestamp for ordered processing
        files.sort();
        Ok(files)
    }

    /// Parse timestamp from filename format YYYY-MM-DD-HH.jsonl.zst
    fn parse_filename_timestamp(&self, filename: &str) -> Result<DateTime<Utc>> {
        let timestamp_part = &filename[..13]; // "YYYY-MM-DD-HH"
        let _datetime_str = format!("{}:00:00Z", timestamp_part);
        let formatted = format!("{}-{}-{}T{}:00:00Z",
            &timestamp_part[0..4],   // year
            &timestamp_part[5..7],   // month  
            &timestamp_part[8..10],  // day
            &timestamp_part[11..13]  // hour
        );
        
        DateTime::parse_from_rfc3339(&formatted)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| anyhow!("Failed to parse timestamp from filename {}: {}", filename, e))
    }

    /// Create a streaming iterator over messages from a single compressed file
    pub fn stream_file_messages(&self, file_path: PathBuf) -> Result<FileMessageIterator> {
        FileMessageIterator::new(file_path, &self.config)
    }

    /// Create a streaming iterator over all messages in chronological order
    pub fn stream_all_messages(&self) -> Result<ChronologicalMessageIterator> {
        let files = if self.config.download_from_gcs {
            self.discover_files_with_gcs_cache()?
        } else {
            self.discover_files()?
        };
        
        ChronologicalMessageIterator::new(files, &self.config)
    }

    /// Create a streaming iterator with optional GCS downloading
    pub async fn stream_all_messages_async(&self) -> Result<ChronologicalMessageIterator> {
        // Download from GCS if requested
        if self.config.download_from_gcs {
            self.download_from_gcs().await?;
        }

        // Use streaming method after downloading
        self.stream_all_messages()
    }

    /// Download required files from GCS
    async fn download_from_gcs(&self) -> Result<()> {
        let cache_dir = self.config.gcs_cache_directory.clone()
            .unwrap_or_else(|| self.config.data_directory.join("gcs_cache"));

        let downloader = if let Some(creds_path) = &self.config.gcs_credentials_path {
            GcsDownloader::new_with_credentials(cache_dir, creds_path.clone()).await?
        } else {
            GcsDownloader::new(cache_dir).await?
        };

        println!("Downloading files from GCS for time range {} to {}", 
                 self.config.start_timestamp.format("%Y-%m-%d %H:%M:%S UTC"),
                 self.config.end_timestamp.format("%Y-%m-%d %H:%M:%S UTC"));

        let downloaded_files = downloader
            .download_for_time_range(self.config.start_timestamp, self.config.end_timestamp)
            .await?;

        println!("Downloaded {} files from GCS", downloaded_files.len());

        // Update data directory to point to downloaded files if we have any
        if !downloaded_files.is_empty() {
            // We'll update the search to include the cache directory
            // This is handled in discover_files_with_gcs_cache below
        }

        Ok(())
    }

    /// Discover files including GCS cache directory
    pub fn discover_files_with_gcs_cache(&self) -> Result<Vec<PathBuf>> {
        let mut files = self.discover_files()?;

        // Also check GCS cache directory if it exists
        if let Some(cache_dir) = &self.config.gcs_cache_directory {
            if cache_dir.exists() {
                let cache_files = self.discover_files_in_directory(cache_dir)?;
                files.extend(cache_files);
            }
        }

        // Remove duplicates and sort
        files.sort();
        files.dedup();

        Ok(files)
    }

    /// Discover files in a specific directory
    fn discover_files_in_directory(&self, directory: &Path) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();

        if directory.exists() {
            for entry in fs::read_dir(directory)? {
                let entry = entry?;
                let path = entry.path();
                
                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    // Match pattern: YYYY-MM-DD-HH.jsonl.zst
                    if file_name.ends_with(".jsonl.zst") && 
                       file_name.len() == "YYYY-MM-DD-HH.jsonl.zst".len() &&
                       file_name.chars().nth(4) == Some('-') &&
                       file_name.chars().nth(7) == Some('-') &&
                       file_name.chars().nth(10) == Some('-') {
                        
                        // Parse timestamp from filename to check if it's in range
                        if let Ok(file_time) = self.parse_filename_timestamp(file_name) {
                            // Include file if it could contain messages in our time range
                            let file_end = file_time + chrono::Duration::hours(1);
                            if file_end > self.config.start_timestamp && 
                               file_time < self.config.end_timestamp {
                                files.push(path);
                            }
                        }
                    }
                }
            }
        }

        Ok(files)
    }

    /// Analyze local file coverage and detect missing time ranges
    pub fn analyze_coverage(&self) -> Result<CoverageAnalysis> {
        let local_files = self.discover_files()?;
        let mut covered_hours = std::collections::HashSet::new();

        // Track which hours are covered by local files
        for file_path in &local_files {
            if let Some(file_name) = file_path.file_name().and_then(|n| n.to_str()) {
                if file_name == "log.jsonl.zst" {
                    // Current log file - need to check its actual time range
                    if let Ok(time_range) = self.get_file_time_range(file_path) {
                        let mut current = time_range.0.with_minute(0).unwrap().with_second(0).unwrap().with_nanosecond(0).unwrap();
                        while current <= time_range.1 {
                            covered_hours.insert(current);
                            current += chrono::Duration::hours(1);
                        }
                    }
                } else if let Ok(file_time) = self.parse_filename_timestamp(file_name) {
                    covered_hours.insert(file_time);
                }
            }
        }

        // Generate required hours for the time range
        let mut required_hours = Vec::new();
        let mut current = self.config.start_timestamp.with_minute(0).unwrap().with_second(0).unwrap().with_nanosecond(0).unwrap();
        while current < self.config.end_timestamp {
            required_hours.push(current);
            current += chrono::Duration::hours(1);
        }

        // Find missing hours
        let missing_hours: Vec<DateTime<Utc>> = required_hours
            .into_iter()
            .filter(|hour| !covered_hours.contains(hour))
            .collect();

        let coverage_ratio = if covered_hours.is_empty() {
            0.0
        } else {
            let total_required = (self.config.end_timestamp - self.config.start_timestamp).num_hours() as f64;
            let covered_count = covered_hours.len() as f64;
            (covered_count / total_required).min(1.0)
        };

        let needs_download = !missing_hours.is_empty();
        Ok(CoverageAnalysis {
            local_files,
            missing_hours,
            coverage_ratio,
            needs_download,
        })
    }

    /// Get the actual time range covered by a file by reading its contents
    fn get_file_time_range(&self, file_path: &Path) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
        let file = std::fs::File::open(file_path)?;
        let decoder = Decoder::new(file)?;
        let reader = BufReader::new(decoder);
        
        let mut first_timestamp: Option<DateTime<Utc>> = None;
        let mut last_timestamp: Option<DateTime<Utc>> = None;

        for line_result in reader.lines() {
            let line = line_result?;
            if line.trim().is_empty() {
                continue;
            }

            if let Ok(message) = LogMessage::from_json_line(&line) {
                let ts = message.get_timestamp();
                if first_timestamp.is_none() {
                    first_timestamp = Some(ts);
                }
                last_timestamp = Some(ts);
            }
        }

        match (first_timestamp, last_timestamp) {
            (Some(first), Some(last)) => Ok((first, last)),
            _ => Err(anyhow!("Could not determine time range for file: {:?}", file_path)),
        }
    }

    /// Legacy method: Get all messages in time order (kept for backward compatibility)
    /// Warning: This loads all messages into memory and may consume large amounts of RAM
    #[deprecated(since = "0.2.0", note = "Use stream_all_messages_with_auto_download for memory efficiency")]
    pub async fn get_all_messages_with_auto_download(&self) -> Result<Vec<LogMessage>> {
        let iterator = self.stream_all_messages_with_auto_download().await?;
        iterator.collect::<Result<Vec<_>>>()
    }

    /// Create a streaming iterator with automatic download detection
    pub async fn stream_all_messages_with_auto_download(&self) -> Result<ChronologicalMessageIterator> {
        // Check if auto-download is disabled
        if self.config.no_auto_download {
            return self.stream_all_messages_async().await;
        }

        // Analyze coverage first
        let analysis = self.analyze_coverage()?;
        
        if analysis.needs_download {
            println!("Coverage analysis: {:.1}% of requested time range is available locally", 
                     analysis.coverage_ratio * 100.0);
            println!("Missing {} hours of data. Downloading from GCS...", analysis.missing_hours.len());
            
            // Enable GCS download for missing data
            let mut enhanced_config = self.config.clone();
            if !enhanced_config.download_from_gcs {
                let cache_dir = enhanced_config.gcs_cache_directory.clone()
                    .unwrap_or_else(|| enhanced_config.data_directory.join("gcs_cache"));
                enhanced_config.download_from_gcs = true;
                enhanced_config.gcs_cache_directory = Some(cache_dir);
            }

            let enhanced_reader = HistoricalDataReader::new(enhanced_config);
            enhanced_reader.download_from_gcs().await?;
            enhanced_reader.stream_all_messages()
        } else {
            println!("All requested data is available locally (coverage: {:.1}%)", 
                     analysis.coverage_ratio * 100.0);
            self.stream_all_messages()
        }
    }
}

/// Analysis of local file coverage for a time range
#[derive(Debug)]
pub struct CoverageAnalysis {
    pub local_files: Vec<PathBuf>,
    pub missing_hours: Vec<DateTime<Utc>>,
    pub coverage_ratio: f64,
    pub needs_download: bool,
}

/// Iterator over messages from a single compressed file
/// 
/// This iterator lazily decompresses and parses messages from a zstd-compressed log file,
/// applying time range and market filters as configured. Memory usage is proportional
/// to the compression buffer size rather than the total file size.
pub struct FileMessageIterator {
    lines_iter: Box<dyn Iterator<Item = std::io::Result<String>>>,
    config: ReplayConfig,
    file_path: PathBuf,
}

impl FileMessageIterator {
    fn new(file_path: PathBuf, config: &ReplayConfig) -> Result<Self> {
        let file = std::fs::File::open(&file_path)
            .map_err(|e| anyhow!("Failed to open file {:?}: {}", file_path, e))?;
        let decoder = Decoder::new(file)
            .map_err(|e| anyhow!("Failed to create zstd decoder for {:?}: {}", file_path, e))?;
        let buf_reader = BufReader::new(decoder);
        let lines_iter = Box::new(buf_reader.lines());
        
        Ok(Self {
            lines_iter,
            config: config.clone(),
            file_path,
        })
    }
}

impl Iterator for FileMessageIterator {
    type Item = Result<LogMessage>;
    
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.lines_iter.next() {
                Some(Ok(line)) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    
                    match LogMessage::from_json_line(&line) {
                        Ok(message) => {
                            // Check if message is in time range
                            if self.config.is_in_range(&message.get_timestamp()) {
                                // Check market filter if applicable
                                match message.get_market_ids() {
                                    Ok(market_ids) => {
                                        if market_ids.is_empty() || 
                                           market_ids.iter().any(|id| self.config.should_include_market(id)) {
                                            return Some(Ok(message));
                                        }
                                    }
                                    Err(_) => {
                                        // Non-market messages (shutdown, connections, etc.)
                                        return Some(Ok(message));
                                    }
                                }
                            }
                            // Message filtered out, continue to next
                        }
                        Err(e) => {
                            eprintln!("Warning: Failed to parse log line in {:?}: {}", self.file_path, e);
                            // Continue to next line instead of returning error
                        }
                    }
                }
                Some(Err(e)) => {
                    return Some(Err(anyhow!("IO error reading from {:?}: {}", self.file_path, e)));
                }
                None => return None,
            }
        }
    }
}

/// Helper struct for merge sort heap
#[derive(Debug)]
struct FileMessageWithIndex {
    message: LogMessage,
    file_index: usize,
}

impl PartialEq for FileMessageWithIndex {
    fn eq(&self, other: &Self) -> bool {
        self.message.get_timestamp() == other.message.get_timestamp()
    }
}

impl Eq for FileMessageWithIndex {}

impl PartialOrd for FileMessageWithIndex {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FileMessageWithIndex {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse comparison for min-heap behavior
        other.message.get_timestamp().cmp(&self.message.get_timestamp())
            .then_with(|| other.file_index.cmp(&self.file_index))
    }
}

/// Iterator that merges messages from multiple files in chronological order
///
/// Uses a min-heap to efficiently merge sorted message streams from multiple files,
/// ensuring chronological output while maintaining constant memory usage regardless
/// of the number or size of input files.
pub struct ChronologicalMessageIterator {
    iterators: Vec<FileMessageIterator>,
    heap: BinaryHeap<FileMessageWithIndex>,
}

impl ChronologicalMessageIterator {
    fn new(file_paths: Vec<PathBuf>, config: &ReplayConfig) -> Result<Self> {
        let mut iterators = Vec::new();
        let mut heap = BinaryHeap::new();
        
        // Create iterators for each file
        for (file_index, file_path) in file_paths.into_iter().enumerate() {
            match FileMessageIterator::new(file_path.clone(), config) {
                Ok(mut iterator) => {
                    // Prime the heap with the first message from each file
                    if let Some(result) = iterator.next() {
                        match result {
                            Ok(message) => {
                                heap.push(FileMessageWithIndex {
                                    message,
                                    file_index,
                                });
                            }
                            Err(e) => {
                                eprintln!("Warning: Error reading first message from {:?}: {}", file_path, e);
                            }
                        }
                    }
                    iterators.push(iterator);
                }
                Err(e) => {
                    eprintln!("Warning: Failed to create iterator for {:?}: {}", file_path, e);
                }
            }
        }
        
        Ok(Self {
            iterators,
            heap,
        })
    }
}

impl Iterator for ChronologicalMessageIterator {
    type Item = Result<LogMessage>;
    
    fn next(&mut self) -> Option<Self::Item> {
        // Get the earliest message from the heap
        if let Some(FileMessageWithIndex { message, file_index }) = self.heap.pop() {
            // Try to get the next message from the same file
            if let Some(result) = self.iterators[file_index].next() {
                match result {
                    Ok(next_message) => {
                        self.heap.push(FileMessageWithIndex {
                            message: next_message,
                            file_index,
                        });
                    }
                    Err(e) => {
                        eprintln!("Warning: Error reading message from file {}: {}", file_index, e);
                    }
                }
            }
            
            Some(Ok(message))
        } else {
            None
        }
    }
}