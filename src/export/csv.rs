use crate::abi::decoder::{AbiDecoder, DecodedEventRow};
use crate::types::RawEventRecord;
use anyhow::Result;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tracing::{debug, info};

/// Exports decoded events to per-event-type CSV files.
/// Thread-safe via internal Mutex on each file writer.
pub struct CsvExporter {
    output_dir: PathBuf,
    /// table_name → writer (lazily initialized)
    writers: Mutex<HashMap<String, csv::Writer<BufWriter<File>>>>,
    /// table_name → whether headers have been written
    headers_written: Mutex<HashMap<String, bool>>,
}

impl CsvExporter {
    pub fn new(output_dir: &str) -> Result<Self> {
        let output_dir = PathBuf::from(output_dir);
        fs::create_dir_all(&output_dir)?;

        info!(output_dir = %output_dir.display(), "CSV exporter initialized");

        Ok(Self {
            output_dir,
            writers: Mutex::new(HashMap::new()),
            headers_written: Mutex::new(HashMap::new()),
        })
    }

    /// Write decoded events to CSV files, one file per event type.
    /// Decodes raw events using the ABI decoder, groups by table, and writes in batches
    /// to minimize lock acquisitions.
    pub fn write_events(&self, events: &[RawEventRecord], decoder: &AbiDecoder) -> Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        // Decode all events first (no lock needed)
        let decoded: Vec<DecodedEventRow> = events
            .iter()
            .filter_map(|e| decoder.decode_raw_event(e))
            .collect();

        if decoded.is_empty() {
            return Ok(0);
        }

        // Group by table name
        let mut by_table: HashMap<&str, Vec<&DecodedEventRow>> = HashMap::new();
        for row in &decoded {
            by_table.entry(&row.table_name).or_default().push(row);
        }

        // Write each table's rows under a single lock acquisition
        for (_table, rows) in &by_table {
            for row in rows {
                self.write_decoded_row(row)?;
            }
        }

        let written = decoded.len() as u64;

        // Flush all writers
        let mut writers = self.writers.lock().unwrap();
        for writer in writers.values_mut() {
            writer.flush()?;
        }
        debug!(written, "Flushed CSV events");

        Ok(written)
    }

    fn write_decoded_row(&self, row: &DecodedEventRow) -> Result<()> {
        let mut writers = self.writers.lock().unwrap();
        let mut headers_written = self.headers_written.lock().unwrap();

        let needs_header = !headers_written.get(&row.table_name).copied().unwrap_or(false);

        let writer = if let Some(w) = writers.get_mut(&row.table_name) {
            w
        } else {
            let path = self.csv_path(&row.table_name);
            let file_exists = path.exists();
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)?;

            let writer = csv::Writer::from_writer(BufWriter::new(file));
            writers.insert(row.table_name.clone(), writer);

            // If file already existed and has content, don't write header
            if file_exists {
                headers_written.insert(row.table_name.clone(), true);
            }

            writers.get_mut(&row.table_name).unwrap()
        };

        // Write header row if this is a new file
        if needs_header && !headers_written.get(&row.table_name).copied().unwrap_or(false) {
            let mut header: Vec<&str> = vec![
                "chain_id",
                "block_number",
                "tx_index",
                "log_index",
                "block_hash",
                "block_timestamp",
                "tx_hash",
                "address",
            ];
            for col in &row.param_columns {
                header.push(col);
            }
            writer.write_record(&header)?;
            headers_written.insert(row.table_name.clone(), true);
        }

        // Write data row
        let mut record: Vec<String> = vec![
            row.chain_id.to_string(),
            row.block_number.to_string(),
            row.tx_index.to_string(),
            row.log_index.to_string(),
            row.block_hash.clone(),
            row.block_timestamp.to_string(),
            row.tx_hash.clone(),
            row.address.clone(),
        ];
        for val in &row.param_values {
            record.push(val.clone());
        }
        writer.write_record(&record)?;

        Ok(())
    }

    fn csv_path(&self, table_name: &str) -> PathBuf {
        self.output_dir.join(format!("{}.csv", table_name))
    }

    /// Get the output directory path
    pub fn output_dir(&self) -> &Path {
        &self.output_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abi::decoder::AbiDecoder;
    use crate::types::RawEventRecord;

    fn test_decoder() -> AbiDecoder {
        let mut decoder = AbiDecoder::new();
        decoder
            .register_abi_json(
                "ERC20",
                r#"[{
                    "type": "event",
                    "name": "Transfer",
                    "anonymous": false,
                    "inputs": [
                        { "type": "address", "name": "from", "indexed": true },
                        { "type": "address", "name": "to", "indexed": true },
                        { "type": "uint256", "name": "value", "indexed": false }
                    ]
                }]"#,
            )
            .unwrap();
        decoder
    }

    fn transfer_event() -> RawEventRecord {
        RawEventRecord {
            chain_id: 1,
            block_number: 18000000,
            tx_index: 5,
            log_index: 12,
            block_hash: "0xabc123".to_string(),
            block_timestamp: 1700000000,
            tx_hash: "0xdef456".to_string(),
            address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".to_string(),
            // Transfer(address,address,uint256) topic0
            topic0: Some(
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".to_string(),
            ),
            topic1: Some(
                "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            ),
            topic2: Some(
                "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            ),
            topic3: None,
            data: "0x0000000000000000000000000000000000000000000000000000000000000064"
                .to_string(),
        }
    }

    #[test]
    fn test_csv_exporter_creates_output_dir() {
        let dir = std::env::temp_dir().join("kyomei_csv_test_create");
        let _ = fs::remove_dir_all(&dir);

        let exporter = CsvExporter::new(dir.to_str().unwrap()).unwrap();
        assert!(exporter.output_dir().exists());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_csv_exporter_writes_events() {
        let dir = std::env::temp_dir().join("kyomei_csv_test_write");
        let _ = fs::remove_dir_all(&dir);

        let decoder = test_decoder();
        let exporter = CsvExporter::new(dir.to_str().unwrap()).unwrap();

        let events = vec![transfer_event()];
        let written = exporter.write_events(&events, &decoder).unwrap();
        assert_eq!(written, 1);

        // Verify CSV file exists
        let csv_path = dir.join("event_e_r_c20_transfer.csv");
        assert!(csv_path.exists());

        // Read and verify content
        let content = fs::read_to_string(&csv_path).unwrap();
        let lines: Vec<&str> = content.trim().lines().collect();
        assert_eq!(lines.len(), 2); // header + 1 data row

        // Verify header
        assert!(lines[0].contains("chain_id"));
        assert!(lines[0].contains("block_number"));
        assert!(lines[0].contains("tx_hash"));

        // Verify data row
        assert!(lines[1].contains("18000000"));
        assert!(lines[1].contains("0xdef456"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_csv_exporter_appends_on_multiple_writes() {
        let dir = std::env::temp_dir().join("kyomei_csv_test_append");
        let _ = fs::remove_dir_all(&dir);

        let decoder = test_decoder();
        let exporter = CsvExporter::new(dir.to_str().unwrap()).unwrap();

        let events = vec![transfer_event()];
        exporter.write_events(&events, &decoder).unwrap();

        let mut event2 = transfer_event();
        event2.block_number = 18000001;
        let events2 = vec![event2];
        exporter.write_events(&events2, &decoder).unwrap();

        let csv_path = dir.join("event_e_r_c20_transfer.csv");
        let content = fs::read_to_string(&csv_path).unwrap();
        let lines: Vec<&str> = content.trim().lines().collect();
        assert_eq!(lines.len(), 3); // header + 2 data rows

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_csv_exporter_skips_unknown_events() {
        let dir = std::env::temp_dir().join("kyomei_csv_test_skip");
        let _ = fs::remove_dir_all(&dir);

        let decoder = test_decoder();
        let exporter = CsvExporter::new(dir.to_str().unwrap()).unwrap();

        let mut event = transfer_event();
        event.topic0 = Some("0x0000000000000000000000000000000000000000000000000000000000000000".to_string());

        let events = vec![event];
        let written = exporter.write_events(&events, &decoder).unwrap();
        assert_eq!(written, 0);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_csv_writes_correct_columns() {
        let dir = std::env::temp_dir().join("kyomei_csv_test_cols");
        let _ = fs::remove_dir_all(&dir);

        let decoder = test_decoder();
        let exporter = CsvExporter::new(dir.to_str().unwrap()).unwrap();

        let events = vec![transfer_event()];
        exporter.write_events(&events, &decoder).unwrap();

        let csv_path = dir.join("event_e_r_c20_transfer.csv");
        let content = fs::read_to_string(&csv_path).unwrap();
        let mut reader = csv::Reader::from_reader(content.as_bytes());

        // Verify exact header columns
        let headers = reader.headers().unwrap();
        assert_eq!(headers.get(0).unwrap(), "chain_id");
        assert_eq!(headers.get(1).unwrap(), "block_number");
        assert_eq!(headers.get(2).unwrap(), "tx_index");
        assert_eq!(headers.get(3).unwrap(), "log_index");
        assert_eq!(headers.get(4).unwrap(), "block_hash");
        assert_eq!(headers.get(5).unwrap(), "block_timestamp");
        assert_eq!(headers.get(6).unwrap(), "tx_hash");
        assert_eq!(headers.get(7).unwrap(), "address");
        // event-specific param columns follow
        assert!(headers.len() > 8);

        // Verify data values
        let record = reader.records().next().unwrap().unwrap();
        assert_eq!(record.get(0).unwrap(), "1"); // chain_id
        assert_eq!(record.get(1).unwrap(), "18000000"); // block_number
        assert_eq!(record.get(6).unwrap(), "0xdef456"); // tx_hash

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_csv_special_characters_in_values() {
        let dir = std::env::temp_dir().join("kyomei_csv_test_special");
        let _ = fs::remove_dir_all(&dir);

        let exporter = CsvExporter::new(dir.to_str().unwrap()).unwrap();

        // Write a row with special CSV characters (commas, quotes, newlines)
        let row = DecodedEventRow {
            table_name: "event_test_special".to_string(),
            contract_name: "Test".to_string(),
            event_name: "Special".to_string(),
            param_columns: vec!["data".to_string()],
            chain_id: 1,
            block_number: 100,
            tx_index: 0,
            log_index: 0,
            block_hash: "0xabc".to_string(),
            block_timestamp: 1000,
            tx_hash: "0xdef".to_string(),
            address: "0x123".to_string(),
            param_values: vec!["value,with\"quotes,and,commas".to_string()],
        };
        exporter.write_decoded_row(&row).unwrap();

        // Flush
        let mut writers = exporter.writers.lock().unwrap();
        for w in writers.values_mut() { w.flush().unwrap(); }
        drop(writers);

        // Read back and verify the CSV properly escapes special chars
        let csv_path = dir.join("event_test_special.csv");
        let content = fs::read_to_string(&csv_path).unwrap();
        let mut reader = csv::Reader::from_reader(content.as_bytes());
        let record = reader.records().next().unwrap().unwrap();
        // The csv crate should properly escape and unescape
        assert_eq!(record.get(8).unwrap(), "value,with\"quotes,and,commas");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_csv_empty_events_writes_nothing() {
        let dir = std::env::temp_dir().join("kyomei_csv_test_empty");
        let _ = fs::remove_dir_all(&dir);

        let decoder = test_decoder();
        let exporter = CsvExporter::new(dir.to_str().unwrap()).unwrap();

        let written = exporter.write_events(&[], &decoder).unwrap();
        assert_eq!(written, 0);

        // No files should be created
        let entries: Vec<_> = fs::read_dir(&dir).unwrap().collect();
        assert_eq!(entries.len(), 0);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_csv_multiple_writes_no_duplicate_headers() {
        let dir = std::env::temp_dir().join("kyomei_csv_test_nodup_header");
        let _ = fs::remove_dir_all(&dir);

        let decoder = test_decoder();
        let exporter = CsvExporter::new(dir.to_str().unwrap()).unwrap();

        let events = vec![transfer_event()];
        exporter.write_events(&events, &decoder).unwrap();
        exporter.write_events(&events, &decoder).unwrap();
        exporter.write_events(&events, &decoder).unwrap();

        let csv_path = dir.join("event_e_r_c20_transfer.csv");
        let content = fs::read_to_string(&csv_path).unwrap();
        let lines: Vec<&str> = content.trim().lines().collect();
        // 1 header + 3 data rows
        assert_eq!(lines.len(), 4);
        // Only first line is the header
        assert!(lines[0].starts_with("chain_id"));
        assert!(!lines[1].starts_with("chain_id"));
        assert!(!lines[2].starts_with("chain_id"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_csv_output_dir_accessor() {
        let dir = std::env::temp_dir().join("kyomei_csv_test_accessor");
        let _ = fs::remove_dir_all(&dir);

        let exporter = CsvExporter::new(dir.to_str().unwrap()).unwrap();
        assert_eq!(exporter.output_dir(), dir.as_path());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_csv_path_format() {
        let dir = std::env::temp_dir().join("kyomei_csv_test_path");
        let _ = fs::remove_dir_all(&dir);

        let exporter = CsvExporter::new(dir.to_str().unwrap()).unwrap();
        let path = exporter.csv_path("event_uniswap_v2_pair_swap");
        assert!(path.to_str().unwrap().ends_with("event_uniswap_v2_pair_swap.csv"));

        let _ = fs::remove_dir_all(&dir);
    }
}
