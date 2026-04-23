use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::debug;

/// Install the Prometheus metrics recorder.
/// Must be called once before any metrics are recorded.
/// Returns the Prometheus handle for rendering metrics text.
pub fn init() -> metrics_exporter_prometheus::PrometheusHandle {
    let builder = PrometheusBuilder::new();
    let handle = builder
        .install_recorder()
        .expect("Failed to install Prometheus recorder");
    debug!("Prometheus metrics recorder installed");
    handle
}

// --- Block indexing ---

pub fn blocks_indexed(chain_id: u32, phase: &str, count: u64) {
    counter!("kyomei_blocks_indexed_total", "chain_id" => chain_id.to_string(), "phase" => phase.to_string())
        .increment(count);
}

pub fn events_stored(chain_id: u32, count: u64) {
    counter!("kyomei_events_stored_total", "chain_id" => chain_id.to_string())
        .increment(count);
}

// --- RPC ---

pub fn rpc_request(chain_id: u32, status: &str) {
    counter!("kyomei_rpc_requests_total", "chain_id" => chain_id.to_string(), "status" => status.to_string())
        .increment(1);
}

pub fn rpc_latency(chain_id: u32, seconds: f64) {
    histogram!("kyomei_rpc_latency_seconds", "chain_id" => chain_id.to_string())
        .record(seconds);
}

// --- Sync progress ---

pub fn sync_current_block(chain_id: u32, block: u64) {
    gauge!("kyomei_sync_current_block", "chain_id" => chain_id.to_string())
        .set(block as f64);
}

pub fn sync_chain_tip(chain_id: u32, block: u64) {
    gauge!("kyomei_sync_chain_tip", "chain_id" => chain_id.to_string())
        .set(block as f64);
}

pub fn consecutive_errors(chain_id: u32, count: u32) {
    gauge!("kyomei_consecutive_errors", "chain_id" => chain_id.to_string())
        .set(count as f64);
}

// --- Reorg ---

pub fn reorg_detected(chain_id: u32) {
    counter!("kyomei_reorgs_detected_total", "chain_id" => chain_id.to_string())
        .increment(1);
}

// --- Traces ---

pub fn traces_indexed(chain_id: u32, phase: &str, count: u64) {
    counter!("kyomei_traces_indexed_total", "chain_id" => chain_id.to_string(), "phase" => phase.to_string())
        .increment(count);
}

// --- Accounts ---

pub fn account_events_indexed(chain_id: u32, event_type: &str, count: u64) {
    counter!("kyomei_account_events_indexed_total", "chain_id" => chain_id.to_string(), "event_type" => event_type.to_string())
        .increment(count);
}

// --- DB ---

pub fn db_copy_duration(chain_id: u32, seconds: f64) {
    histogram!("kyomei_db_copy_duration_seconds", "chain_id" => chain_id.to_string())
        .record(seconds);
}
