use anyhow::Result;
use redis::AsyncCommands;
use serde::Serialize;

/// Publishes event notifications to Redis Streams
pub struct EventPublisher {
    connection: redis::aio::MultiplexedConnection,
    chain_id: i32,
}

/// Types of messages published to Redis Streams
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamMessage {
    /// Batch of events from historic sync
    EventBatch {
        chain_id: i32,
        from_block: i64,
        to_block: i64,
        event_count: u64,
        mode: String,
    },
    /// Single block from live sync
    LiveBlock {
        chain_id: i32,
        block_number: i64,
        block_hash: String,
        event_count: u64,
        mode: String,
    },
    /// Reorg detected
    Reorg {
        chain_id: i32,
        common_ancestor_block: i64,
        depth: u64,
        reorged_blocks: Vec<i64>,
    },
    /// Sync status change
    SyncStatus {
        chain_id: i32,
        synced: bool,
        current_block: i64,
        target_block: i64,
    },
    /// New factory child discovered
    FactoryChild {
        chain_id: i32,
        factory_address: String,
        child_address: String,
        contract_name: String,
    },
}

impl EventPublisher {
    pub async fn new(client: &redis::Client, chain_id: i32) -> Result<Self> {
        let connection = client.get_multiplexed_async_connection().await?;
        Ok(Self {
            connection,
            chain_id,
        })
    }

    /// Publish an event batch notification (historic mode)
    pub async fn publish_event_batch(
        &mut self,
        from_block: i64,
        to_block: i64,
        event_count: u64,
    ) -> Result<()> {
        let stream_key = format!("kyomei:events:{}", self.chain_id);
        self.connection
            .xadd::<_, _, _, _, String>(
                &stream_key,
                "*",
                &[
                    ("type", "event_batch"),
                    ("chain_id", &self.chain_id.to_string()),
                    ("from_block", &from_block.to_string()),
                    ("to_block", &to_block.to_string()),
                    ("event_count", &event_count.to_string()),
                    ("mode", "historical"),
                ],
            )
            .await?;
        Ok(())
    }

    /// Publish a live block notification
    pub async fn publish_live_block(
        &mut self,
        block_number: i64,
        block_hash: &str,
        event_count: u64,
    ) -> Result<()> {
        let stream_key = format!("kyomei:events:{}", self.chain_id);
        self.connection
            .xadd::<_, _, _, _, String>(
                &stream_key,
                "*",
                &[
                    ("type", "live_block"),
                    ("chain_id", &self.chain_id.to_string()),
                    ("block_number", &block_number.to_string()),
                    ("block_hash", block_hash),
                    ("event_count", &event_count.to_string()),
                    ("mode", "live"),
                ],
            )
            .await?;
        Ok(())
    }

    /// Publish a reorg control message
    pub async fn publish_reorg(
        &mut self,
        common_ancestor_block: i64,
        depth: u64,
    ) -> Result<()> {
        let stream_key = format!("kyomei:control:{}", self.chain_id);
        self.connection
            .xadd::<_, _, _, _, String>(
                &stream_key,
                "*",
                &[
                    ("type", "reorg"),
                    ("chain_id", &self.chain_id.to_string()),
                    ("common_ancestor_block", &common_ancestor_block.to_string()),
                    ("depth", &depth.to_string()),
                ],
            )
            .await?;
        Ok(())
    }

    /// Publish sync status change
    pub async fn publish_sync_status(
        &mut self,
        synced: bool,
        current_block: i64,
        target_block: i64,
    ) -> Result<()> {
        let stream_key = format!("kyomei:control:{}", self.chain_id);
        self.connection
            .xadd::<_, _, _, _, String>(
                &stream_key,
                "*",
                &[
                    ("type", "sync_status"),
                    ("chain_id", &self.chain_id.to_string()),
                    ("synced", &synced.to_string()),
                    ("current_block", &current_block.to_string()),
                    ("target_block", &target_block.to_string()),
                ],
            )
            .await?;
        Ok(())
    }

    /// Publish factory child discovery
    pub async fn publish_factory_child(
        &mut self,
        factory_address: &str,
        child_address: &str,
        contract_name: &str,
    ) -> Result<()> {
        let stream_key = format!("kyomei:control:{}", self.chain_id);
        self.connection
            .xadd::<_, _, _, _, String>(
                &stream_key,
                "*",
                &[
                    ("type", "factory_child"),
                    ("chain_id", &self.chain_id.to_string()),
                    ("factory_address", factory_address),
                    ("child_address", child_address),
                    ("contract_name", contract_name),
                ],
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_message_event_batch_serialization() {
        let msg = StreamMessage::EventBatch {
            chain_id: 1,
            from_block: 10000000,
            to_block: 10001000,
            event_count: 5000,
            mode: "historical".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"event_batch\""));
        assert!(json.contains("\"chain_id\":1"));
        assert!(json.contains("\"from_block\":10000000"));
        assert!(json.contains("\"mode\":\"historical\""));
    }

    #[test]
    fn test_stream_message_live_block_serialization() {
        let msg = StreamMessage::LiveBlock {
            chain_id: 137,
            block_number: 55000000,
            block_hash: "0xabcdef".to_string(),
            event_count: 42,
            mode: "live".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"live_block\""));
        assert!(json.contains("\"block_number\":55000000"));
        assert!(json.contains("\"block_hash\":\"0xabcdef\""));
    }

    #[test]
    fn test_stream_message_reorg_serialization() {
        let msg = StreamMessage::Reorg {
            chain_id: 1,
            common_ancestor_block: 17999990,
            depth: 10,
            reorged_blocks: vec![17999991, 17999992, 17999993],
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"reorg\""));
        assert!(json.contains("\"depth\":10"));
        assert!(json.contains("17999991"));
    }

    #[test]
    fn test_stream_message_sync_status_serialization() {
        let msg = StreamMessage::SyncStatus {
            chain_id: 1,
            synced: true,
            current_block: 18000000,
            target_block: 18000000,
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"sync_status\""));
        assert!(json.contains("\"synced\":true"));
    }

    #[test]
    fn test_stream_message_factory_child_serialization() {
        let msg = StreamMessage::FactoryChild {
            chain_id: 1,
            factory_address: "0xfactory".to_string(),
            child_address: "0xchild".to_string(),
            contract_name: "UniswapV2Pair".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"factory_child\""));
        assert!(json.contains("\"contract_name\":\"UniswapV2Pair\""));
    }

    #[test]
    fn test_stream_key_format_events() {
        let chain_id = 1;
        let key = format!("kyomei:events:{}", chain_id);
        assert_eq!(key, "kyomei:events:1");
    }

    #[test]
    fn test_stream_key_format_control() {
        let chain_id = 137;
        let key = format!("kyomei:control:{}", chain_id);
        assert_eq!(key, "kyomei:control:137");
    }

    #[test]
    fn test_stream_message_reorg_empty_blocks() {
        let msg = StreamMessage::Reorg {
            chain_id: 1,
            common_ancestor_block: 100,
            depth: 0,
            reorged_blocks: vec![],
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"reorged_blocks\":[]"));
    }
}
