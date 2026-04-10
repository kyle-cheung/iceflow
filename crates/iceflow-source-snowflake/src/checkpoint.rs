use anyhow::{Error, Result};
use iceflow_types::CheckpointId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Checkpoint {
    Snapshot { anchor_query_id: String },
    Stream { boundary_query_id: String },
}

pub fn encode_checkpoint(checkpoint: Checkpoint) -> CheckpointId {
    match checkpoint {
        Checkpoint::Snapshot { anchor_query_id } => {
            CheckpointId::from(format!("snowflake:v1:snapshot:{anchor_query_id}"))
        }
        Checkpoint::Stream { boundary_query_id } => {
            CheckpointId::from(format!("snowflake:v1:stream:{boundary_query_id}"))
        }
    }
}

pub fn decode_checkpoint(value: &CheckpointId) -> Result<Checkpoint> {
    let raw = value.as_str();
    let parts: Vec<_> = raw.splitn(4, ':').collect();
    match parts.as_slice() {
        ["snowflake", "v1", "snapshot", query_id] => Ok(Checkpoint::Snapshot {
            anchor_query_id: (*query_id).to_string(),
        }),
        ["snowflake", "v1", "stream", query_id] => Ok(Checkpoint::Stream {
            boundary_query_id: (*query_id).to_string(),
        }),
        _ => Err(Error::msg(format!("invalid snowflake checkpoint: {raw}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_and_decodes_snapshot_checkpoint() {
        let encoded = encode_checkpoint(Checkpoint::Snapshot {
            anchor_query_id: "01b12345-0600-1234-0000-000000000000".to_string(),
        });

        assert_eq!(
            decode_checkpoint(&encoded).expect("decode"),
            Checkpoint::Snapshot {
                anchor_query_id: "01b12345-0600-1234-0000-000000000000".to_string(),
            }
        );
    }
}
