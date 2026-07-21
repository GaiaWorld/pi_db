use super::{STARTUP_TABLE_META_BATCH_LIMIT, stage_startup_table_meta};

fn stage_all(count: usize) -> (Vec<usize>, Vec<Vec<usize>>) {
    let mut buffer = Vec::with_capacity(STARTUP_TABLE_META_BATCH_LIMIT);
    let mut emitted_at = Vec::new();
    let mut batches = Vec::new();
    for item in 0..count {
        if let Some(batch) = stage_startup_table_meta(&mut buffer, item) {
            emitted_at.push(item);
            batches.push(batch);
        }
    }
    if !buffer.is_empty() {
        batches.push(buffer);
    }
    (emitted_at, batches)
}

#[test]
fn test_stage_startup_table_meta_preserves_all_boundary_items() {
    for count in [0,
                  1,
                  STARTUP_TABLE_META_BATCH_LIMIT - 1,
                  STARTUP_TABLE_META_BATCH_LIMIT,
                  STARTUP_TABLE_META_BATCH_LIMIT + 1,
                  STARTUP_TABLE_META_BATCH_LIMIT * 2,
                  STARTUP_TABLE_META_BATCH_LIMIT * 2 + 1] {
        let (emitted_at, batches) = stage_all(count);
        let expected_emitted_at = (STARTUP_TABLE_META_BATCH_LIMIT..count)
            .step_by(STARTUP_TABLE_META_BATCH_LIMIT)
            .collect::<Vec<_>>();
        assert_eq!(emitted_at, expected_emitted_at,
                   "startup full batches were emitted at the wrong input positions for item count {count}");
        let expected_batch_count = if count == 0 {
            0
        } else {
            (count - 1) / STARTUP_TABLE_META_BATCH_LIMIT + 1
        };
        assert_eq!(batches.len(), expected_batch_count,
                   "unexpected batch count for item count {count}");
        assert!(batches.iter().all(|batch| !batch.is_empty()),
                "empty startup batch for item count {count}");
        if batches.len() > 1 {
            assert!(batches[..batches.len() - 1]
                        .iter()
                        .all(|batch| batch.len() == STARTUP_TABLE_META_BATCH_LIMIT),
                    "non-final startup batch has an invalid size for item count {count}");
        }
        if let Some(final_batch) = batches.last() {
            let expected_final_len = (count - 1) % STARTUP_TABLE_META_BATCH_LIMIT + 1;
            assert_eq!(final_batch.len(), expected_final_len,
                       "unexpected final batch size for item count {count}");
        }

        let mut expected_item = 0;
        for batch in &batches {
            for item in batch {
                assert_eq!(*item, expected_item,
                           "startup item was lost, duplicated, or reordered for item count {count}");
                expected_item += 1;
            }
        }
        assert_eq!(expected_item, count,
                   "startup batching did not consume every item for item count {count}");
    }
}
