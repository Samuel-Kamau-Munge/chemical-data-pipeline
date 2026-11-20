select
    event_id as quarantine_id,
    source_sha256,
    row_number,
    record ->> 'sample_id' as sample_id,
    record ->> 'batch_id' as batch_id,
    record ->> 'analyte' as analyte,
    record ->> 'value' as original_value,
    record ->> 'unit' as original_unit,
    validation_errors as errors,
    occurred_at as event_received_at
from {{ source('raw', 'lab_result_events') }}
where status = 'quarantined'
