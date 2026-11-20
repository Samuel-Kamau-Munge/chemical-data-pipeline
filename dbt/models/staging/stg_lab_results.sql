with accepted_events as (
    select
        event_id,
        source_sha256,
        row_number,
        record,
        occurred_at
    from {{ source('raw', 'lab_result_events') }}
    where status = 'accepted'
)

select
    event_id as result_id,
    source_sha256,
    row_number,
    record ->> 'sample_id' as sample_id,
    record ->> 'batch_id' as batch_id,
    record ->> 'analyte' as analyte,
    (record ->> 'value')::numeric as result_value,
    record ->> 'unit' as unit,
    record ->> 'original_value' as original_value,
    record ->> 'original_unit' as original_unit,
    case
        when record ->> 'timestamp_timezone_known' = '1'
        then (record ->> 'measured_at')::timestamptz
    end as measured_at_utc,
    case
        when record ->> 'timestamp_timezone_known' = '0'
        then (record ->> 'measured_at')::timestamp
    end as measured_at_local,
    record ->> 'timestamp_timezone_known' = '1' as timestamp_timezone_known,
    record ->> 'method' as method,
    record ->> 'instrument' as instrument,
    occurred_at as event_received_at
from accepted_events
