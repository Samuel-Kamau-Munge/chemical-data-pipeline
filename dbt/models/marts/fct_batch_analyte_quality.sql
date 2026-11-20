select
    batch_id,
    analyte,
    unit,
    count(*) as accepted_result_count,
    count(distinct sample_id) as distinct_sample_count,
    min(result_value) as minimum_value,
    max(result_value) as maximum_value,
    avg(result_value) as average_value,
    count(distinct method) as method_count,
    min(measured_at_utc) as first_measured_at_utc,
    max(measured_at_utc) as last_measured_at_utc,
    count(*) filter (where not timestamp_timezone_known)
        as timezone_unknown_result_count
from {{ ref('stg_lab_results') }}
group by batch_id, analyte, unit
