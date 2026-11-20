select
    f.source_sha256,
    f.source_filename,
    f.ingested_at,
    f.completed_at,
    f.status as ingestion_status,
    f.total_rows,
    f.accepted_rows,
    f.rejected_rows,
    case
        when f.total_rows = 0 then null
        else f.rejected_rows::numeric / f.total_rows
    end as rejected_row_fraction,
    f.last_error
from {{ source('raw', 'ingestion_files') }} as f
