-- One typed row per Debezium change (same logic as pipeline/staging.py).
-- A delete has `after = null`, so the key must come from `before`.
with src as (
    select *, _payload::json as j
    from {{ source('bronze', 'tickets') }}
    where _op is not null                -- skip Kafka tombstones (value = null)
)
select
    coalesce(j->'value'->'after'->>'ticket_id',
             j->'value'->'before'->>'ticket_id',
             j->'key'->>'ticket_id')                                 as ticket_id,
    _op,
    (j->'value'->'source'->>'lsn')::bigint                           as _lsn,
    j->'value'->'after'->>'user_id'                                  as user_id,
    j->'value'->'after'->>'subject'                                  as subject,
    j->'value'->'after'->>'body'                                     as body,
    j->'value'->'after'->>'priority'                                 as priority,
    j->'value'->'after'->>'status'                                   as status,
    j->'value'->'after'->>'category'                                 as category,
    make_timestamp((j->'value'->'after'->>'created_at')::bigint)     as created_at,
    make_timestamp((j->'value'->'after'->>'updated_at')::bigint)     as updated_at,
    _batch_id
from src
