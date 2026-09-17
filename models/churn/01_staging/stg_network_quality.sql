{{ config(materialized='view', schema='DBT_STAGING', tags=['staging','depuracion']) }}
with source_data as (select * from {{ source('raw_churn','network_quality') }}),
deduplicated as (
 select * from source_data
 where nullif(trim(event_id),'') is not null and nullif(trim(customer_id),'') is not null
   and _ab_cdc_deleted_at is null
 qualify row_number() over (partition by trim(event_id) order by _airbyte_extracted_at desc, _airbyte_generation_id desc)=1
)
select trim(event_id) as event_id, trim(customer_id) as customer_id, trim(customer_ref) as customer_ref,
 trim(device_id) as device_id, try_to_timestamp_ntz(timestamp) as event_timestamp,
 to_char(try_to_timestamp_ntz(timestamp),'YYYY-MM') as year_month,
 upper(trim(event_type)) as event_type, upper(trim(technology)) as technology,
 upper(trim(network_node)) as network_node,
 coalesce(latency_ms,0)::number(18,2) as latency_ms, coalesce(jitter_ms,0)::number(18,2) as jitter_ms,
 coalesce(packet_loss_pct,0)::number(18,4) as packet_loss_pct,
 coalesce(signal_strength_dbm,0)::number(18,2) as signal_strength_dbm,
 coalesce(download_mbps,0)::number(18,2) as download_mbps,
 coalesce(upload_mbps,0)::number(18,2) as upload_mbps,
 coalesce(outage_duration_seconds,0)::number(18,2) as outage_duration_seconds,
 coalesce(try_to_boolean(connection_dropped),false) as connection_dropped,
 _airbyte_extracted_at as source_extracted_at
from deduplicated
where try_to_timestamp_ntz(timestamp) is not null
