{{ config(materialized='incremental',schema='DBT_MARTS',unique_key='network_event_key',incremental_strategy='merge',on_schema_change='sync_all_columns',tags=['gold','fact']) }}
select
    {{ generate_surrogate_key(['n.event_id']) }} as network_event_key,
    {{ generate_surrogate_key(['n.customer_id']) }} as customer_key,
    {{ generate_surrogate_key(['c.region']) }} as region_key,
    {{ generate_surrogate_key(['n.network_node']) }} as network_node_key,
    to_number(to_char(n.event_timestamp::date, 'YYYYMMDD')) as event_date_key,
    n.event_id,
    n.device_id,
    n.event_timestamp,
    n.event_type_norm,
    n.technology_norm,
    n.latency_ms,
    n.jitter_ms,
    n.packet_loss_pct,
    n.download_mbps,
    n.upload_mbps,
    n.signal_strength_dbm,
    n.outage_duration_seconds,
    n.connection_dropped_bool,
    iff(n.connection_dropped_bool, 1, 0) as connection_drop_count,
    1 as network_event_count,
    n.network_dq_status,
    n.source_is_deleted,
    'PRE_OUTCOME_FEATURE' as ml_field_class,
    n.source_raw_id,
    n.source_extracted_at,
    n.source_generation_id
from {{ ref('int_network_clean') }} n
left join {{ ref('int_customer_clean') }} c
    using (customer_id)
{% if is_incremental() %}
    where
        n.source_extracted_at
        > (
            select
                coalesce(max(source_extracted_at), '1900-01-01'::timestamp_tz)
            from {{ this }}
        )
{% endif %}
