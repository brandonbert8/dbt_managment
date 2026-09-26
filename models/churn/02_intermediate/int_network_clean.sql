{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['silver','deep_cleaning','network']) }}

with normalized as (
    select
        event_id,
        customer_id,
        device_id,
        event_timestamp,
        coalesce(nullif(upper(trim(event_type)), ''), 'UNKNOWN') as event_type_norm,
        coalesce(nullif(upper(trim(technology)), ''), 'UNKNOWN') as technology_norm,
        coalesce(nullif(upper(trim(network_node)), ''), 'UNKNOWN') as network_node,
        latency_ms,
        jitter_ms,
        packet_loss_pct,
        download_mbps,
        upload_mbps,
        signal_strength_dbm,
        outage_duration_seconds,
        coalesce(connection_dropped, false) as connection_dropped_bool,
        source_extracted_at::timestamp_ltz as source_extracted_at
    from {{ ref('stg_network_quality') }}
    where event_id is not null
      and customer_id is not null
),
cleaned as (
    select
        *,
        least(greatest(coalesce(latency_ms, 0), 0), 10000)::number(18,2) as latency_clean,
        least(greatest(coalesce(jitter_ms, 0), 0), 5000)::number(18,2) as jitter_clean,
        least(greatest(coalesce(packet_loss_pct, 0), 0), 100)::number(18,4) as packet_loss_clean,
        greatest(coalesce(download_mbps, 0), 0)::number(18,2) as download_clean,
        greatest(coalesce(upload_mbps, 0), 0)::number(18,2) as upload_clean,
        iff(signal_strength_dbm between -150 and -10, signal_strength_dbm, null)::number(18,2)
            as signal_strength_clean,
        greatest(coalesce(outage_duration_seconds, 0), 0)::number(18,2) as outage_clean
    from normalized
)
select
    event_id,
    customer_id,
    device_id,
    event_timestamp,
    event_type_norm,
    technology_norm,
    network_node,
    latency_clean as latency_ms,
    jitter_clean as jitter_ms,
    packet_loss_clean as packet_loss_pct,
    download_clean as download_mbps,
    upload_clean as upload_mbps,
    signal_strength_clean as signal_strength_dbm,
    outage_clean as outage_duration_seconds,
    connection_dropped_bool,
    case
        when event_timestamp is null then 'INVALID_TIMESTAMP'
        when event_timestamp::date > current_date() then 'FUTURE_EVENT_EXCLUDED'
        when latency_ms < 0 or jitter_ms < 0 or download_mbps < 0 or upload_mbps < 0
            or outage_duration_seconds < 0 then 'NEGATIVE_METRIC_CORRECTED'
        when packet_loss_pct not between 0 and 100 then 'PACKET_LOSS_CAPPED'
        when signal_strength_dbm is null or signal_strength_dbm not between -150 and -10 then 'INVALID_SIGNAL_TO_NULL'
        when latency_ms > 2000 or jitter_ms > 1000 or outage_duration_seconds > 86400 then 'EXTREME_OUTLIER_CAPPED'
        else 'VALID'
    end as network_dq_status,
    case
        when connection_dropped_bool or outage_clean > 0 then 'OUTAGE_OR_DROP'
        when packet_loss_clean >= 5 or latency_clean >= 200 or jitter_clean >= 50 then 'DEGRADED'
        when signal_strength_clean is null or signal_strength_clean < -110 then 'WEAK_SIGNAL'
        else 'NORMAL'
    end as network_quality_band,
    latency_ms is null or latency_ms >= 0 as dq_valid_latency,
    packet_loss_pct is null or packet_loss_pct between 0 and 100 as dq_valid_packet_loss,
    signal_strength_dbm is null or signal_strength_dbm between -150 and -10 as dq_valid_signal,
    false as source_is_deleted,
    null::varchar as source_raw_id,
    source_extracted_at,
    null::number as source_generation_id
from cleaned
where event_timestamp is not null
  and event_timestamp::date <= current_date()
qualify row_number() over (partition by event_id order by source_extracted_at desc) = 1
