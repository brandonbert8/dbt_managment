{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['silver','deep_cleaning','usage']) }}

with normalized as (
    select
        usage_id,
        customer_id,
        try_to_date(to_varchar(usage_date)) as usage_date,
        data_download_gb,
        data_upload_gb,
        voice_minutes,
        sms_count,
        streaming_hours,
        total_session_count,
        avg_session_minutes,
        international_minutes,
        peak_usage_gb,
        offpeak_usage_gb,
        source_extracted_at::timestamp_ltz as source_extracted_at
    from {{ ref('stg_service_usage') }}
    where usage_id is not null
      and customer_id is not null
),
cleaned as (
    select
        *,
        greatest(coalesce(data_download_gb, 0), 0)::number(18,4) as download_clean,
        greatest(coalesce(data_upload_gb, 0), 0)::number(18,4) as upload_clean,
        greatest(coalesce(voice_minutes, 0), 0)::number(18,2) as voice_clean,
        greatest(coalesce(sms_count, 0), 0)::integer as sms_clean,
        least(greatest(coalesce(streaming_hours, 0), 0), 24)::number(18,2) as streaming_clean,
        greatest(coalesce(total_session_count, 0), 0)::integer as sessions_clean,
        least(greatest(coalesce(avg_session_minutes, 0), 0), 1440)::number(18,2) as avg_session_clean,
        greatest(coalesce(international_minutes, 0), 0)::number(18,2) as international_clean,
        greatest(coalesce(peak_usage_gb, 0), 0)::number(18,4) as peak_clean,
        greatest(coalesce(offpeak_usage_gb, 0), 0)::number(18,4) as offpeak_clean
    from normalized
)
select
    usage_id,
    customer_id,
    usage_date,
    download_clean as data_download_gb,
    upload_clean as data_upload_gb,
    voice_clean as voice_minutes,
    sms_clean as sms_count,
    streaming_clean as streaming_hours,
    sessions_clean as total_session_count,
    avg_session_clean as avg_session_minutes,
    international_clean as international_minutes,
    peak_clean as peak_usage_gb,
    offpeak_clean as offpeak_usage_gb,
    (download_clean + upload_clean)::number(18,4) as total_data_gb,
    (voice_clean + international_clean)::number(18,2) as total_voice_minutes,
    iff(sessions_clean > 0, (download_clean + upload_clean) / sessions_clean, null)::number(18,4)
        as avg_data_gb_per_session,
    download_clean >= 0 and upload_clean >= 0 and voice_clean >= 0
        and sms_clean >= 0 and streaming_clean >= 0 as dq_nonnegative_metrics,
    abs((peak_clean + offpeak_clean) - download_clean) <= greatest(0.10, download_clean * 0.10)
        as dq_peak_offpeak_reconciles,
    case
        when usage_date is null then 'INVALID_DATE'
        when usage_date > current_date() then 'FUTURE_DATE_EXCLUDED'
        when coalesce(streaming_hours, 0) > 24 or coalesce(avg_session_minutes, 0) > 1440 then 'OUTLIER_CAPPED'
        when download_clean + upload_clean + voice_clean + sms_clean + streaming_clean = 0 then 'NO_RECORDED_USAGE'
        when abs((peak_clean + offpeak_clean) - download_clean) > greatest(0.10, download_clean * 0.10)
            then 'USAGE_COMPONENT_MISMATCH'
        else 'VALID'
    end as usage_dq_status,
    iff(download_clean + upload_clean + voice_clean + sms_clean + streaming_clean > 0, true, false)
        as has_service_activity,
    false as source_is_deleted,
    null::varchar as source_raw_id,
    source_extracted_at,
    null::number as source_generation_id
from cleaned
where usage_date is not null
  and usage_date <= current_date()
qualify row_number() over (partition by usage_id order by source_extracted_at desc) = 1
