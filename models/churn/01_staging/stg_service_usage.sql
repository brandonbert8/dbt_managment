{{ config(materialized='view', schema='DBT_STAGING', tags=['staging','depuracion']) }}
with source_data as (select * from {{ source('raw_churn','service_usage') }}),
deduplicated as (
 select * from source_data
 where nullif(trim(usage_id),'') is not null and nullif(trim(customer_id),'') is not null and usage_date is not null
 qualify row_number() over (partition by trim(usage_id) order by _airbyte_extracted_at desc, _airbyte_generation_id desc)=1
)
select trim(usage_id) as usage_id, trim(customer_id) as customer_id, usage_date,
 to_char(usage_date,'YYYY-MM') as year_month,
 greatest(coalesce(sms_count,0),0)::integer as sms_count,
 greatest(coalesce(voice_minutes,0),0)::number(18,2) as voice_minutes,
 greatest(coalesce(international_minutes,0),0)::number(18,2) as international_minutes,
 greatest(coalesce(data_upload_gb,0),0)::number(18,4) as data_upload_gb,
 greatest(coalesce(data_download_gb,0),0)::number(18,4) as data_download_gb,
 greatest(coalesce(peak_usage_gb,0),0)::number(18,4) as peak_usage_gb,
 greatest(coalesce(offpeak_usage_gb,0),0)::number(18,4) as offpeak_usage_gb,
 greatest(coalesce(streaming_hours,0),0)::number(18,2) as streaming_hours,
 greatest(coalesce(avg_session_minutes,0),0)::number(18,2) as avg_session_minutes,
 greatest(coalesce(total_session_count,0),0)::integer as total_session_count,
 _airbyte_extracted_at as source_extracted_at
from deduplicated
