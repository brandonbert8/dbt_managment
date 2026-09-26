{{ config(materialized='incremental',schema='DBT_MARTS',unique_key='subscription_event_key',incremental_strategy='merge',on_schema_change='sync_all_columns',tags=['gold','fact']) }}
select
    {{ generate_surrogate_key(['s.subscription_id','s.contract_start_date']) }}
        as subscription_event_key,
    {{ generate_surrogate_key(['s.subscription_id']) }} as subscription_key,
    {{ generate_surrogate_key(['s.customer_id']) }} as customer_key,
    {{ generate_surrogate_key(['s.plan_name','s.plan_tier']) }} as plan_key,
    to_number(to_char(s.contract_start_date, 'YYYYMMDD')) as start_date_key,
    s.subscription_id,
    s.status,
    s.contract_type,
    s.base_monthly_price,
    iff(s.plan_change_date is not null, 1, 0) as plan_change_count,
    1 as subscription_event_count,
    s.dq_valid_contract_dates,
    s.dq_valid_plan_change_date,
    s.source_raw_id,
    s.source_extracted_at,
    s.source_generation_id
from {{ ref('int_subscription_clean') }} s
{% if is_incremental() %}
    where
        s.source_extracted_at
        > (
            select
                coalesce(max(source_extracted_at), '1900-01-01'::timestamp_tz)
            from {{ this }}
        )
{% endif %}
