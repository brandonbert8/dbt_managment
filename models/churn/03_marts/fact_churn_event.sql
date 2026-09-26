{{ config(materialized='incremental',schema='DBT_MARTS',unique_key='churn_event_key',incremental_strategy='merge',on_schema_change='sync_all_columns',tags=['gold','fact','churn']) }}
select
    {{ generate_surrogate_key(['ch.customer_id','ch.churn_date']) }}
        as churn_event_key,
    {{ generate_surrogate_key(['ch.customer_id']) }} as customer_key,
    {{ generate_surrogate_key(['c.region']) }} as region_key,
    to_number(to_char(ch.churn_date, 'YYYYMMDD')) as churn_date_key,
    ch.customer_id,
    ch.tenure_months_at_end,
    1 as churn_event_count,
    ch.churn_reason,
    ch.customer_status,
    ch.churned_field_class,
    ch.churn_detail_field_class,
    ch.source_raw_id,
    ch.source_extracted_at::timestamp_tz as source_extracted_at,
    ch.source_generation_id
from {{ ref('int_churn_clean') }} ch
left join {{ ref('int_customer_clean') }} c
    using (customer_id)
{% if is_incremental() %}
    where
        ch.source_extracted_at
        > (
            select
                coalesce(max(source_extracted_at), '1900-01-01'::timestamp_tz)
            from {{ this }}
        )
{% endif %}
