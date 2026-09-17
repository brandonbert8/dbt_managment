{{ config(materialized='view', schema='DBT_STAGING', tags=['staging','depuracion']) }}

with source_data as (
    select * from {{ source('raw_churn','subscriptions_contracts') }}
), deduplicated as (
    select * from source_data
    where nullif(trim(subscription_id),'') is not null
      and nullif(trim(customer_id),'') is not null
      and contract_start_date is not null
    qualify row_number() over (
        partition by trim(subscription_id)
        order by _airbyte_extracted_at desc, _airbyte_generation_id desc
    ) = 1
)
select
    trim(subscription_id) as subscription_id,
    trim(customer_id) as customer_id,
    upper(trim(status)) as subscription_status,
    upper(trim(plan_name)) as plan_name,
    upper(trim(plan_tier)) as plan_tier,
    upper(trim(contract_type)) as contract_type,
    contract_start_date,
    contract_end_date,
    plan_change_date,
    nullif(upper(trim(previous_plan)),'') as previous_plan,
    greatest(coalesce(base_monthly_price,0),0)::number(18,2) as base_monthly_price,
    iff(coalesce(phone_service,0)=1,true,false) as has_phone_service,
    iff(coalesce(multiple_lines,0)=1,true,false) as has_multiple_lines,
    iff(coalesce(internet_service,0)=1,true,false) as has_internet_service,
    iff(coalesce(online_security,0)=1,true,false) as has_online_security,
    iff(coalesce(online_backup,0)=1,true,false) as has_online_backup,
    iff(coalesce(device_protection,0)=1,true,false) as has_device_protection,
    iff(coalesce(tech_support,0)=1,true,false) as has_tech_support,
    iff(coalesce(streaming_tv,0)=1,true,false) as has_streaming_tv,
    iff(coalesce(streaming_movies,0)=1,true,false) as has_streaming_movies,
    iff(coalesce(paperless_billing,0)=1,true,false) as has_paperless_billing,
    _airbyte_extracted_at as source_extracted_at
from deduplicated
