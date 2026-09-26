{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['silver','deep_cleaning','subscriptions']) }}

with normalized as (
    select
        subscription_id,
        customer_id,
        case
            when upper(trim(subscription_status)) in ('ACTIVE','ACTIVO','ENABLED') then 'ACTIVE'
            when upper(trim(subscription_status)) in ('CANCELLED','CANCELED','INACTIVE','BAJA') then 'CANCELLED'
            when upper(trim(subscription_status)) in ('SUPERSEDED','REPLACED') then 'SUPERSEDED'
            else 'UNKNOWN'
        end as status,
        coalesce(nullif(upper(trim(plan_name)), ''), 'UNKNOWN') as plan_name,
        coalesce(nullif(upper(trim(plan_tier)), ''), 'UNKNOWN') as plan_tier,
        case
            when upper(trim(contract_type)) in ('MONTH-TO-MONTH','MONTH TO MONTH','MENSUAL') then 'MONTH_TO_MONTH'
            when upper(trim(contract_type)) in ('ONE YEAR','1 YEAR','ANNUAL') then 'ONE_YEAR'
            when upper(trim(contract_type)) in ('TWO YEAR','2 YEAR','BIENNIAL') then 'TWO_YEAR'
            else coalesce(nullif(upper(trim(contract_type)), ''), 'UNKNOWN')
        end as contract_type,
        try_to_date(to_varchar(contract_start_date)) as contract_start_date,
        try_to_date(to_varchar(contract_end_date)) as contract_end_date_source,
        try_to_date(to_varchar(plan_change_date)) as plan_change_date_source,
        nullif(upper(trim(previous_plan)), '') as previous_plan,
        base_monthly_price,
        coalesce(has_phone_service, false) as has_phone_service,
        coalesce(has_multiple_lines, false) as has_multiple_lines,
        coalesce(has_internet_service, false) as has_internet_service,
        coalesce(has_online_security, false) as has_online_security,
        coalesce(has_online_backup, false) as has_online_backup,
        coalesce(has_device_protection, false) as has_device_protection,
        coalesce(has_tech_support, false) as has_tech_support,
        coalesce(has_streaming_tv, false) as has_streaming_tv,
        coalesce(has_streaming_movies, false) as has_streaming_movies,
        coalesce(has_paperless_billing, false) as paperless_billing,
        source_extracted_at::timestamp_ltz as source_extracted_at
    from {{ ref('stg_subscriptions_contracts') }}
    where subscription_id is not null
      and customer_id is not null
),
cleaned as (
    select
        *,
        iff(contract_end_date_source is null or contract_end_date_source >= contract_start_date,
            contract_end_date_source, null) as contract_end_date,
        iff(plan_change_date_source is null or plan_change_date_source >= contract_start_date,
            plan_change_date_source, null) as plan_change_date,
        greatest(coalesce(base_monthly_price, 0), 0)::number(18,2) as base_monthly_price_clean
    from normalized
)
select
    subscription_id,
    customer_id,
    status,
    plan_name,
    plan_tier,
    contract_type,
    contract_start_date,
    contract_end_date,
    contract_end_date_source,
    plan_change_date,
    plan_change_date_source,
    previous_plan,
    base_monthly_price_clean as base_monthly_price,
    has_phone_service,
    has_multiple_lines,
    has_internet_service,
    has_online_security,
    has_online_backup,
    has_device_protection,
    has_tech_support,
    has_streaming_tv,
    has_streaming_movies,
    paperless_billing,
    contract_end_date_source is null or contract_end_date_source >= contract_start_date
        as dq_valid_contract_dates,
    plan_change_date_source is null or plan_change_date_source >= contract_start_date
        as dq_valid_plan_change_date,
    plan_change_date_source is null or previous_plan is not null as dq_plan_change_has_previous_plan,
    case
        when contract_start_date is null then 'INVALID_START_DATE'
        when contract_start_date > current_date() then 'FUTURE_START_EXCLUDED'
        when contract_end_date_source < contract_start_date then 'INVALID_END_DATE_TO_NULL'
        when plan_change_date_source < contract_start_date then 'INVALID_PLAN_CHANGE_TO_NULL'
        when plan_change_date_source is not null and previous_plan is null then 'MISSING_PREVIOUS_PLAN'
        when base_monthly_price is null then 'PRICE_IMPUTED_ZERO'
        when base_monthly_price < 0 then 'NEGATIVE_PRICE_CORRECTED'
        else 'VALID'
    end as subscription_dq_status,
    iff(status = 'ACTIVE'
        and (contract_end_date is null or contract_end_date >= current_date()), true, false)
        as is_active_subscription,
    iff(status = 'CANCELLED'
        or (contract_end_date is not null and contract_end_date < current_date()), true, false)
        as churn_candidate_flag,
    datediff(month, contract_start_date, coalesce(contract_end_date, current_date()))::integer
        as tenure_months,
    false as source_is_deleted,
    null::varchar as source_raw_id,
    source_extracted_at,
    null::number as source_generation_id
from cleaned
where contract_start_date is not null
  and contract_start_date <= current_date()
qualify row_number() over (
    partition by subscription_id
    order by source_extracted_at desc, contract_start_date desc
) = 1
