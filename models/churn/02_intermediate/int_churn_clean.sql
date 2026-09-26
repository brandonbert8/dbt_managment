{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['silver','deep_cleaning','churn']) }}

with source_churn as (
    select
        customer_id,
        try_to_date(to_varchar(churn_date)) as churn_date,
        tenure_months_at_end as tenure_months_source,
        coalesce(nullif(upper(trim(churn_reason)), ''), 'UNKNOWN') as churn_reason_raw,
        source_extracted_at::timestamp_ltz as source_extracted_at
    from {{ ref('stg_churn_labels') }}
    where customer_id is not null
      and churned = 1
),
validated as (
    select
        ch.*,
        c.registration_date,
        iff(c.registration_date is not null and ch.churn_date >= c.registration_date,
            datediff(month, c.registration_date, ch.churn_date), null)::integer
            as tenure_months_recalculated
    from source_churn ch
    left join {{ ref('int_customer_clean') }} c using (customer_id)
)
select
    customer_id,
    churn_date,
    greatest(coalesce(tenure_months_recalculated, tenure_months_source, 0), 0)::integer
        as tenure_months_at_end,
    tenure_months_source,
    tenure_months_recalculated,
    case
        when churn_reason_raw like '%PRICE%' or churn_reason_raw like '%COST%'
          or churn_reason_raw like '%EXPENS%' then 'PRICE'
        when churn_reason_raw like '%NETWORK%' or churn_reason_raw like '%SIGNAL%'
          or churn_reason_raw like '%OUTAGE%' then 'NETWORK_QUALITY'
        when churn_reason_raw like '%SUPPORT%' or churn_reason_raw like '%SERVICE%'
          or churn_reason_raw like '%ATTENTION%' then 'CUSTOMER_SERVICE'
        when churn_reason_raw like '%COMPET%' then 'COMPETITOR'
        when churn_reason_raw like '%MOVE%' or churn_reason_raw like '%RELOCAT%' then 'RELOCATION'
        when churn_reason_raw = 'UNKNOWN' then 'UNKNOWN'
        else 'OTHER'
    end as churn_reason,
    churn_reason_raw,
    'CHURNED' as customer_status,
    case
        when churn_date >= dateadd(day, -30, current_date()) then 'CHURN_0_30_DAYS'
        when churn_date >= dateadd(day, -60, current_date()) then 'CHURN_31_60_DAYS'
        when churn_date >= dateadd(day, -90, current_date()) then 'CHURN_61_90_DAYS'
        else 'CHURN_OVER_90_DAYS'
    end as churn_recency_window,
    datediff(day, churn_date, current_date())::integer as days_since_churn,
    registration_date is null or churn_date >= registration_date as dq_churn_after_registration,
    tenure_months_source is null or tenure_months_source >= 0 as dq_valid_source_tenure,
    case
        when churn_date is null then 'INVALID_CHURN_DATE'
        when churn_date > current_date() then 'FUTURE_CHURN_EXCLUDED'
        when registration_date is not null and churn_date < registration_date then 'CHURN_BEFORE_REGISTRATION_EXCLUDED'
        when tenure_months_source < 0 then 'NEGATIVE_TENURE_RECALCULATED'
        when tenure_months_recalculated is not null
          and tenure_months_source is not null
          and abs(tenure_months_source - tenure_months_recalculated) > 1
            then 'TENURE_RECALCULATED'
        else 'VALID'
    end as churn_dq_status,
    'TARGET_OUTCOME' as churned_field_class,
    'POST_OUTCOME_DETAIL' as churn_detail_field_class,
    false as source_is_deleted,
    null::varchar as source_raw_id,
    source_extracted_at,
    null::number as source_generation_id
from validated
where churn_date is not null
  and churn_date <= current_date()
  and (registration_date is null or churn_date >= registration_date)
qualify row_number() over (partition by customer_id order by source_extracted_at desc, churn_date desc) = 1
