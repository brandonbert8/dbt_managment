{{ config(materialized='incremental',schema='DBT_MARTS',unique_key='billing_key',incremental_strategy='merge',on_schema_change='sync_all_columns',tags=['gold','fact']) }}
select
    {{ generate_surrogate_key(['b.invoice_id']) }} as billing_key,
    {{ generate_surrogate_key(['b.customer_id']) }} as customer_key,
    {{ generate_surrogate_key(['b.subscription_id']) }} as subscription_key,
    {{ generate_surrogate_key(['s.plan_name','s.plan_tier']) }} as plan_key,
    {{ generate_surrogate_key(['c.region']) }} as region_key,
    to_number(to_char(b.issue_date, 'YYYYMMDD')) as issue_date_key,
    iff(
        b.due_date is not null, to_number(to_char(b.due_date, 'YYYYMMDD')), null
    ) as due_date_key,
    b.invoice_id,
    b.account_id,
    b.billing_period,
    b.invoice_status,
    b.currency,
    b.base_charge,
    b.service_charge,
    b.equipment_charge,
    b.extra_usage_charge,
    b.tax,
    b.discount,
    b.total_amount_source,
    b.total_amount_recalculated,
    b.amount_variance::number(38,0) as amount_variance,
    b.previous_month_amount,
    b.amount_change,
    b.amount_change_pct,
    1 as invoice_count,
    b.dq_valid_dates,
    b.dq_nonnegative_total,
    b.dq_total_match,
    b.source_raw_id,
    b.source_extracted_at,
    b.source_generation_id
from {{ ref('int_billing_clean') }} b
left join {{ ref('int_subscription_clean') }} s using (subscription_id)
left join {{ ref('int_customer_clean') }} c on b.customer_id = c.customer_id
{% if is_incremental() %}
    where
        b.source_extracted_at
        > (
            select
                coalesce(max(source_extracted_at), '1900-01-01'::timestamp_tz)
            from {{ this }}
        )
{% endif %}
