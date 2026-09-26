{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['silver','deep_cleaning','billing']) }}

with normalized as (
    select
        invoice_id,
        customer_id,
        subscription_id,
        account_id,
        issue_date,
        due_date as due_date_source,
        year_month as billing_period,
        coalesce(nullif(upper(trim(invoice_status)), ''), 'UNKNOWN') as invoice_status,
        coalesce(nullif(upper(trim(currency)), ''), 'BOB') as currency,
        base_charge,
        service_charge,
        equipment_charge,
        extra_usage_charge,
        tax,
        discount,
        total_amount,
        previous_month_amount,
        amount_change,
        amount_change_pct,
        source_extracted_at::timestamp_ltz as source_extracted_at
    from {{ ref('stg_billing_invoices') }}
    where invoice_id is not null
      and customer_id is not null
      and issue_date is not null
      and issue_date <= current_date()
),
components_cleaned as (
    select
        *,
        iff(due_date_source is null or due_date_source >= issue_date, due_date_source, null) as due_date,
        greatest(coalesce(base_charge, 0), 0)::number(18,2) as base_charge_clean,
        greatest(coalesce(service_charge, 0), 0)::number(18,2) as service_charge_clean,
        greatest(coalesce(equipment_charge, 0), 0)::number(18,2) as equipment_charge_clean,
        greatest(coalesce(extra_usage_charge, 0), 0)::number(18,2) as extra_usage_charge_clean,
        greatest(coalesce(tax, 0), 0)::number(18,2) as tax_clean,
        greatest(coalesce(discount, 0), 0)::number(18,2) as discount_clean,
        greatest(coalesce(total_amount, 0), 0)::number(18,2) as total_amount_source,
        greatest(coalesce(previous_month_amount, 0), 0)::number(18,2) as previous_month_amount_clean
    from normalized
),
reconciled as (
    select
        *,
        greatest(
            base_charge_clean + service_charge_clean + equipment_charge_clean
            + extra_usage_charge_clean + tax_clean - discount_clean,
            0
        )::number(18,2) as total_amount_recalculated
    from components_cleaned
)
select
    invoice_id,
    customer_id,
    subscription_id,
    account_id,
    issue_date,
    due_date,
    due_date_source,
    billing_period,
    invoice_status,
    currency,
    base_charge_clean as base_charge,
    service_charge_clean as service_charge,
    equipment_charge_clean as equipment_charge,
    extra_usage_charge_clean as extra_usage_charge,
    tax_clean as tax,
    discount_clean as discount,
    total_amount_source,
    total_amount_recalculated,
    (total_amount_source - total_amount_recalculated)::number(18,2) as amount_variance,
    previous_month_amount_clean as previous_month_amount,
    coalesce(amount_change, total_amount_source - previous_month_amount_clean)::number(18,2) as amount_change,
    coalesce(
        amount_change_pct,
        iff(previous_month_amount_clean > 0,
            100 * (total_amount_source - previous_month_amount_clean) / previous_month_amount_clean,
            null)
    )::number(18,4) as amount_change_pct,
    due_date_source is null or due_date_source >= issue_date as dq_valid_dates,
    total_amount_source >= 0 as dq_nonnegative_total,
    abs(total_amount_source - total_amount_recalculated) <= 0.05 as dq_total_match,
    case
        when due_date_source < issue_date then 'INVALID_DUE_DATE_TO_NULL'
        when abs(total_amount_source - total_amount_recalculated) > 0.05 then 'TOTAL_MISMATCH_REVIEW'
        when total_amount is null then 'TOTAL_IMPUTED_ZERO'
        else 'VALID'
    end as billing_dq_status,
    false as source_is_deleted,
    null::varchar as source_raw_id,
    source_extracted_at,
    null::number as source_generation_id
from reconciled
qualify row_number() over (partition by invoice_id order by source_extracted_at desc) = 1
