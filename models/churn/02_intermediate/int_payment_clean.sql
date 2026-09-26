{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['silver','deep_cleaning','payments']) }}

with normalized as (
    select
        payment_id,
        invoice_id,
        customer_id,
        try_to_date(to_varchar(payment_date)) as payment_date,
        coalesce(nullif(upper(trim(payment_method)), ''), 'UNKNOWN') as payment_method,
        case
            when upper(trim(payment_status)) in ('COMPLETED','PAID','SUCCESS','SUCCEEDED') then 'COMPLETED'
            when upper(trim(payment_status)) in ('FAILED','DECLINED','REJECTED') then 'FAILED'
            when upper(trim(payment_status)) in ('PARTIAL','PARTIALLY_PAID') then 'PARTIAL'
            when upper(trim(payment_status)) in ('PENDING','PROCESSING') then 'PENDING'
            else 'UNKNOWN'
        end as payment_status,
        coalesce(nullif(upper(trim(collection_action)), ''), 'NONE') as collection_action,
        amount_due,
        amount_paid,
        outstanding_balance as outstanding_balance_source,
        days_late,
        failed_attempts,
        source_extracted_at::timestamp_ltz as source_extracted_at
    from {{ ref('stg_payments') }}
    where payment_id is not null
      and customer_id is not null
),
cleaned as (
    select
        *,
        greatest(coalesce(amount_due, 0), 0)::number(18,2) as amount_due_clean,
        greatest(coalesce(amount_paid, 0), 0)::number(18,2) as amount_paid_clean,
        greatest(coalesce(outstanding_balance_source, 0), 0)::number(18,2) as outstanding_balance_source_clean,
        greatest(coalesce(days_late, 0), 0)::integer as days_late_clean,
        greatest(coalesce(failed_attempts, 0), 0)::integer as failed_attempts_clean
    from normalized
)
select
    payment_id,
    invoice_id,
    customer_id,
    payment_date,
    payment_method,
    payment_status,
    collection_action,
    amount_due_clean as amount_due,
    amount_paid_clean as amount_paid,
    greatest(amount_due_clean - amount_paid_clean, 0)::number(18,2) as outstanding_balance,
    outstanding_balance_source_clean as outstanding_balance_source,
    (outstanding_balance_source_clean - greatest(amount_due_clean - amount_paid_clean, 0))::number(18,2)
        as outstanding_balance_variance,
    days_late_clean as days_late,
    failed_attempts_clean as failed_attempts,
    amount_due is null or amount_due >= 0 as dq_valid_amount_due,
    amount_paid is null or amount_paid >= 0 as dq_valid_amount_paid,
    outstanding_balance_source is null or outstanding_balance_source >= 0 as dq_valid_outstanding_source,
    amount_paid_clean <= amount_due_clean * 1.05 or amount_due_clean = 0 as dq_payment_not_materially_overpaid,
    abs(outstanding_balance_source_clean - greatest(amount_due_clean - amount_paid_clean, 0)) <= 0.05
        as dq_outstanding_reconciles,
    case
        when payment_date > current_date() then 'FUTURE_PAYMENT_DATE_REVIEW'
        when amount_due is null or amount_paid is null then 'NULL_AMOUNT_IMPUTED_ZERO'
        when coalesce(amount_due, 0) < 0 or coalesce(amount_paid, 0) < 0 then 'NEGATIVE_AMOUNT_CORRECTED'
        when amount_paid_clean > amount_due_clean * 1.05 and amount_due_clean > 0 then 'OVERPAYMENT_REVIEW'
        when abs(outstanding_balance_source_clean - greatest(amount_due_clean - amount_paid_clean, 0)) > 0.05 then 'BALANCE_RECALCULATED'
        else 'VALID'
    end as payment_dq_status,
    case
        when days_late is null then 'IMPUTED_ZERO'
        when days_late < 0 then 'CORRECTED_TO_ZERO'
        else 'VALID'
    end as days_late_dq_status,
    false as source_is_deleted,
    null::varchar as source_raw_id,
    source_extracted_at,
    null::number as source_generation_id
from cleaned
qualify row_number() over (
    partition by payment_id
    order by source_extracted_at desc, payment_date desc nulls last
) = 1
