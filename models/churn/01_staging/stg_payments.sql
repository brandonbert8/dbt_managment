{{ config(materialized='view', schema='DBT_STAGING', tags=['staging','depuracion']) }}

with source_data as (
    select * from {{ source('raw_churn','payments') }}
), deduplicated as (
    select * from source_data
    where nullif(trim(payment_id),'') is not null
      and nullif(trim(customer_id),'') is not null
      and coalesce(amount_due,0) >= 0
      and coalesce(amount_paid,0) >= 0
    qualify row_number() over (
        partition by trim(payment_id)
        order by _airbyte_extracted_at desc, _airbyte_generation_id desc
    ) = 1
)
select
    trim(payment_id) as payment_id,
    trim(invoice_id) as invoice_id,
    trim(customer_id) as customer_id,
    payment_date,
    to_char(payment_date,'YYYY-MM') as year_month,
    upper(trim(payment_method)) as payment_method,
    upper(trim(payment_status)) as payment_status,
    coalesce(amount_due,0)::number(18,2) as amount_due,
    coalesce(amount_paid,0)::number(18,2) as amount_paid,
    greatest(coalesce(outstanding_balance,0),0)::number(18,2) as outstanding_balance,
    greatest(coalesce(days_late,0),0)::integer as days_late,
    greatest(coalesce(failed_attempts,0),0)::integer as failed_attempts,
    nullif(upper(trim(collection_action)),'') as collection_action,
    _airbyte_extracted_at as source_extracted_at
from deduplicated
