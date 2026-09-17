{{ config(materialized='view', schema='DBT_STAGING', tags=['staging','depuracion']) }}

with source_data as (
    select * from {{ source('raw_churn','billing_invoices') }}
), deduplicated as (
    select * from source_data
    where nullif(trim(invoice_id),'') is not null
      and nullif(trim(customer_id),'') is not null
      and issue_date is not null
      and coalesce(total_amount,0) >= 0
    qualify row_number() over (
        partition by trim(invoice_id)
        order by _airbyte_extracted_at desc, _airbyte_generation_id desc
    ) = 1
)
select
    trim(invoice_id) as invoice_id,
    trim(customer_id) as customer_id,
    trim(subscription_id) as subscription_id,
    trim(account_id) as account_id,
    issue_date,
    due_date,
    to_char(issue_date,'YYYY-MM') as year_month,
    upper(trim(currency)) as currency,
    upper(trim(invoice_status)) as invoice_status,
    coalesce(base_charge,0)::number(18,2) as base_charge,
    coalesce(service_charge,0)::number(18,2) as service_charge,
    coalesce(equipment_charge,0)::number(18,2) as equipment_charge,
    coalesce(extra_usage_charge,0)::number(18,2) as extra_usage_charge,
    coalesce(discount,0)::number(18,2) as discount,
    coalesce(tax,0)::number(18,2) as tax,
    total_amount::number(18,2) as total_amount,
    coalesce(previous_month_amount,0)::number(18,2) as previous_month_amount,
    coalesce(amount_change,0)::number(18,2) as amount_change,
    coalesce(amount_change_pct,0)::number(18,4) as amount_change_pct,
    _airbyte_extracted_at as source_extracted_at
from deduplicated
