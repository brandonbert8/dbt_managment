{{ config(materialized='view', schema='DBT_STAGING', tags=['staging','depuracion']) }}

with source_data as (
  select * from {{ source('raw_churn','xref_customer_id') }}
)
select
  trim(account_id) as account_id,
  trim(customer_id) as customer_id,
  upper(trim(source_crm)) as source_crm,
  upper(trim(source_billing)) as source_billing,
  _airbyte_extracted_at as source_extracted_at
from source_data
where nullif(trim(account_id),'') is not null
  and nullif(trim(customer_id),'') is not null
qualify row_number() over (
  partition by trim(account_id), trim(customer_id)
  order by _airbyte_extracted_at desc, _airbyte_generation_id desc
) = 1
