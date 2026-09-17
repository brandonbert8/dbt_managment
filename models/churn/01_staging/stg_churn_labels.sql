{{ config(materialized='view', schema='DBT_STAGING', tags=['staging','depuracion']) }}

with source_data as (
  select * from {{ source('raw_churn','churn_labels') }}
),
deduplicated as (
  select * from source_data
  where nullif(trim(customer_id),'') is not null
    and churned in (0,1)
  qualify row_number() over (
    partition by trim(customer_id)
    order by _airbyte_extracted_at desc, _airbyte_generation_id desc
  ) = 1
)
select
  trim(customer_id) as customer_id,
  churned::integer as churned,
  churn_date,
  upper(trim(churn_reason)) as churn_reason,
  upper(trim(customer_status)) as customer_status,
  greatest(coalesce(tenure_months_at_end,0),0)::integer as tenure_months_at_end,
  _airbyte_extracted_at as source_extracted_at
from deduplicated
