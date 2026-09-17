{{ config(materialized='table', schema='DBT_MARTS', tags=['mart','kimball','r2','powerbi']) }}

select
  *,
  case migration_priority
    when 'HIGH' then 'OFFER_TWO_YEAR'
    when 'MEDIUM' then 'OFFER_ONE_YEAR'
    when 'LOW' then 'NURTURE'
    else 'EXCLUDE'
  end as recommended_action,
  current_timestamp() as dbt_loaded_at
from {{ ref('int_r2_contract_opportunity') }}
