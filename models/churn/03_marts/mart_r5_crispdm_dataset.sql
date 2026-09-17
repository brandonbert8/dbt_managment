{{ config(materialized='table', schema='DBT_MARTS', tags=['mart','crisp_dm','r5','anti_leakage','ml']) }}

select
  *,
  current_timestamp() as dbt_loaded_at
from {{ ref('int_r5_ml_features') }}
