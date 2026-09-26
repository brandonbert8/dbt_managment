{{ config(materialized='table', schema='DBT_MARTS', tags=['gold','dimension']) }}
select {{ generate_surrogate_key(['region']) }} as region_key, region as region_name
from {{ ref('int_customer_clean') }} group by region
