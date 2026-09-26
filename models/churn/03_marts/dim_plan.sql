{{ config(materialized='table', schema='DBT_MARTS', tags=['gold','dimension']) }}
select {{ generate_surrogate_key(['plan_name','plan_tier']) }} as plan_key, plan_name, plan_tier
from {{ ref('int_subscription_clean') }} group by plan_name,plan_tier
