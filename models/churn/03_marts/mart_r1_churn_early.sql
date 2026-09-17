{{ config(materialized='table', schema='DBT_MARTS', tags=['mart','kimball','r1','powerbi']) }}

select
  region,
  customer_segment,
  count(distinct customer_id) as customers_first_12_months,
  count_if(churned=1) as churned_customers,
  round(100 * count_if(churned=1) / nullif(count(distinct customer_id),0),2) as churn_rate_pct,
  avg(tenure_months_at_end) as avg_tenure_months,
  current_timestamp() as dbt_loaded_at
from {{ ref('int_r1_early_churn') }}
group by 1,2
