{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['intermediate','hefesto','r2']) }}

select
  c.customer_id,
  c.region,
  c.customer_segment,
  s.subscription_id,
  s.plan_name,
  s.plan_tier,
  s.contract_type,
  s.base_monthly_price,
  s.contract_start_date,
  datediff('month',s.contract_start_date,current_date()) as tenure_months,
  coalesce(cl.churned,0) as churned,
  case
    when coalesce(cl.churned,0)=1 then 'NO_ACTION_CHURNED'
    when datediff('month',s.contract_start_date,current_date())>=12 then 'HIGH'
    when datediff('month',s.contract_start_date,current_date())>=6 then 'MEDIUM'
    else 'LOW'
  end as migration_priority
from {{ ref('stg_crm_customers') }} c
join {{ ref('stg_subscriptions_contracts') }} s using (customer_id)
left join {{ ref('stg_churn_labels') }} cl using (customer_id)
where s.contract_type='MONTH_TO_MONTH'
