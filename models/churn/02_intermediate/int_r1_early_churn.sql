{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['intermediate','hefesto','r1']) }}

select
  c.customer_id,
  c.region,
  c.customer_segment,
  c.gender,
  c.age,
  s.subscription_id,
  s.plan_name,
  s.plan_tier,
  s.contract_type,
  s.contract_start_date,
  cl.churned,
  cl.churn_date,
  cl.churn_reason,
  cl.tenure_months_at_end,
  iff(coalesce(cl.tenure_months_at_end,
      datediff('month',s.contract_start_date,current_date())) between 0 and 12,
      true,false) as is_first_12_months
from {{ ref('stg_crm_customers') }} c
join {{ ref('stg_subscriptions_contracts') }} s using (customer_id)
left join {{ ref('stg_churn_labels') }} cl using (customer_id)
where coalesce(cl.tenure_months_at_end,
      datediff('month',s.contract_start_date,current_date())) between 0 and 12
