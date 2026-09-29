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
  iff(cl.customer_id is not null, 1, 0) as churned,
  cl.churn_date,
  cl.churn_reason,
  cl.tenure_months_at_end,
  iff(coalesce(cl.tenure_months_at_end,
      datediff('month',s.contract_start_date,current_date())) between 0 and 12,
      true,false) as is_first_12_months
from {{ ref('int_customer_clean') }} c
join {{ ref('int_subscription_clean') }} s using (customer_id)
left join {{ ref('int_churn_clean') }} cl using (customer_id)
where coalesce(cl.tenure_months_at_end,
      datediff('month',s.contract_start_date,current_date())) between 0 and 12
