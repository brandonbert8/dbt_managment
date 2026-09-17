{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['intermediate','crisp_dm','r5','anti_leakage']) }}

with subscriptions as (
  select * from {{ ref('stg_subscriptions_contracts') }}
  qualify row_number() over (
    partition by customer_id
    order by contract_start_date desc, subscription_id desc
  ) = 1
),
features as (
  select
    f.*,
    last_day(f.month_start) as observation_date,
    c.age,
    c.gender,
    c.customer_segment,
    c.is_senior_citizen,
    s.plan_tier,
    s.contract_type,
    s.base_monthly_price,
    m.ipc_index,
    m.unemployment_pct,
    m.monthly_inflation_pct,
    m.bob_usd_exchange_rate,
    m.tariff_shock
  from {{ ref('int_r3_friction_monthly') }} f
  left join {{ ref('stg_crm_customers') }} c using (customer_id)
  left join subscriptions s using (customer_id)
  left join {{ ref('stg_macro_monthly') }} m using (year_month)
)
select
  f.*,
  iff(
    cl.churn_date > f.observation_date
    and cl.churn_date <= dateadd('day',90,f.observation_date),
    1,0
  ) as label_churn_next_90d
from features f
left join {{ ref('stg_churn_labels') }} cl using (customer_id)
where cl.churn_date is null or cl.churn_date > f.observation_date
