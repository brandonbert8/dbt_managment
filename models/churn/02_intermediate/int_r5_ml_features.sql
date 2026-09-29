{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['intermediate','crisp_dm','r5','anti_leakage']) }}

with subscriptions as (
  select * from {{ ref('int_subscription_clean') }}
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
    c.senior_citizen as is_senior_citizen,
    s.plan_tier,
    s.contract_type,
    s.base_monthly_price,
    m.ipc_index,
    m.desempleo_pct as unemployment_pct,
    m.inflacion_mensual as monthly_inflation_pct,
    m.tipo_cambio_bob_usd as bob_usd_exchange_rate,
    iff(m.shock_tarifa = 1, true, false) as tariff_shock
  from {{ ref('int_r3_friction_monthly') }} f
  left join {{ ref('int_customer_clean') }} c using (customer_id)
  left join subscriptions s using (customer_id)
  left join {{ ref('int_macro_clean') }} m using (year_month)
)
select
  f.*,
  iff(
    cl.churn_date > f.observation_date
    and cl.churn_date <= dateadd('day',90,f.observation_date),
    1,0
  ) as label_churn_next_90d
from features f
left join {{ ref('int_churn_clean') }} cl using (customer_id)
where cl.churn_date is null or cl.churn_date > f.observation_date
