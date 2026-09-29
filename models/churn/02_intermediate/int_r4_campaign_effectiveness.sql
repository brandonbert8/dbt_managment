{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['intermediate','hefesto','r4']) }}

select
  m.interaction_id,
  m.campaign_id,
  m.campaign_name,
  m.customer_id,
  c.region,
  c.customer_segment,
  m.contact_date_parsed as contact_date,
  to_char(m.contact_date_parsed,'YYYY-MM') as year_month,
  m.channel_norm as channel,
  m.offer_type_norm as offer_type,
  m.discount_pct_num as discount_pct,
  m.free_months_num as free_months,
  m.campaign_cost_num as campaign_cost,
  m.estimated_customer_value_num as estimated_customer_value,
  m.accepted_bool as accepted,
  m.retained_30d,
  m.retained_60d,
  m.retained_90d,
  m.upgrade_offered,
  m.equipment_upgrade,
  m.retention_reason,
  iff(m.accepted_bool, m.estimated_customer_value_num - m.campaign_cost_num, -m.campaign_cost_num) as estimated_net_value
from {{ ref('int_marketing_clean') }} m
left join {{ ref('int_customer_clean') }} c using (customer_id)

