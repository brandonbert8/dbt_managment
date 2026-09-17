{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['intermediate','hefesto','r4']) }}

select
  m.interaction_id,
  m.campaign_id,
  m.campaign_name,
  m.customer_id,
  c.region,
  c.customer_segment,
  m.contact_date,
  to_char(m.contact_date,'YYYY-MM') as year_month,
  m.channel,
  m.offer_type,
  m.discount_pct,
  m.free_months,
  m.campaign_cost,
  m.estimated_customer_value,
  m.accepted,
  m.retained_30d,
  m.retained_60d,
  m.retained_90d,
  m.upgrade_offered,
  m.equipment_upgrade,
  m.retention_reason,
  iff(m.accepted, m.estimated_customer_value - m.campaign_cost, -m.campaign_cost) as estimated_net_value
from {{ ref('stg_marketing_retention') }} m
left join {{ ref('stg_crm_customers') }} c using (customer_id)

