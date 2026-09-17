{{ config(materialized='table', schema='DBT_MARTS', tags=['mart','kimball','r4','powerbi']) }}

select
  year_month,
  region,
  campaign_id,
  campaign_name,
  channel,
  offer_type,
  count(*) as contacts,
  count_if(accepted) as accepted_offers,
  count_if(retained_30d) as retained_30d,
  count_if(retained_60d) as retained_60d,
  count_if(retained_90d) as retained_90d,
  round(100 * count_if(accepted) / nullif(count(*),0),2) as acceptance_rate_pct,
  round(100 * count_if(retained_90d) / nullif(count_if(accepted),0),2) as retention_90d_pct,
  sum(campaign_cost) as campaign_cost,
  sum(estimated_net_value) as estimated_net_value,
  current_timestamp() as dbt_loaded_at
from {{ ref('int_r4_campaign_effectiveness') }}
group by 1,2,3,4,5,6
