{{ config(materialized='table', schema='DBT_MARTS', tags=['mart','kimball','r3','powerbi']) }}

select
  *,
  (case when billing_amount_change > 0 then 1 else 0 end
   + case when failed_payment_attempts > 0 then 1 else 0 end
   + case when dropped_connections > 0 or outage_seconds > 0 then 1 else 0 end
   + case when ticket_count > 0 then 1 else 0 end) as friction_score,
  current_timestamp() as dbt_loaded_at
from {{ ref('int_r3_friction_monthly') }}
