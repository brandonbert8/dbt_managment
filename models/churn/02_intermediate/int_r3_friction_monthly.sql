{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['intermediate','hefesto','r3']) }}

with invoices as (
  select customer_id, date_trunc('month',issue_date)::date as month_start,
         sum(total_amount) as billed_amount,
         sum(amount_change) as billing_amount_change
  from {{ ref('stg_billing_invoices') }}
  group by 1,2
),
payments as (
  select i.customer_id, date_trunc('month',p.payment_date)::date as month_start,
         sum(p.failed_attempts) as failed_payment_attempts,
         sum(p.outstanding_balance) as outstanding_balance,
         sum(p.days_late) as total_days_late
  from {{ ref('stg_payments') }} p
  join {{ ref('stg_billing_invoices') }} i using (invoice_id)
  group by 1,2
),
usage_data as (
  select customer_id, date_trunc('month',usage_date)::date as month_start,
         sum(data_download_gb + data_upload_gb) as total_data_gb,
         sum(voice_minutes) as voice_minutes
  from {{ ref('stg_service_usage') }}
  group by 1,2
),
network as (
  select customer_id, date_trunc('month',event_timestamp)::date as month_start,
         count_if(connection_dropped) as dropped_connections,
         sum(outage_duration_seconds) as outage_seconds,
         avg(latency_ms) as avg_latency_ms,
         avg(packet_loss_pct) as avg_packet_loss_pct
  from {{ ref('stg_network_quality') }}
  group by 1,2
),
support as (
  select customer_id, date_trunc('month',created_at)::date as month_start,
         count(*) as ticket_count,
         avg(resolution_minutes) as avg_resolution_minutes,
         avg(satisfaction_score) as avg_satisfaction_score
  from {{ ref('stg_support_tickets') }}
  group by 1,2
),
spine as (
  select customer_id,month_start from invoices union
  select customer_id,month_start from payments union
  select customer_id,month_start from usage_data union
  select customer_id,month_start from network union
  select customer_id,month_start from support
)
select
  s.customer_id,
  to_char(s.month_start,'YYYY-MM') as year_month,
  s.month_start,
  c.region,
  coalesce(i.billed_amount,0) as billed_amount,
  coalesce(i.billing_amount_change,0) as billing_amount_change,
  coalesce(p.failed_payment_attempts,0) as failed_payment_attempts,
  coalesce(p.outstanding_balance,0) as outstanding_balance,
  coalesce(p.total_days_late,0) as total_days_late,
  coalesce(u.total_data_gb,0) as total_data_gb,
  coalesce(u.voice_minutes,0) as voice_minutes,
  coalesce(n.dropped_connections,0) as dropped_connections,
  coalesce(n.outage_seconds,0) as outage_seconds,
  coalesce(n.avg_latency_ms,0) as avg_latency_ms,
  coalesce(n.avg_packet_loss_pct,0) as avg_packet_loss_pct,
  coalesce(t.ticket_count,0) as ticket_count,
  coalesce(t.avg_resolution_minutes,0) as avg_resolution_minutes,
  coalesce(t.avg_satisfaction_score,0) as avg_satisfaction_score
from spine s
left join {{ ref('stg_crm_customers') }} c using (customer_id)
left join invoices i using (customer_id,month_start)
left join payments p using (customer_id,month_start)
left join usage_data u using (customer_id,month_start)
left join network n using (customer_id,month_start)
left join support t using (customer_id,month_start)

