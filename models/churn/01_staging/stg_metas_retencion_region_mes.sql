{{ config(materialized='view', schema='DBT_STAGING', tags=['staging','depuracion']) }}

with source_data as (
  select * from {{ source('raw_churn','metas_retencion_region_mes') }}
),
deduplicated as (
  select * from source_data
  where nullif(trim(target_id),'') is not null
    and regexp_like(trim(year_month),'^[0-9]{4}-(0[1-9]|1[0-2])$')
  qualify row_number() over (
    partition by trim(target_id)
    order by _airbyte_extracted_at desc, _airbyte_generation_id desc
  ) = 1
)
select
  trim(target_id) as target_id,
  upper(trim(region)) as region,
  trim(year_month) as year_month,
  to_date(trim(year_month) || '-01') as month_start,
  try_to_decimal(regexp_replace(arpu_target_bob,'[^0-9.-]',''),18,2) as arpu_target_bob,
  try_to_number(regexp_replace(active_base_target,'[^0-9.-]',''))::integer as active_base_target,
  try_to_decimal(regexp_replace(retention_budget_bob,'[^0-9.-]',''),18,2) as retention_budget_bob,
  try_to_decimal(regexp_replace(churn_rate_target_pct,'[^0-9.-]',''),18,4) as churn_rate_target_pct,
  try_to_number(regexp_replace(retained_customers_target,'[^0-9.-]',''))::integer as retained_customers_target,
  upper(trim(responsable_comercial)) as commercial_owner,
  nullif(trim(comentario),'') as comment_text,
  try_to_timestamp_ntz(ultima_actualizacion) as last_updated_at,
  _airbyte_extracted_at as source_extracted_at
from deduplicated

