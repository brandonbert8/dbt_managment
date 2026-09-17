{{ config(materialized='view', schema='DBT_STAGING', tags=['staging','depuracion']) }}

with source_data as (
  select * from {{ source('raw_churn','macro_monthly') }}
)
select
  trim(year_month) as year_month,
  to_date(trim(year_month) || '-01') as month_start,
  ipc_index::number(18,4) as ipc_index,
  desempleo_pct::number(18,4) as unemployment_pct,
  inflacion_mensual::number(18,4) as monthly_inflation_pct,
  tipo_cambio_bob_usd::number(18,6) as bob_usd_exchange_rate,
  iff(coalesce(shock_tarifa,0)=1,true,false) as tariff_shock,
  _airbyte_extracted_at as source_extracted_at
from source_data
where regexp_like(trim(year_month),'^[0-9]{4}-(0[1-9]|1[0-2])$')
qualify row_number() over (
  partition by trim(year_month)
  order by _airbyte_extracted_at desc, _airbyte_generation_id desc
) = 1
