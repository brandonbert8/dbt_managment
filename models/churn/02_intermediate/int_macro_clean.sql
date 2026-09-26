{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['silver','deep_cleaning','macro']) }}

with normalized as (
    select
        trim(year_month) as year_month,
        try_to_date(to_varchar(month_start)) as month_start,
        ipc_index::float as ipc_source,
        unemployment_pct::float as unemployment_source,
        monthly_inflation_pct::float as inflation_source,
        bob_usd_exchange_rate::float as exchange_rate_source,
        coalesce(tariff_shock, false) as tariff_shock,
        source_extracted_at::timestamp_ltz as source_extracted_at
    from {{ ref('stg_macro_monthly') }}
)
select
    year_month,
    month_start,
    iff(ipc_source > 0 and ipc_source < 10000, ipc_source, null)::float as ipc_index,
    iff(unemployment_source between 0 and 100, unemployment_source, null)::float as desempleo_pct,
    iff(inflation_source between -50 and 100, inflation_source, null)::float as inflacion_mensual,
    iff(exchange_rate_source > 0 and exchange_rate_source < 100, exchange_rate_source, null)::float
        as tipo_cambio_bob_usd,
    iff(tariff_shock, 1, 0) as shock_tarifa,
    regexp_like(year_month, '^[0-9]{4}-(0[1-9]|1[0-2])$')
        and month_start = try_to_date(year_month || '-01') as dq_valid_year_month,
    ipc_source > 0 and ipc_source < 10000 as dq_valid_ipc,
    unemployment_source between 0 and 100 as dq_valid_unemployment,
    inflation_source between -50 and 100 as dq_valid_inflation,
    exchange_rate_source > 0 and exchange_rate_source < 100 as dq_valid_exchange_rate,
    case
        when not regexp_like(year_month, '^[0-9]{4}-(0[1-9]|1[0-2])$') then 'INVALID_YEAR_MONTH'
        when month_start > date_trunc('month', current_date()) then 'FUTURE_MONTH_EXCLUDED'
        when ipc_source is null or unemployment_source is null
          or inflation_source is null or exchange_rate_source is null then 'MISSING_INDICATOR_TO_NULL'
        when not (ipc_source > 0 and ipc_source < 10000)
          or not (unemployment_source between 0 and 100)
          or not (inflation_source between -50 and 100)
          or not (exchange_rate_source > 0 and exchange_rate_source < 100) then 'OUTLIER_TO_NULL'
        else 'VALID'
    end as macro_dq_status,
    false as source_is_deleted,
    null::varchar as source_raw_id,
    source_extracted_at,
    null::number as source_generation_id
from normalized
where month_start is not null
  and month_start <= date_trunc('month', current_date())
qualify row_number() over (partition by year_month order by source_extracted_at desc) = 1
