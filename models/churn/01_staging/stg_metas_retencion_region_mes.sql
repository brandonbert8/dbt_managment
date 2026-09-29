{{ config(materialized='view', schema='DBT_STAGING', tags=['bronze','raw']) }}

-- Bronze: replica logica RAW; conserva columnas, valores, duplicados y metadatos.
-- La limpieza, el tipado y las validaciones se aplican en Silver.
select *
from {{ source('raw_churn','metas_retencion_region_mes') }}
