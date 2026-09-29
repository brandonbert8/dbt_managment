{{ config(materialized='view', schema='DBT_STAGING', tags=['bronze','raw']) }}

-- Bronze RAW: conserva filas, valores y metadatos sin limpieza ni deduplicacion.
select *
from {{ source('raw_churn','xref_customer_id') }}
