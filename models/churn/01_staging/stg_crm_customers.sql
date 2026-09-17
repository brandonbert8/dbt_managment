{{ config(materialized='view', schema='DBT_STAGING', tags=['staging','depuracion']) }}

with source_data as (
    select * from {{ source('raw_churn','crm_customers') }}
), deduplicated as (
    select *
    from source_data
    where nullif(trim(customer_id),'') is not null
      and age between 18 and 120
    qualify row_number() over (
        partition by trim(customer_id)
        order by _airbyte_extracted_at desc, _airbyte_generation_id desc
    ) = 1
)
select
    trim(customer_id) as customer_id,
    sha2(coalesce(msisdn,''),256) as msisdn_hash,
    age::integer as age,
    upper(trim(gender)) as gender,
    initcap(trim(city)) as city,
    upper(trim(region)) as region,
    trim(postal_code) as postal_code,
    upper(trim(marital_status)) as marital_status,
    iff(coalesce(senior_citizen,0)=1,true,false) as is_senior_citizen,
    upper(trim(customer_segment)) as customer_segment,
    registration_date,
    iff(coalesce(dependents,0)=1,true,false) as has_dependents,
    coalesce(number_of_dependents,0)::integer as number_of_dependents,
    coalesce(upper(trim(estimated_income_band)),'UNKNOWN') as estimated_income_band,
    _airbyte_extracted_at as source_extracted_at
from deduplicated
