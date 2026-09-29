{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['silver','deep_cleaning','customer']) }}

-- Silver: tipado, limpieza y deduplicacion trasladados desde Bronze.
with bronze_prepared as (
    with source_data as (
        select * from {{ ref('stg_crm_customers') }}
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
),
normalized as (
    select
        customer_id,
        msisdn_hash,
        age::integer as age,
        case
            when upper(trim(gender)) in ('M','MALE','MASCULINO','HOMBRE') then 'MALE'
            when upper(trim(gender)) in ('F','FEMALE','FEMENINO','MUJER') then 'FEMALE'
            when upper(trim(gender)) in ('NON_BINARY','NO BINARIO','NB') then 'NON_BINARY'
            else 'UNKNOWN'
        end as gender,
        coalesce(nullif(initcap(trim(city)), ''), 'Unknown') as city,
        coalesce(nullif(upper(trim(region)), ''), 'UNKNOWN') as region,
        nullif(upper(trim(postal_code)), '') as postal_code,
        case
            when upper(trim(marital_status)) in ('MARRIED','CASADO','CASADA') then 'MARRIED'
            when upper(trim(marital_status)) in ('SINGLE','SOLTERO','SOLTERA') then 'SINGLE'
            when upper(trim(marital_status)) in ('DIVORCED','DIVORCIADO','DIVORCIADA') then 'DIVORCED'
            when upper(trim(marital_status)) in ('WIDOWED','VIUDO','VIUDA') then 'WIDOWED'
            else 'UNKNOWN'
        end as marital_status,
        coalesce(is_senior_citizen, false) as senior_citizen_source,
        coalesce(nullif(upper(trim(customer_segment)), ''), 'UNKNOWN') as customer_segment,
        try_to_date(to_varchar(registration_date)) as registration_date,
        coalesce(has_dependents, false) as has_dependents_source,
        number_of_dependents,
        coalesce(nullif(upper(trim(estimated_income_band)), ''), 'UNKNOWN') as estimated_income_band,
        source_extracted_at::timestamp_ltz as source_extracted_at
    from bronze_prepared
    where customer_id is not null
),
cleaned as (
    select
        *,
        greatest(coalesce(number_of_dependents, 0), 0)::integer as dependents_count,
        iff(age >= 65, true, false) as senior_citizen,
        iff(greatest(coalesce(number_of_dependents, 0), 0) > 0, true, false) as has_dependents
    from normalized
)
select
    customer_id,
    msisdn_hash,
    age,
    gender,
    city,
    region,
    postal_code,
    marital_status,
    senior_citizen,
    senior_citizen_source,
    customer_segment,
    registration_date,
    has_dependents,
    has_dependents_source,
    dependents_count,
    estimated_income_band,
    case
        when number_of_dependents is null then 'IMPUTED_ZERO'
        when number_of_dependents < 0 then 'CORRECTED_TO_ZERO'
        when has_dependents_source != has_dependents then 'BOOLEAN_ALIGNED_TO_COUNT'
        else 'VALID'
    end as dependents_dq_status,
    case
        when registration_date is null then 'MISSING'
        when registration_date > current_date() then 'FUTURE_TO_EXCLUDE'
        else 'VALID'
    end as registration_date_dq_status,
    case
        when senior_citizen_source != senior_citizen then 'FLAG_ALIGNED_TO_AGE'
        else 'VALID'
    end as senior_dq_status,
    case
        when age between 18 and 24 then '18_24'
        when age between 25 and 34 then '25_34'
        when age between 35 and 44 then '35_44'
        when age between 45 and 64 then '45_64'
        when age between 65 and 120 then '65_PLUS'
        else 'INVALID'
    end as age_band,
    datediff(month, registration_date, current_date())::integer as customer_tenure_months,
    false as source_is_deleted,
    null::varchar as source_raw_id,
    source_extracted_at,
    null::number as source_generation_id
from cleaned
where age between 18 and 120
  and registration_date is not null
  and registration_date <= current_date()
qualify row_number() over (partition by customer_id order by source_extracted_at desc) = 1
