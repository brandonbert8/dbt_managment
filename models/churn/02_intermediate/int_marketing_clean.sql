{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['silver','deep_cleaning','marketing']) }}

-- Silver: tipado, limpieza y deduplicacion trasladados desde Bronze.
with bronze_prepared as (
    with source_data as (select * from {{ ref('stg_marketing_retention') }}),
    deduplicated as (
     select * from source_data
     where nullif(trim(interaction_id),'') is not null and nullif(trim(customer_id),'') is not null and contact_date is not null
     qualify row_number() over (partition by trim(interaction_id) order by _airbyte_extracted_at desc, _airbyte_generation_id desc)=1
    )
    select trim(interaction_id) as interaction_id, trim(customer_id) as customer_id,
     trim(campaign_id) as campaign_id, initcap(trim(campaign_name)) as campaign_name,
     upper(trim(channel)) as channel, upper(trim(offer_type)) as offer_type,
     contact_date, response_date, to_char(contact_date,'YYYY-MM') as year_month,
     coalesce(accepted,false) as accepted,
     coalesce(retained_30d,false) as retained_30d, coalesce(retained_60d,false) as retained_60d,
     coalesce(retained_90d,false) as retained_90d, coalesce(upgrade_offered,false) as upgrade_offered,
     coalesce(equipment_upgrade,false) as equipment_upgrade,
     greatest(coalesce(discount_pct,0),0)::number(18,4) as discount_pct,
     greatest(coalesce(free_months,0),0)::integer as free_months,
     greatest(coalesce(campaign_cost,0),0)::number(18,2) as campaign_cost,
     greatest(coalesce(estimated_customer_value,0),0)::number(18,2) as estimated_customer_value,
     nullif(upper(trim(retention_reason)),'') as retention_reason,
     _airbyte_extracted_at as source_extracted_at
    from deduplicated
),
normalized as (
    select
        interaction_id,
        customer_id,
        nullif(trim(campaign_id), '') as campaign_id,
        coalesce(nullif(initcap(trim(campaign_name)), ''), 'Unknown') as campaign_name,
        try_to_date(to_varchar(contact_date)) as contact_date_parsed,
        try_to_date(to_varchar(response_date)) as response_date_source,
        coalesce(nullif(upper(trim(channel)), ''), 'UNKNOWN') as channel_norm,
        coalesce(nullif(upper(trim(offer_type)), ''), 'UNKNOWN') as offer_type_norm,
        coalesce(accepted, false) as accepted_bool,
        coalesce(upgrade_offered, false) as upgrade_offered,
        coalesce(equipment_upgrade, false) as equipment_upgrade,
        discount_pct,
        free_months,
        campaign_cost,
        estimated_customer_value,
        coalesce(retained_30d, false) as retained_30d_source,
        coalesce(retained_60d, false) as retained_60d_source,
        coalesce(retained_90d, false) as retained_90d_source,
        nullif(upper(trim(retention_reason)), '') as retention_reason,
        source_extracted_at::timestamp_ltz as source_extracted_at
    from bronze_prepared
    where interaction_id is not null
      and customer_id is not null
),
cleaned as (
    select
        *,
        iff(
            response_date_source is null
            or (response_date_source >= contact_date_parsed and response_date_source <= current_date()),
            response_date_source,
            null
        ) as response_date_parsed,
        least(greatest(coalesce(discount_pct, 0), 0), 100)::number(18,4) as discount_pct_num,
        least(greatest(coalesce(free_months, 0), 0), 24)::integer as free_months_num,
        greatest(coalesce(campaign_cost, 0), 0)::number(18,2) as campaign_cost_num,
        greatest(coalesce(estimated_customer_value, 0), 0)::number(18,2) as estimated_customer_value_num,
        (retained_30d_source or retained_60d_source or retained_90d_source) as retained_30d,
        (retained_60d_source or retained_90d_source) as retained_60d,
        retained_90d_source as retained_90d
    from normalized
)
select
    interaction_id,
    customer_id,
    campaign_id,
    campaign_name,
    contact_date_parsed,
    response_date_parsed,
    response_date_source,
    channel_norm,
    offer_type_norm,
    accepted_bool,
    upgrade_offered,
    equipment_upgrade,
    discount_pct_num,
    free_months_num,
    campaign_cost_num,
    estimated_customer_value_num,
    retained_30d,
    retained_60d,
    retained_90d,
    retained_30d_source,
    retained_60d_source,
    retained_90d_source,
    retention_reason,
    iff(campaign_cost_num > 0,
        (estimated_customer_value_num - campaign_cost_num) / campaign_cost_num,
        null)::number(18,4) as estimated_campaign_roi,
    discount_pct is null or discount_pct between 0 and 100 as dq_valid_discount,
    response_date_source is null
        or (response_date_source >= contact_date_parsed and response_date_source <= current_date())
        as dq_valid_response_date,
    not (retained_90d_source and not retained_60d_source)
        and not (retained_60d_source and not retained_30d_source)
        as dq_retention_windows_monotonic,
    case
        when contact_date_parsed is null then 'INVALID_CONTACT_DATE'
        when contact_date_parsed > current_date() then 'FUTURE_CONTACT_EXCLUDED'
        when response_date_source < contact_date_parsed or response_date_source > current_date()
            then 'INVALID_RESPONSE_DATE_TO_NULL'
        when discount_pct not between 0 and 100 then 'DISCOUNT_CAPPED'
        when free_months > 24 then 'FREE_MONTHS_CAPPED'
        when (retained_90d_source and not retained_60d_source)
          or (retained_60d_source and not retained_30d_source)
            then 'RETENTION_WINDOWS_CORRECTED'
        else 'VALID'
    end as marketing_dq_status,
    response_date_source is not null
        or retained_30d_source or retained_60d_source or retained_90d_source
        or retention_reason is not null as has_post_outcome_data,
    false as source_is_deleted,
    null::varchar as source_raw_id,
    source_extracted_at,
    null::number as source_generation_id
from cleaned
where contact_date_parsed is not null
  and contact_date_parsed <= current_date()
qualify row_number() over (partition by interaction_id order by source_extracted_at desc) = 1
