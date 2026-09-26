{{ config(materialized='incremental',schema='DBT_MARTS',unique_key='marketing_interaction_key',incremental_strategy='merge',on_schema_change='sync_all_columns',tags=['gold','fact']) }}
select
    {{ generate_surrogate_key(['m.interaction_id']) }}
        as marketing_interaction_key,
    {{ generate_surrogate_key(['m.customer_id']) }} as customer_key,
    {{ generate_surrogate_key(['c.region']) }} as region_key,
    iff(
        m.campaign_id is not null,
        {{ generate_surrogate_key(['m.campaign_id']) }},
        null
    ) as campaign_key,
    to_number(to_char(m.contact_date_parsed, 'YYYYMMDD')) as contact_date_key,
    m.interaction_id,
    m.contact_date_parsed,
    m.response_date_parsed,
    m.channel_norm,
    m.offer_type_norm,
    m.accepted_bool,
    m.upgrade_offered,
    m.equipment_upgrade,
    m.discount_pct_num,
    m.free_months_num,
    m.campaign_cost_num,
    m.estimated_customer_value_num,
    m.retained_30d,
    m.retained_60d,
    m.retained_90d,
    m.retention_reason,
    iff(m.accepted_bool, 1, 0) as accepted_count,
    1 as contact_count,
    m.dq_valid_discount,
    m.dq_valid_response_date,
    m.has_post_outcome_data,
    m.source_raw_id,
    m.source_extracted_at,
    m.source_generation_id
from {{ ref('int_marketing_clean') }} m
left join {{ ref('int_customer_clean') }} c using (customer_id)
{% if is_incremental() %}
    where
        m.source_extracted_at
        > (
            select
                coalesce(max(source_extracted_at), '1900-01-01'::timestamp_tz)
            from {{ this }}
        )
{% endif %}
