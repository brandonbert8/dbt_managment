{{ config(materialized='incremental',schema='DBT_MARTS',unique_key='support_ticket_key',incremental_strategy='merge',on_schema_change='sync_all_columns',tags=['gold','fact']) }}
select
    {{ generate_surrogate_key(['t.ticket_id']) }} as support_ticket_key,
    {{ generate_surrogate_key(['t.customer_id']) }} as customer_key,
    {{ generate_surrogate_key(['c.region']) }} as region_key,
    {{ generate_surrogate_key(["coalesce(nullif(t.agent_team,''),'UNKNOWN')"]) }}
        as agent_team_key,
    to_number(to_char(t.created_timestamp::date, 'YYYYMMDD'))
        as created_date_key,
    t.ticket_id,
    t.contact_id,
    t.created_timestamp,
    t.resolved_timestamp,
    t.status,
    t.channel,
    t.category,
    t.priority,
    t.reason,
    t.classification,
    t.reopened,
    t.interaction_count,
    t.resolution_minutes_num,
    t.satisfaction_score,
    t.first_contact_resolution,
    iff(t.first_contact_resolution, 1, 0) as first_contact_resolution_count,
    iff(t.reopened, 1, 0) as reopened_count,
    1 as ticket_count,
    t.satisfaction_dq_status,
    t.dq_valid_resolution_minutes,
    t.source_is_deleted,
    'PRE_OUTCOME_FEATURE' as ml_field_class,
    t.source_raw_id,
    t.source_extracted_at::timestamp_tz as source_extracted_at,
    t.source_generation_id
from {{ ref('int_support_clean') }} t
left join {{ ref('int_customer_clean') }} c
    using (customer_id)
{% if is_incremental() %}
    where
        t.source_extracted_at
        > (
            select
                coalesce(max(source_extracted_at), '1900-01-01'::timestamp_tz)
            from {{ this }}
        )
{% endif %}
