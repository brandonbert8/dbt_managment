{{ config(materialized='view', schema='DBT_STAGING', tags=['staging','depuracion']) }}
with source_data as (select * from {{ source('raw_churn','support_tickets') }}),
deduplicated as (
 select * from source_data
 where nullif(trim(ticket_id),'') is not null and nullif(trim(customer_id),'') is not null and _ab_cdc_deleted_at is null
 qualify row_number() over (partition by trim(ticket_id) order by _airbyte_extracted_at desc, _airbyte_generation_id desc)=1
)
select trim(ticket_id) as ticket_id, trim(customer_id) as customer_id,
 upper(trim(status)) as ticket_status, upper(trim(channel)) as channel,
 upper(trim(category)) as category, upper(trim(reason)) as reason, upper(trim(priority)) as priority,
 upper(trim(agent_team)) as agent_team, try_to_timestamp_ntz(created_at) as created_at,
 try_to_timestamp_ntz(to_varchar(resolved_at)) as resolved_at,
 to_char(try_to_timestamp_ntz(created_at),'YYYY-MM') as year_month,
 coalesce(try_to_boolean(reopened),false) as reopened,
 coalesce(try_to_boolean(first_contact_resolution),false) as first_contact_resolution,
 greatest(coalesce(try_to_number(to_varchar(resolution_minutes)),0),0)::number(18,2) as resolution_minutes,
 greatest(coalesce(interaction_count,0),0)::integer as interaction_count,
 satisfaction_score::number(18,2) as satisfaction_score,
 _airbyte_extracted_at as source_extracted_at
from deduplicated where try_to_timestamp_ntz(created_at) is not null
