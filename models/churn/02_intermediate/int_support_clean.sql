{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['silver','deep_cleaning','support']) }}

-- Silver: tipado, limpieza y deduplicacion trasladados desde Bronze.
with bronze_prepared as (
    with source_data as (select * from {{ ref('stg_support_tickets') }}),
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
),
normalized as (
    select
        ticket_id,
        customer_id,
        null::varchar as contact_id,
        try_to_timestamp_ntz(to_varchar(created_at)) as created_timestamp,
        try_to_timestamp_ntz(to_varchar(resolved_at)) as resolved_timestamp_source,
        case
            when upper(trim(ticket_status)) in ('CLOSED','CERRADO') then 'CLOSED'
            when upper(trim(ticket_status)) in ('RESOLVED','RESUELTO') then 'RESOLVED'
            when upper(trim(ticket_status)) in ('OPEN','ABIERTO') then 'OPEN'
            when upper(trim(ticket_status)) in ('PENDING','PENDIENTE') then 'PENDING'
            else 'UNKNOWN'
        end as status,
        coalesce(nullif(upper(trim(channel)), ''), 'UNKNOWN') as channel,
        coalesce(nullif(upper(trim(category)), ''), 'UNKNOWN') as category,
        coalesce(nullif(upper(trim(priority)), ''), 'UNKNOWN') as priority,
        coalesce(nullif(upper(trim(reason)), ''), 'UNKNOWN') as reason,
        coalesce(nullif(upper(trim(agent_team)), ''), 'UNKNOWN') as agent_team,
        coalesce(reopened, false) as reopened,
        interaction_count,
        resolution_minutes,
        satisfaction_score,
        coalesce(first_contact_resolution, false) as first_contact_resolution,
        source_extracted_at::timestamp_ltz as source_extracted_at
    from bronze_prepared
    where ticket_id is not null
      and customer_id is not null
),
cleaned as (
    select
        *,
        iff(resolved_timestamp_source is null or resolved_timestamp_source >= created_timestamp,
            resolved_timestamp_source, null) as resolved_timestamp,
        greatest(coalesce(interaction_count, 0), 0)::integer as interaction_count_clean,
        greatest(
            coalesce(
                resolution_minutes,
                iff(resolved_timestamp_source >= created_timestamp,
                    datediff(minute, created_timestamp, resolved_timestamp_source), null),
                0
            ),
            0
        )::number(18,2) as resolution_minutes_clean,
        iff(satisfaction_score between 0 and 5, satisfaction_score, null)::number(18,2)
            as satisfaction_score_clean
    from normalized
)
select
    ticket_id,
    customer_id,
    contact_id,
    created_timestamp,
    resolved_timestamp,
    resolved_timestamp_source,
    status,
    channel,
    category,
    priority,
    reason,
    agent_team,
    case
        when category like '%CANCEL%' or reason like '%CANCEL%'
          or category like '%CHURN%' or reason like '%CHURN%' then 'RETENTION_RISK'
        when category like '%BILL%' or reason like '%PAY%' then 'BILLING'
        when category like '%NETWORK%' or category like '%OUTAGE%'
          or reason like '%SIGNAL%' then 'NETWORK'
        when category like '%TECH%' then 'TECHNICAL'
        else 'GENERAL'
    end as classification,
    reopened,
    interaction_count_clean as interaction_count,
    resolution_minutes_clean as resolution_minutes_num,
    satisfaction_score_clean as satisfaction_score,
    first_contact_resolution,
    case
        when satisfaction_score is null then 'MISSING'
        when satisfaction_score between 0 and 5 then 'VALID'
        else 'OUT_OF_RANGE_TO_NULL'
    end as satisfaction_dq_status,
    resolved_timestamp_source is null or resolved_timestamp_source >= created_timestamp
        as dq_valid_resolution_timestamp,
    resolution_minutes is null or resolution_minutes >= 0 as dq_valid_resolution_minutes,
    case
        when created_timestamp is null then 'INVALID_CREATED_TIMESTAMP'
        when created_timestamp::date > current_date() then 'FUTURE_TICKET_EXCLUDED'
        when resolved_timestamp_source < created_timestamp then 'INVALID_RESOLUTION_TO_NULL'
        when resolution_minutes < 0 then 'NEGATIVE_DURATION_CORRECTED'
        when satisfaction_score not between 0 and 5 then 'SATISFACTION_OUTLIER_TO_NULL'
        when status in ('RESOLVED','CLOSED') and resolved_timestamp is null then 'CLOSED_WITHOUT_VALID_RESOLUTION'
        else 'VALID'
    end as support_dq_status,
    case
        when resolved_timestamp is null then 'OPEN_OR_UNRESOLVED'
        when priority in ('CRITICAL','URGENT') and resolution_minutes_clean > 240 then 'SLA_BREACH'
        when priority = 'HIGH' and resolution_minutes_clean > 480 then 'SLA_BREACH'
        when resolution_minutes_clean > 1440 then 'SLA_BREACH'
        else 'WITHIN_SLA'
    end as sla_status,
    false as source_is_deleted,
    null::varchar as source_raw_id,
    source_extracted_at,
    null::number as source_generation_id
from cleaned
where created_timestamp is not null
  and created_timestamp::date <= current_date()
qualify row_number() over (partition by ticket_id order by source_extracted_at desc) = 1
