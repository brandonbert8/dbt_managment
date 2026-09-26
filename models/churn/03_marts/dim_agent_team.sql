{{ config(materialized='table', schema='DBT_MARTS', tags=['gold','dimension']) }}

with normalized as (
    select distinct
        coalesce(nullif(upper(trim(agent_team)), ''), 'UNKNOWN') as agent_team_name
    from {{ ref('int_support_clean') }}
)
select
    {{ generate_surrogate_key(['agent_team_name']) }} as agent_team_key,
    agent_team_name
from normalized
