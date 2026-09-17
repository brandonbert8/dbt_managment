{{ config(materialized='view', schema='DBT_STAGING', tags=['staging','depuracion']) }}
with source_data as (select * from {{ source('raw_churn','marketing_retention') }}),
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
