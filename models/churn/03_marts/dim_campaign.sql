{{ config(materialized='table',schema='DBT_MARTS',tags=['gold','dimension']) }}
select {{ generate_surrogate_key(['campaign_id']) }} as campaign_key,campaign_id,campaign_name from {{ ref('int_marketing_clean') }} where campaign_id is not null qualify row_number() over(partition by campaign_id order by source_extracted_at desc)=1
