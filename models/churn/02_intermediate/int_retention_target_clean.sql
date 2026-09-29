{{ config(materialized='view', schema='DBT_INTERMEDIATE', tags=['silver','deep_cleaning','retention_targets']) }}

-- Silver: tipado, limpieza y deduplicacion trasladados desde Bronze.
with bronze_prepared as (
    with source_data as (
      select * from {{ ref('stg_metas_retencion_region_mes') }}
    ),
    deduplicated as (
      select * from source_data
      where nullif(trim(target_id),'') is not null
        and regexp_like(trim(year_month),'^[0-9]{4}-(0[1-9]|1[0-2])$')
      qualify row_number() over (
        partition by trim(target_id)
        order by _airbyte_extracted_at desc, _airbyte_generation_id desc
      ) = 1
    )
    select
      trim(target_id) as target_id,
      upper(trim(region)) as region,
      trim(year_month) as year_month,
      to_date(trim(year_month) || '-01') as month_start,
      try_to_decimal(regexp_replace(arpu_target_bob,'[^0-9.-]',''),18,2) as arpu_target_bob,
      try_to_number(regexp_replace(active_base_target,'[^0-9.-]',''))::integer as active_base_target,
      try_to_decimal(regexp_replace(retention_budget_bob,'[^0-9.-]',''),18,2) as retention_budget_bob,
      try_to_decimal(regexp_replace(churn_rate_target_pct,'[^0-9.-]',''),18,4) as churn_rate_target_pct,
      try_to_number(regexp_replace(retained_customers_target,'[^0-9.-]',''))::integer as retained_customers_target,
      upper(trim(responsable_comercial)) as commercial_owner,
      nullif(trim(comentario),'') as comment_text,
      try_to_timestamp_ntz(ultima_actualizacion) as last_updated_at,
      _airbyte_extracted_at as source_extracted_at
    from deduplicated
),
normalized as (
    select
        target_id,
        coalesce(nullif(upper(trim(region)), ''), 'UNKNOWN') as region,
        trim(year_month) as year_month,
        try_to_date(to_varchar(month_start)) as month_start,
        arpu_target_bob,
        active_base_target,
        retention_budget_bob,
        churn_rate_target_pct,
        retained_customers_target,
        coalesce(nullif(upper(trim(commercial_owner)), ''), 'UNASSIGNED') as commercial_owner,
        try_to_timestamp_ntz(to_varchar(last_updated_at)) as last_updated_at,
        nullif(trim(comment_text), '') as comment_text,
        source_extracted_at::timestamp_ltz as source_extracted_at
    from bronze_prepared
    where target_id is not null
),
cleaned as (
    select
        *,
        greatest(coalesce(arpu_target_bob, 0), 0)::number(18,2) as arpu_clean,
        greatest(coalesce(active_base_target, 0), 0)::integer as active_base_clean,
        greatest(coalesce(retention_budget_bob, 0), 0)::number(18,2) as budget_clean,
        least(greatest(coalesce(churn_rate_target_pct, 0), 0), 100)::number(18,4) as churn_rate_clean,
        greatest(coalesce(retained_customers_target, 0), 0)::integer as retained_clean
    from normalized
)
select
    target_id,
    region,
    year_month,
    month_start,
    arpu_clean as arpu_target_bob_num,
    active_base_clean as active_base_target_num,
    budget_clean as retention_budget_bob_num,
    churn_rate_clean as churn_rate_target_pct_num,
    least(retained_clean, active_base_clean) as retained_customers_target_num,
    commercial_owner as responsable_comercial,
    last_updated_at as ultima_actualizacion_ts,
    comment_text as comentario,
    iff(active_base_clean > 0,
        100 * least(retained_clean, active_base_clean) / active_base_clean,
        null)::number(18,4) as retention_rate_target_pct_num,
    regexp_like(year_month, '^[0-9]{4}-(0[1-9]|1[0-2])$')
        and month_start = try_to_date(year_month || '-01') as dq_valid_year_month,
    case
        when arpu_target_bob is null then 'IMPUTED_ZERO'
        when arpu_target_bob < 0 then 'NEGATIVE_CORRECTED_TO_ZERO'
        else 'VALID'
    end as arpu_target_dq_status,
    case
        when churn_rate_target_pct is null then 'IMPUTED_ZERO'
        when churn_rate_target_pct < 0 then 'NEGATIVE_CORRECTED_TO_ZERO'
        when churn_rate_target_pct > 100 then 'CAPPED_AT_100'
        else 'VALID'
    end as churn_target_dq_status,
    case
        when retained_customers_target is null then 'IMPUTED_ZERO'
        when retained_customers_target < 0 then 'NEGATIVE_CORRECTED_TO_ZERO'
        when retained_clean > active_base_clean then 'CAPPED_TO_ACTIVE_BASE'
        else 'VALID'
    end as retained_target_dq_status,
    case
        when retention_budget_bob is null then 'IMPUTED_ZERO'
        when retention_budget_bob < 0 then 'NEGATIVE_CORRECTED_TO_ZERO'
        else 'VALID'
    end as budget_target_dq_status,
    false as source_is_deleted,
    null::varchar as source_raw_id,
    source_extracted_at,
    null::number as source_generation_id
from cleaned
where region is not null
  and month_start is not null
qualify row_number() over (
    partition by target_id
    order by last_updated_at desc nulls last, source_extracted_at desc
) = 1
