{{ config(materialized='table',schema='DBT_MARTS',tags=['gold','dimension']) }}
select {{ generate_surrogate_key(['network_node']) }} as network_node_key,network_node from {{ ref('int_network_clean') }} group by network_node
