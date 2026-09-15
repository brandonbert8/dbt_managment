select
l_orderkey as order_key,
l_partkey as part_key,
l_quantity as quantity,
l_linenumber as line_number,
l_extendedprice as extended_price,
l_discount as discount_percentage,
l_tax as tax_rate 
from {{source('tpch','lineitem')}}