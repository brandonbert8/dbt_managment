select
 order_key,
 line_number,
 extended_price,
 item_discount_amount
from {{ ref('order_item') }}
where abs(item_discount_amount) > extended_price