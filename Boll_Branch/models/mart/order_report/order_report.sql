
with final as (
SELECT * 
from {{ ref('int_orders_products') }})

select * from final