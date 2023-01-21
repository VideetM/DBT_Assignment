with orders as (

    select * from {{ ref('stg_orders') }}

)

select * from orders



-- select o.order_id,o.order_created_at,p.product_category,p.sku as product_sku,p.option1 as product_style,
-- p.option2 as  product_size
-- from `ae_data_challenge_v1_vmalpe.stg_orders` o
-- inner join `ae_data_challenge_v1_vmalpe.stg_products` p
-- on o.product_id=p.product_id
-- and o.variant_id =p.variant_id
-- where p.option1 is not null and p.option2 is not null
-- and o.order_id='B1785115'
-- --order by order_id
-- limit 100