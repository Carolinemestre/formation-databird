with order_item_grouped_by_order as (

select order_id,
    sum(total_order_item_amount) as total_order_amount,
    sum(item_quantity) as total_items,
    count(distinct product_id) as total_distinct_items
from {{ ref('stg_sales_database__order_item') }}
group by order_id

), feedback_grouped_by_order as (

select order_id,
    avg(feedback_score) as average_feedback_score
from {{ ref('stg_sales_database__feedback') }}
group by order_id

)

select o.order_id,
    o.user_id,
    o.order_status,
    o.order_created_at,
    o.order_approved_at,
    u.user_city,
    f.average_feedback_score,
    oi.total_order_amount,
    oi.total_items,
    oi.total_distinct_items
from {{ ref('stg_sales_database__order') }} as o
left join order_item_grouped_by_order as oi on o.order_id = io.order_id
left join feedback_grouped_by_order as f on f.order_id = o.order_id
left join {{ ref('stg_sales_database__user' )}} as u on u.order_id = o.order_id
