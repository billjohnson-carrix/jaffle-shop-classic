with orders as (
    select *
    from {{ ref('raw_orders') }}
)

, payments as (
    select *
    from {{ ref('raw_payments') }}
)

, customers as (
    select *
    from {{ ref('raw_customers') }}
)

, silver_orders as (
    select
        id as order_id
        , user_id as customer_id
        , order_date
        , status as order_status
    from orders
)

, silver_payments as (
    select
        id as payment_id
        , orderid as order_id
        , paymentmethod as payment_method
        , status as payment_status
        , amount
        , created as created_date
    from payments
)

, silver_customers as (
    select
        id as customer_id
        , first_name
        , last_name
    from customers
)

, payments_successful as (
    select
        order_id
        , max(created_date) as payment_finalized_date
        , sum(amount) / 100.0 as total_amount_paid
    from silver_payments
    where payment_status <> 'fail'
    group by 1
)

, orders_joined as (
    select 
        silver_orders.order_id,
        silver_orders.customer_id,
        silver_orders.order_date,
        silver_orders.order_status,
        payments_successful.total_amount_paid,
        payments_successful.payment_finalized_date,
        silver_customers.first_name,
        silver_customers.last_name
from silver_orders
left join payments_successful on silver_orders.order_id = payments_successful.order_id
left join silver_customers on silver_orders.customer_id = silver_customers.customer_id 
)

, total_payment_per_order as (
    select
        p.order_id,
        sum(orders_joined.total_amount_paid) as total_paid
    from orders_joined p
    left join orders_joined on p.customer_id = orders_joined.customer_id and p.order_id >= orders_joined.order_id
    group by 1
    order by p.order_id
)

, customer_orders as (
    select c.customer_id
        , min(silver_orders.order_date) as first_order_date
        , max(silver_orders.order_date) as most_recent_order_date
        , count(silver_orders.order_id) as number_of_orders
    from silver_customers c 
    left join silver_orders
    on silver_orders.customer_id = c.customer_id 
    group by 1
)

, final as (
    select
        p.*,
        row_number() over (order by p.order_id) as transaction_seq,
        row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,
        case when c.first_order_date = p.order_date
            then 'new'
            else 'return' end as nvsr,
        total_payment_per_order.total_paid as customer_lifetime_value,
        c.first_order_date as fdos
    from orders_joined p
    left join customer_orders as c using (customer_id)
    left outer join total_payment_per_order on 
        total_payment_per_order.order_id = p.order_id
    order by order_id
)

select *
from final
