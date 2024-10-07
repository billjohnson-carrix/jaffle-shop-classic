with base_orders as (
    select *
    from {{ source('jaffle_shop','raw_orders') }}
)

, base_customers as (
    select *
    from {{ source('jaffle_shop','raw_customers') }}
)

, base_payments as (
    select *
    from {{ source('jaffle_shop','raw_payments') }}
)

, orders as (
    select 
        id as order_id
        , order_date
        , user_id as customer_id
        , status as order_status
        , count(id) over (partition by user_id) as order_count
        , case
            when status not in ('returned','return_pending')
            then order_date
        end as non_returned_order_date
    from base_orders
)

, customers as (
    select
        id as customer_id
        , first_name as givenname
        , last_name as surname
        , first_name || ' ' || last_name as full_name
    from base_customers
)

, payments as (
    select
        id as payment_id
        , orderid as order_id
        , amount
        , round(amount/100.0,2) as payment_amount
        , paymentmethod
        , status as payment_status
        , created as payment_created_date
    from base_payments
)

, joined_no_failed_payments as (
    select
        customers.*
        , orders.* exclude (customer_id)
        , payments.* exclude (order_id)
    from orders
    join customers on
        orders.customer_id = customers.customer_id
    left outer join payments on 
        orders.order_id = payments.order_id
    where payment_status != 'fail'
)

, customer_order_history as (
    select 
        customer_id,
        full_name,
        surname,
        givenname,
        min(order_date) as first_order_date,
        min(non_returned_order_date) as first_non_returned_order_date,
        max(non_returned_order_date) as most_recent_non_returned_order_date,
        order_count,
        sum(
            case
                when order_status not in ('returned','return_pending')
                then payment_amount
                else 0
            end
        ) as total_lifetime_value,
        count(
            case 
              when order_status not in ('returned','return_pending') 
              then 1 
            end
        ) as total_lifetime_order_count,
        total_lifetime_value / nullif(total_lifetime_order_count, 0) as avg_non_returned_order_value,
    from joined_no_failed_payments
    where order_status not in ('pending')
    group by 1, 2, 3, 4, 8
)

, final as (
    select 
        joined_no_failed_payments.order_id,
        joined_no_failed_payments.customer_id,
        joined_no_failed_payments.surname,
        joined_no_failed_payments.givenname,
        customer_order_history.first_order_date,
        customer_order_history.order_count,
        customer_order_history.total_lifetime_value,
        payment_amount as order_value_dollars,
        joined_no_failed_payments.order_status as order_status,
        joined_no_failed_payments.payment_status as payment_status
    from joined_no_failed_payments
    join customer_order_history on 
        joined_no_failed_payments.customer_id = customer_order_history.customer_id
)

select *
from final
