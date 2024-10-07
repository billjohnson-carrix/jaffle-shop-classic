{%- set payment_methods = ['credit_card', 'gift_card', 'bank_transfer', 'coupon'] -%}

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

, joined as (
    select
        customers.id as customer_id
        , customers.first_name
        , customers.last_name
        , orders.id as order_id
        , orders.order_date
        , orders.status as order_status
        , payments.id as payment_id
        , payments.paymentmethod as payment_method
        , payments.status as payment_status
        , payments.amount
        , payments.created as payment_created
    from customers
    left join orders on
        customers.id = orders.user_id
    left join payments on
        orders.id = payments.orderid
)

, pivoted as (
    select
        order_id
        {#- A jinja comment -#}
        {%- for payment_method in payment_methods %}
        , sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount
        {%- endfor %}
    from joined
    where payment_status = 'success'
    group by order_id
)

select *
from pivoted

