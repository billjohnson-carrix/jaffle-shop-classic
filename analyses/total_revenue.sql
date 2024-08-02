with payments as (
    select * from {{ ref('stg_payments') }}
)
select
    sum(amount) as total_revenue
from payments