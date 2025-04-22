{% set raw_customers = source('landing', 'raw_customers') %}

with customers as (
    select
        {{ dbt_utils.star(from=raw_customers) }}
    from {{ raw_customers }}
), 

final as (
    select
        *
    from customers
)

select * from final
