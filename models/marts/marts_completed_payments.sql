with orders_source as (

    select *
    from {{ ref('stg_payments') }}

), aggregated_and_sorted as (

    select
        order_id,
        
        sum(gross_amount)                       as total_gross_amount,
        sum(gross_tax_amount)                   as total_gross_tax_amount,
        sum(gross_shipping_amount)              as total_gross_shipping_amount,
        sum(gross_total_amount)                 as gross_total_amount
    from orders_source
    where status = 'completed'
    group by order_id

) 

select * from aggregated_and_sorted