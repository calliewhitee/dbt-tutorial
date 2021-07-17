with orders_source as (

    select *
    from {{ ref('stg_orders') }}

), aggregated_and_sorted as (

    select
        order_id,
        sum(iff(status = 'completed', tax_amount_cents, 0) as gross_tax_amount_cents,
        sum(iff(status = 'completed', amount_cents, 0) as gross_amount_cents,
        sum(iff(status = 'completed', amount_shipping_cents, 0) as gross_shipping_amount_cents,
        sum(iff(status = 'completed', 
            tax_amount_cents + amount_cents + amount_shipping_cents,
            0) as gross_total_amount_cents
    from orders_source