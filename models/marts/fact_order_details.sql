with orders_source as (

    select *
    from {{ ref('marts_orders') }}

), devices_source as (

    select *
    from {{ ref('dim_order_devices') }}

), addresses_source as (

    select *
    from {{ ref('dim_addresses') }}

), completed_payments_source as (

    select *
    from {{ ref('marts_completed_payments') }}

), combined_order_details as (

    select
        orders_source.order_id,
        orders_source.user_id,
        orders_source.created_at,
        orders_source.updated_at,
        orders_source.shipped_at,
        orders_source.currency,
        orders_source.order_status,
        orders_source.order_status_category,
        addresses_source.country_type,
        orders_source.shipping_method,
        devices_source.purchase_device_type,
        devices_source.device AS purchase_device,

        orders_source.amount_total,
        completed_payments_source.gross_total_amount,
        case
            when orders_source.currency = 'USD' 
                then orders_source.amount_total
            else completed_payments_source.gross_total_amount
        end as total_amount_usd,
        completed_payments_source.total_gross_tax_amount,
        completed_payments_source.total_gross_amount,
        completed_payments_source.total_gross_shipping_amount

    from orders_source
        left join devices_source
            on orders_source.order_id = devices_source.order_id
        left join addresses_source
            on orders_source.order_id = addresses_source.order_id
        left join completed_payments_source
            on orders_source.order_id = completed_payments_source.order_id

) 

select * from combined_order_details