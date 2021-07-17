with orders_source as (

    select *
    from {{ ref('orders_base') }}

), renamed_and_sorted as (

    select
        order_id,
        user_id,
        iff(status != 'cancelled',
            min(order_id) over (partition by user_id 
                order by created_at),
            null) as first_completed_order,
        currency,
        status as order_status,
        iff(status in ('paid', 'completed', 'shipped'), 
            'completed', 
            status) as order_status_category,
        shipping_method,
        created_at,
        updated_at,
        shipped_at,

        amount_total_cents
    
    from orders_source

