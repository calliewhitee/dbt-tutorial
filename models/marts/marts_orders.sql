with orders_source as (

    select *
    from {{ ref('orders_base') }}

), renamed_and_sorted as (

    select
        order_id,
        user_id,
        if(status != 'cancelled',
            min(order_id) over (partition by user_id 
                order by created_at),
            null) as first_completed_order_id,
        currency,
        status as order_status,
        if(status in ('paid', 'completed', 'shipped'), 
            'completed', 
            status) as order_status_category,
        shipping_method,
        created_at,
        updated_at,
        shipped_at,

        amount_total_cents / 100 as amount_total
    
    from orders_source

), added_user_type as (

    select 
        order_id,
        user_id,
        first_completed_order_id,
        if(first_completed_order_id = order_id, 'new', 'repeat') as user_type,
        currency,
        order_status,
        order_status_category,
        shipping_method,
        created_at,
        updated_at,
        shipped_at,

        amount_total

    from renamed_and_sorted

)

select * from added_user_type