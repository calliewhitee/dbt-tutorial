with payments_source as (

    select *
    from {{ ref('payments_base') }}

), renamed_and_sorted as (

    select
        payment_id,
        order_id,
        status,
        payment_type,
        created_at,

        amount_cents / 100 as amount,
        amount_shipped_cents / 100 as shipping_amount,
        tax_amount_cents / 100 as tax_amount
)

select * from renamed_and_sorted