with payments_source as (

    select *
    from {{ ref('payments_base') }}

), renamed_and_converted as (

    select
        order_id,
        status,
        
        amount_cents / 100              as gross_amount,
        amount_shipping_cents / 100     as gross_shipping_amount,
        tax_amount_cents / 100          as gross_tax_amount

    from payments_source

), total_amount_added as (

    select *,
        gross_amount + gross_shipping_amount 
            + gross_tax_amount          as gross_total_amount

    from renamed_and_converted

)

select * from total_amount_added