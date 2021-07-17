with addresses_source as (

    select *
    from {{ ref('addresses_base') }}

), renamed_and_sorted as (

    select
        order_id,
        user_id,
        name,
        address
)

select * from renamed_and_sorted

--going to be dim_addresses