with addresses_source as (

    select *
    from {{ ref('addresses_base') }}

), renamed_and_sorted as (

    select
        order_id,
        user_id,
        name,
        address,
        case
            when country_code is null 
                then 'Null country'
            when country_code = 'US'
                then 'US'
            when country_code != 'US'
                then 'International'
        end                                 as country_type

    from addresses_source

)

select * from renamed_and_sorted
