with devices_source as (

    select *
    from {{ ref('devices_base') }}

), renamed_and_sorted as (

    select
        distinct cast(type_id as int64) as order_id,
        type,
        first_value(device) over (partition by type_id
            order by created_at rows between unbounded preceding 
                and unbounded following) as device
        created_at,
        updated_at
    from devices_source
    where type = 'order'


        
        
)

select * from renamed_and_sorted

--going to be a dim table