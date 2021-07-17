with devices_source as (

    select *
    from {{ ref('devices_base') }}

), determined_first_order_device as (

    select
        distinct cast(type_id as int64) as order_id,
        type,
        first_value(device) over (partition by type_id
            order by created_at rows between unbounded preceding 
                and unbounded following) as device,
        created_at,
        updated_at
    from devices_source
    where type = 'order'

), added_purchase_device_type as (

    select
        order_id,
        type,
        case 
            when device = 'web'
                then 'desktop'
            when device in ('ios-app', 'android-app')
                then 'mobile-app'
            when device in ('mobile', 'tablet')
                then 'mobile-web'
            when nullif(device, '') is null
                then 'unknown'
            else 'ERROR'
        end                                         as purchase_device_type,
        device,
        created_at,
        updated_at

    from determined_first_order_device

)

select * from added_purchase_device_type
