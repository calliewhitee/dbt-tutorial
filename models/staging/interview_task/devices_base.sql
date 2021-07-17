with devices_source as (

    select *
    from {{ source('interview_task', 'devices') }}

)

select * from devices_source