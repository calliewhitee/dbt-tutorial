with addresses_source as (

    select *
    from {{ source('interview_task', 'addresses') }}

)

select * from addresses_source