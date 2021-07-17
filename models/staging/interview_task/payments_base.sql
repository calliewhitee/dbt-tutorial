with payments_source as (

    select *
    from {{ source('interview_task', 'payments') }}

)

select * from payments_source