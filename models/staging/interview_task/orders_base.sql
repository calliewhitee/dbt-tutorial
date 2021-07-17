with orders_source as (

    select *
    from {{ source('interview_task', 'orders') }}

)

select * from orders_source