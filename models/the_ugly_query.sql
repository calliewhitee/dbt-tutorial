{{ config(materialized='table') }}

with payments_source as (

    select *
    from {{ ref('payments_base') }}

), addresses_source as (

    select *
    from {{ ref('addresses_base') }}

), orders_source as (

    select *
    from {{ ref('orders_base') }}

), orders_source as (

    select *
    from {{ ref('devices_base') }}

)
