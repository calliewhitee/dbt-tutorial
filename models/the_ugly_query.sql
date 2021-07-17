{{ config(materialized='table') }}

select *,
    amount_total_cents / 100 as amount_total,
    gross_total_amount_cents/ 100 as gross_total_amount,
    total_amount_cents/ 100 as total_amount,
    gross_tax_amount_cents/ 100 as gross_tax_amount,
    gross_amount_cents/ 100 as gross_amount,
    gross_shipping_amount_cents/ 100 as gross_shipping_amount
from (
    --cte #1
        select
        o.order_id,
        o.user_id,
        o.created_at,
        o.updated_at,
        o.shipped_at,
        o.currency,
        o.status AS order_status,
        case
            when o.status in ('paid', 'completed', 'shipped')
                then 'completed'
            else o.status
        end as order_status_category,
        case
            when oa.country_code is null 
                then 'Null country'
            when oa.country_code = 'US' 
                then 'US'
            when oa.country_code != 'US' 
                then 'International'
        end as country_type,
        o.shipping_method,
        case
            when d.device = 'web' 
                then 'desktop'
            when d.device in ('ios-app', 'android-app') 
                then 'mobile-app'
            when d.device in ('mobile', 'tablet') 
                then 'mobile-web'
            when nullif(d.device, '') is null
                then 'unknown'
            else 'ERROR'
        end as purchase_device_type,
        d.device AS purchase_device,
        case
            when fo.first_order_id = o.order_id 
                then 'new'
            else 'repeat'
        end as user_type,
        o.amount_total_cents,
        pa.gross_total_amount_cents,
        iff(o.currency = 'USD', o.amount_total_cents,
            pa.gross_total_amount_cents) AS total_amount_cents,
        pa.gross_tax_amount_cents,
        pa.gross_amount_cents,
        pa.gross_shipping_amount_cents
        from `dbt-public.interview_task.orders` o


        left join(
        select
        distinct cast(d.type_id as int64) as order_id,
        first_value(d.device) over (partition by d.type_id
            order by d.created_at rows between unbounded preceding 
                and unbounded following) as device
        from `dbt-public.interview_task.devices` d
        where d.type = 'order'
        ) cte_2 ON d.order_id = o.order_id



        left join (
        select
            fo.user_id,
            min(fo.order_id) as first_order_id
        from `dbt-public.interview_task.orders` as fo
        where
            fo.status != 'cancelled'
        group by
            fo.user_id
        ) fo ON o.user_id = fo.user_id


        left join `dbt-public.interview_task.addresses` oa
        on oa.order_id = o.order_id
        left join (
        select
        order_id,
        sum(iff(status = 'completed', tax_amount_cents, 0) as gross_tax_amount_cents,
        sum(iff(status = 'completed', amount_cents, 0) as gross_amount_cents,
        sum(iff(status = 'completed', amount_shipping_cents, 0) as gross_shipping_amount_cents,
        sum(iff(status = 'completed', 
            tax_amount_cents + amount_cents + amount_shipping_cents,
            0) as gross_total_amount_cents
        from `dbt-public.interview_task.payments`
        group by order_id
        ) pa ON pa.order_id = o.order_id
        )