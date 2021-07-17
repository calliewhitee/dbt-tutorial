My general method to refactor the provided query was as follows:

- Identify sources (addresses, devices, orders, payments)
- Create base tables, source .yml, markdown, and schema .yml files for all
four sources. Lots of copy and paste here.
- Quickly throw in some relevant information and set-up details into 
the .yml and markdown files
- Section the subqueries from the original query into their own tables and
clean the code for readability and slight efficiency 
- Move any remaining logic that is dependent on only one source table into the 
appropriate staging/mart models
- Reconstruct the joins and final table in the fact_order_details table
- Add folder level model configs to the dbt_project.yml for materializations
and consistent tags. The tags aren't super valuable right now, but as this 
jaffle shop quickly scales and adds further sources it will be helpful
to delineate products and potentially separate out dbt Cloud runs easily.
- Run `dbt run --exclude the_ugly_query` with minor debugging
- Run `dbt test`with minor debugging. NOTE: All tests are based on assumed
logic and relationships. Typically I would verify the appropriateness of the
tests with stakeholders or source table owners.
- QA against the original query in BigQuery to ensure it's the same result.
See code for that QA below.

What I would do to improve the project with more time (in order of priority):
- Confirm that the original query returns the expected and desired results
with data users.
- Add in the dbt_utils package at a minimum for more availability of
schema tests.
- Add schema .yml files for the marts models. This would include column 
descriptions references and tests. The column description references would
pull the actual definitions from a common 'glossary.yml' file. This means that
'order_id' can have the exact same definition across all tables without the
definition being repeated and therefore potentially drifting over time.
- Review the project for consistent column naming and improve naming for
business centric usage.
- Work with stakeholders or data source users to understand potential risks
in the data that need to be accounted for in tests. Add those tests as
schema tests or bespoke tests.
- I didn't identify an immediately obvious need for macros, but a quick scan
for potential uses would be valuable here.
- Run, test, and QA again.
- At this point I would be confident in the completeness of the project
for its current usage. Depending on the needs of the client as they use
this project in the future there are endless additional things that could
be implemented. Snapshots, development tools such as schema template creators,
additional folder differentiation for other business domains, more packages, etc.


GENERIC QA QUERY
--currently set to compare number of rows but can be easily changed for
sums of columns, even grouped by dimensions, or other metrics to ensure
that the data is the same. Because the new structure can be really
beautiful, but it's not valuable unless it's correct.
--this is of course assuming that the original query was determined to
be accurate.
--I checked the sum of gross_total_amount, total_gross_amount (which, I
would also improve the names of these columns), total_tax_amount, and
total_shipping_amount. They were all sufficiently identical; there was a very
slight discrepancy in the 1.0E-13ths which is likely due to decimal place
differences between the two methods. I would acknowledge this difference, but
it's likely not a good use of my time to investigate unless otherwise indicated.
I also checked the distinct number of orders and distinct users. These were
also sufficiently identical.

```
with ugly_query as (

    SELECT
*,
amount_total_cents / 100 as amount_total,
gross_total_amount_cents/ 100 as gross_total_amount,
total_amount_cents/ 100 as total_amount,
gross_tax_amount_cents/ 100 as gross_tax_amount,
gross_amount_cents/ 100 as gross_amount,
gross_shipping_amount_cents/ 100 as gross_shipping_amount
FROM (
SELECT
o.order_id,
o.user_id,
o.created_at,
o.updated_at,
o.shipped_at,
o.currency,
o.status AS order_status,
CASE
WHEN o.status IN (
'paid',
'completed',
'shipped'
) THEN 'completed'
ELSE o.status
END AS order_status_category,
CASE
WHEN oa.country_code IS NULL THEN 'Null country'
WHEN oa.country_code = 'US' THEN 'US'
WHEN oa.country_code != 'US' THEN 'International'
END AS country_type,
o.shipping_method,
CASE
WHEN d.device = 'web' THEN 'desktop'
WHEN d.device IN ('ios-app', 'android-app') THEN 'mobile-app'
when d.device IN ('mobile', 'tablet') THEN 'mobile-web'
when NULLIF(d.device, '') IS NULL THEN 'unknown'
ELSE 'ERROR'
END AS purchase_device_type,
d.device AS purchase_device,
CASE
WHEN fo.first_order_id = o.order_id THEN 'new'
ELSE 'repeat'
END AS user_type,
o.amount_total_cents,
pa.gross_total_amount_cents,
CASE
WHEN o.currency = 'USD' then o.amount_total_cents
ELSE pa.gross_total_amount_cents
END AS total_amount_cents,
pa.gross_tax_amount_cents,
pa.gross_amount_cents,
pa.gross_shipping_amount_cents
FROM `dbt-public.interview_task.orders` o
LEFT JOIN (
SELECT
DISTINCT cast(d.type_id as int64) as order_id,
FIRST_VALUE(d.device) OVER (
PARTITION BY d.type_id
ORDER BY
d.created_at ROWS BETWEEN UNBOUNDED PRECEDING
AND UNBOUNDED FOLLOWING
) AS device
FROM `dbt-public.interview_task.devices` d
WHERE d.type = 'order'
) d ON d.order_id = o.order_id
LEFT JOIN (
SELECT
fo.user_id,
MIN(fo.order_id) as first_order_id
FROM `dbt-public.interview_task.orders` as fo
WHERE
fo.status != 'cancelled'
GROUP BY
fo.user_id
) fo ON o.user_id = fo.user_id
left join `dbt-public.interview_task.addresses` oa
ON oa.order_id = o.order_id
LEFT JOIN (
select
order_id,
sum(
CASE
WHEN status = 'completed' THEN tax_amount_cents
ELSE 0
END
) as gross_tax_amount_cents,
sum(
CASE
WHEN status = 'completed' THEN amount_cents
ELSE 0
END
) as gross_amount_cents,
sum(
CASE
WHEN status = 'completed' THEN amount_shipping_cents
ELSE 0
END
) as gross_shipping_amount_cents,
sum(
CASE
WHEN status = 'completed' THEN tax_amount_cents + amount_cents + amount_shipping_cents
ELSE 0
END
) as gross_total_amount_cents
FROM `dbt-public.interview_task.payments`
GROUP BY order_id
) pa ON pa.order_id = o.order_id
)

), old_table as (

select count(*) as old_metric
from ugly_query 

), new_table as (
select count(*) as new_metric
from `fishtowninterview-320102.dbt_callie.fact_order_details`
), compared_tables as (

    select *,
        old_metric - new_metric as diff
    from old_table  
        cross join new_table 
)

select *
from compared_tables 
```