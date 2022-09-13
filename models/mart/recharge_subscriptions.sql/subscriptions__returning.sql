{{ config(materialized='view') }}

with recharge_subscriptions as (
  select * from {{ ref('stg_acme1__recharge_subscriptions') }}
)

, calendar_dates as (
  select * from {{ ref('dates__daily') }}
)

, cancelled_subscriptions as (
  select
    distinct customer_id
  from recharge_subscriptions
  where subscription_status = 'CANCELLED'
)

, returning_customers as (
  select
    recharge_subscriptions.created_at_date
    , count(*) as subscriptions_returning
  from recharge_subscriptions
    join cancelled_subscriptions on
      recharge_subscriptions.customer_id = cancelled_subscriptions.customer_id
  where recharge_subscriptions.subscription_status != 'CANCELLED'
  group by 1
)

, final as (
  select
    calendar_dates.date_day
    , ifnull(returning_customers.subscriptions_returning, 0) as subscriptions_returning
  from calendar_dates
    left join returning_customers on
      calendar_dates.date_day = returning_customers.created_at_date
)

select * from final