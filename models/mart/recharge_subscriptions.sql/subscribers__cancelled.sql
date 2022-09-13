{{ config(materialized='view') }}

with recharge_subscriptions as (
  select * from {{ ref('stg_acme1__recharge_subscriptions') }}
)

, calendar_dates as (
  select * from {{ ref('dates__daily') }}
)

, active_subscribers as (
  select
    distinct customer_id
  from recharge_subscriptions
  where subscription_status = 'ACTIVE'
)

, cancelled_subscriptions as (
  select
    cancelled_at_date
    , count(*) as cancelled_subscribers
  from recharge_subscriptions
    left join active_subscribers on
      recharge_subscriptions.customer_id = active_subscribers.customer_id
  where active_subscribers.customer_id is null
  group by 1
)

, final as (
  select
    calendar_dates.date_day
    , ifnull(cancelled_subscriptions.cancelled_subscribers, 0) as subscribers_cancelled
  from calendar_dates
    left join cancelled_subscriptions on
      calendar_dates.date_day = cancelled_subscriptions.cancelled_at_date
)

select * from final 