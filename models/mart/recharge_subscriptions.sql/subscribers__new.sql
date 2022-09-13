{{ config(materialized='view') }}

with recharge_subscriptions as (
  select * from {{ ref('stg_acme1__recharge_subscriptions') }}
)

, calendar_dates as (
  select * from {{ ref('dates__daily') }}
)

, first_subscription as (
  select
    customer_id
    , created_at_date
    , row_number() over (
        partition by customer_id
        order by created_at asc
      ) as subscription_order
  from recharge_subscriptions
  qualify subscription_order = 1
)

, daily_new_subscriber_count as (
  select
    created_at_date
    , count(*) as new_subscriber_count
  from first_subscription
  group by 1
)

, final as (
  select
    calendar_dates.date_day
    , ifnull(daily_new_subscriber_count.new_subscriber_count, 0) as subscribers_new
  from calendar_dates
    left join daily_new_subscriber_count on 
      calendar_dates.date_day = daily_new_subscriber_count.created_at_date
)

select * from final