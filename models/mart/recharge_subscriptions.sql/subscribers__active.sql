{{ config(materialized='view') }}

with recharge_subscriptions as (
  select * from {{ ref('stg_acme1__recharge_subscriptions') }}
  where subscription_status = 'ACTIVE'
)

, calendar_dates as (
  select * from {{ ref('dates__daily') }}
)

, subscriber_active_dates as (
  select
    customer_id
    , min(created_at_date) as min_created_at_date
    , max(created_at_date) as max_created_at_date
  from recharge_subscriptions
  group by 1
)

, active_subscribers as (
  select
    calendar_dates.date_day
    , customer_id
  from calendar_dates
    left join subscriber_active_dates on
      calendar_dates.date_day between 
        subscriber_active_dates.min_created_at_date and cast(current_date() as date)
)

, final as (
  select
    date_day
    , count(*) as subscribers_active
  from active_subscribers
  group by 1
)

select * from final