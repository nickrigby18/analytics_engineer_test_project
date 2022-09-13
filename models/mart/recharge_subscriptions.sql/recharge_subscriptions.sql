{{ config(materialized='table') }}

with calendar_dates as (
  select * from {{ ref('dates__daily') }}
)

, recharge_subscriptions as (
  select * from {{ ref('stg_acme1__recharge_subscriptions') }}
)

, subscribers_active as (
  select * from {{ ref('subscribers__active') }}
)

, subscribers_cancelled as (
  select * from {{ ref('subscribers__cancelled') }}
)

, subscribers_new as (
  select * from {{ ref('subscribers__new') }}
)

, subscribers_churned as (
  select * from {{ ref('subscribers__churned') }}
)

, subscriptions_returning as (
  select * from {{ ref('subscriptions__returning') }}
)

, subscriptions_active as (
  select * from {{ ref('subscriptions__daily_active') }}
)

, subscriptions_churned as (
  select * from {{ ref('subscriptions__daily_churned') }}
)

-- Count daily new subscriptions
, subscriptions_new as (
  select
    created_at_date
    , count(*) as subscriptions_new
  from recharge_subscriptions 
  group by 1
)

-- Count daily cancelled subscriptions
, subscriptions_cancelled as (
  select
    cancelled_at_date
    , count(*) as subscriptions_cancelled
  from recharge_subscriptions
  group by 1
)

, final as (
  select
    calendar_dates.date_day as date
    , ifnull(subscriptions_new.subscriptions_new, 0) as subscriptions_new
    , subscriptions_returning.subscriptions_returning
    , ifnull(subscriptions_cancelled.subscriptions_cancelled, 0) as subscriptions_cancelled
    , ifnull(subscriptions_active.daily_active_subscriptions, 0) as subscriptions_active
    , ifnull(subscriptions_churned.daily_churned_subscriptions, 0) as subscriptions_churned
    , subscribers_new.subscribers_new
    , subscribers_cancelled.subscribers_cancelled
    , ifnull(subscribers_active.subscribers_active, 0) as subscribers_active
    , ifnull(subscribers_churned.subscribers_churned, 0) as subscribers_churned
  from calendar_dates
    left join subscriptions_new on
      calendar_dates.date_day = subscriptions_new.created_at_date
    left join subscriptions_returning on 
      calendar_dates.date_day = subscriptions_returning.date_day
    left join subscriptions_cancelled on 
      calendar_dates.date_day = subscriptions_cancelled.cancelled_at_date
    left join subscriptions_active on 
      calendar_dates.date_day = subscriptions_active.date_day
    left join subscriptions_churned on 
      calendar_dates.date_day = subscriptions_churned.date_day
    left join subscribers_new on 
      calendar_dates.date_day = subscribers_new.date_day
    left join subscribers_cancelled on 
      calendar_dates.date_day = subscribers_cancelled.date_day
    left join subscribers_active on 
      calendar_dates.date_day = subscribers_active.date_day
    left join subscribers_churned on 
      calendar_dates.date_day = subscribers_churned.date_day
)

select * from final