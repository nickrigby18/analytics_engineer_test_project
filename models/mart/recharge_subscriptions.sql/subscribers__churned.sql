{{ config(materialized='view') }}

with recharge_subscriptions as (
  select * from {{ ref('stg_acme1__recharge_subscriptions') }}
)

, calendar_dates as (
  select * from {{ ref('dates__daily') }}
)

-- Get accounts with 0 ACTIVE subscriptions
, non_active_subscribers as (
  select
    customer_id
    , sum(if(subscription_status = 'ACTIVE', 1, 0)) as active_subscriptions
  from recharge_subscriptions
  group by 1
  having active_subscriptions = 0
)

-- Pull each subscriber's LAST cancellation date
, daily_churned_customers as (
  select
    recharge_subscriptions.customer_id
    , date_add(ifnull(recharge_subscriptions.cancelled_at_date, recharge_subscriptions.updated_at_date), interval 1 day) as churned_at_date
  from recharge_subscriptions
    join non_active_subscribers on 
      recharge_subscriptions.customer_id = non_active_subscribers.customer_id
  qualify row_number() over (
    partition by recharge_subscriptions.customer_id
    order by recharge_subscriptions.cancelled_at desc
  ) = 1
)

, final as (
  select
    calendar_dates.date_day
    , count(*) as subscribers_churned
  from calendar_dates
    left join daily_churned_customers on
      calendar_dates.date_day between 
        daily_churned_customers.churned_at_date and cast(current_date() as date)
  group by 1
)

select * from final