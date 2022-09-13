{{ config(materialized='view') }}

with calendar_dates as (
  select * from {{ ref('dates__daily') }}
)

, recharge_subscriptions as (
  select * from {{ ref('stg_acme1__recharge_subscriptions') }}
  where subscription_status = 'ACTIVE'
)

, min_max_sub_date as (
  select
    *
    , min(created_at_date) over (partition by customer_id) as min_sub_date
    , max(created_at_date) over (partition by customer_id) as max_sub_date
  from recharge_subscriptions
)

, active_subscriptions as (
  select
    calendar_dates.date_day
    , case
        when calendar_dates.date_day >= created_at_date
          then 1
        else 0
    end as subscription_active_count
  from calendar_dates
    left join min_max_sub_date on 
      calendar_dates.date_day between 
        min_max_sub_date.min_sub_date and cast(current_date() as date)
)

, final as (
  select
    date_day
    , sum(subscription_active_count) as daily_active_subscriptions
  from active_subscriptions
  group by 1
)

select * from final