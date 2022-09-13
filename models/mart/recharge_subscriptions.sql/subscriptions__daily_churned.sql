{{ config(materialized='view') }}

with calendar_dates as (
  select * from {{ ref('dates__daily') }}
)

, recharge_subscriptions as (
  select * from {{ ref('stg_acme1__recharge_subscriptions') }}
  where subscription_status = 'CANCELLED'
)

, min_max_sub_date as (
  select
    *
    , min(created_at_date) over (partition by customer_id) as min_sub_date
    , max(created_at_date) over (partition by customer_id) as max_sub_date
  from recharge_subscriptions
)

, churned_subscriptions as (
  select
    calendar_dates.date_day
    , case
        when calendar_dates.date_day >= created_at_date
          then 1
        else 0
    end as subscription_churned_count
  from calendar_dates
    left join min_max_sub_date on 
      calendar_dates.date_day between 
        min_max_sub_date.min_sub_date and cast(current_date() as date)
)

, final as (
  select
    date_day
    , sum(subscription_churned_count) as daily_churned_subscriptions
  from churned_subscriptions
  group by 1
)

select * from final