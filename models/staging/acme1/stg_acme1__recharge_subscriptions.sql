{{ config(materialized='view') }}

with recharge_subscriptions as (
  select * from {{ source('raw_data_sandbox', 'acme1_recharge_subscriptions') }}
)

, renamed_recasted as (
  select
    id as order_id
    , customer_id
    , price as subscription_price
    , quantity as subscription_quantity
    , initcap(order_interval_unit) as subscription_interval_unit
    , status as subscription_status
    , created_at    
    , updated_at
    , cancelled_at
    , cast(created_at as date) as created_at_date
    , cast(updated_at as date) as updated_at_date
    , cast(cancelled_at as date) as cancelled_at_date
  from recharge_subscriptions
)

select * from renamed_recasted