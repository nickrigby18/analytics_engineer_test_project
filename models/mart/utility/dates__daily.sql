{{ config(materialized='ephemeral') }}

with date_spine as (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="cast('2020-10-01' as date)",
      end_date="cast('2022-04-08' as date)"
    )
  }}
)

select cast(date_day as date) as date_day from date_spine