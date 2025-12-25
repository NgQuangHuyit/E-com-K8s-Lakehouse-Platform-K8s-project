{{ config(
    materialized='table',
    schema='sale_mart',
    file_format='delta'
) }}

WITH date_spine AS (
    SELECT explode(sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day)) AS date_val
)

SELECT
  year(date_val) * 10000 + month(date_val) * 100 + day(date_val) AS date_key,
  
  date_val AS calendar_date,
  year(date_val) AS year,
  month(date_val) AS month,
  concat(month(date_val), '-', year(date_val)) AS month_year,
  quarter(date_val) AS quarter,
  concat('Q', quarter(date_val)) AS quarter_name,
  concat('Q', quarter(date_val), '-', year(date_val)) AS quarter_year,
  date_format(date_val, 'MMMM') AS month_name,
  date_format(date_val, 'EEEE') AS day_name,
  
  CASE WHEN dayofweek(date_val) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
  CASE WHEN dayofweek(date_val) NOT IN (1, 7) THEN 1 ELSE 0 END AS is_weekday
FROM date_spine
ORDER BY date_val




