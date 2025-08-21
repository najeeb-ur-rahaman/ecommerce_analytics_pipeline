{{
    config(
        materialized='table',
        unique_key='customer_key',
        tags=['dimension']
    )
}}

WITH unique_customers AS (
    SELECT
        customer_unique_id,
        customer_city,
        customer_state,
        customer_zip_code_prefix,
        ROW_NUMBER() OVER (PARTITION BY customer_unique_id ORDER BY customer_city, customer_state, customer_zip_code_prefix) AS rn
    FROM {{ ref('stg_customers') }}
),
customer_orders AS (
    SELECT
        customer_unique_id,
        MIN(order_purchase_ts) AS first_order_date,
        MAX(order_purchase_ts) AS last_order_date,
        COUNT(DISTINCT order_id) AS total_orders
    FROM {{ ref('int_orders_enriched') }}
    GROUP BY customer_unique_id
),
joined AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['c.customer_unique_id']) }} AS customer_key,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        c.customer_zip_code_prefix,
        co.first_order_date,
        co.last_order_date,
        co.total_orders,
        CURRENT_TIMESTAMP() AS dbt_created_or_updated_at,
        c.rn
    FROM unique_customers c
    INNER JOIN customer_orders co
        ON c.customer_unique_id = co.customer_unique_id
)
SELECT 
customer_key,
customer_unique_id,
customer_city,
customer_state,
customer_zip_code_prefix,
first_order_date,
last_order_date,
total_orders,
dbt_created_or_updated_at
FROM joined
WHERE rn = 1