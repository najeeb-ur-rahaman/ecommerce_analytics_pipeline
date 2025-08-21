{{
    config(
        materialized='table',
        unique_key='order_key',
        tags=['fact']
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS order_key,
    {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }} AS customer_key,
    DATE(order_purchase_ts) AS order_date,
    order_status,
    total_order_value AS revenue,
    total_items AS quantity_sold,
    total_payment_amount,
    payment_accuracy,
    delivery_delay_days,
    review_score,
    CASE 
        WHEN delivery_delay_days > 0 THEN 'Delayed'
        WHEN delivery_delay_days < 0 THEN 'Early'
        ELSE 'OnTime'
    END AS delivery_status,
    CURRENT_TIMESTAMP() AS dbt_created_at,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('int_orders_enriched') }}