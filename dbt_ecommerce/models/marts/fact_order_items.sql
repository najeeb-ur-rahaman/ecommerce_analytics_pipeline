{{
    config(
        materialized='table',
        unique_key='order_item_key',
        tags=['fact']
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['items.order_id', 'items.order_item_id']) }} AS order_item_key,
    {{ dbt_utils.generate_surrogate_key(['items.order_id']) }} AS order_key,
    {{ dbt_utils.generate_surrogate_key(['ord.customer_unique_id']) }} AS customer_key,
    {{ dbt_utils.generate_surrogate_key(['items.product_id']) }} AS product_key,
    items.order_item_id,
    items.unit_price,
    items.unit_freight,
    items.quantity,
    items.unit_price * items.quantity AS item_revenue,
    DATE(ord.order_purchase_ts) AS order_date,
    CURRENT_TIMESTAMP() AS dbt_created_at
FROM {{ ref('int_orders_with_items') }} items
JOIN {{ ref('int_orders_enriched') }} ord 
    ON items.order_id = ord.order_id