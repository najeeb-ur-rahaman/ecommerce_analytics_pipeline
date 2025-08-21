WITH order_items_enriched AS (
    SELECT
        -- Order item details
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.seller_id,
        oi.price AS unit_price,
        oi.freight_value AS unit_freight,
        oi.quantity,
        oi.total_price,
        oi.total_freight,
        oi.estimated_actual_freight,
        oi.total_price + oi.estimated_actual_freight AS total_item_value,
        
        -- Product details
        prod.product_category_name,
        trans.product_category_name_english AS product_category_english,
        prod.product_weight_g,
        prod.product_length_cm,
        prod.product_height_cm,
        prod.product_width_cm,
        
        -- Calculated fields
        ROUND(oi.price * 0.85, 2) AS item_price_after_tax
        
    FROM {{ ref('stg_order_items') }} AS oi
    INNER JOIN {{ ref('stg_products') }} AS prod
        ON oi.product_id = prod.product_id
    LEFT JOIN {{ ref('stg_product_categories') }} AS trans
        ON prod.product_category_name = trans.product_category_name
)

SELECT
    -- Order details
    ord.order_id,
    ord.customer_id,
    ord.order_status,
    ord.order_purchase_ts,
    ord.order_approved_at,
    ord.order_delivered_carrier_date,
    ord.order_delivered_customer_date,
    
    -- Enriched item details (exclude order_id to avoid duplication)
    items.order_item_id,
    items.product_id,
    items.seller_id,
    items.unit_price,
    items.unit_freight,
    items.quantity,
    items.total_price,
    items.total_freight,
    items.estimated_actual_freight,
    items.total_item_value,
    items.product_category_name,
    items.product_category_english,
    items.product_weight_g,
    items.product_length_cm,
    items.product_height_cm,
    items.product_width_cm,
    items.item_price_after_tax,
    
    -- Additional order-level calculations
    SUM(items.total_item_value) OVER (PARTITION BY ord.order_id) AS order_total_value,
    COUNT(items.order_item_id) OVER (PARTITION BY ord.order_id) AS item_count
    
FROM {{ ref('stg_orders') }} AS ord
INNER JOIN order_items_enriched AS items
    ON ord.order_id = items.order_id