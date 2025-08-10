WITH item_groups AS (
    SELECT
        order_id,
        product_id,
        seller_id,
        price,
        freight_value,
        shipping_limit_date,
        COUNT(*) AS quantity,
        MIN(order_item_id) AS original_item_id
    FROM {{ source('brazilian_ecommerce', 'order_items') }}
    GROUP BY 1,2,3,4,5,6
)
SELECT
    order_id,
    original_item_id AS order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    shipping_limit_date,
    quantity,
    -- Price scales with quantity
    price * quantity AS total_price,

    -- Freight DOESN'T scale with quantity
    freight_value AS total_freight,

    -- Realistic shipping cost model (example)
    freight_value + (freight_value * 0.3 * (quantity - 1)) AS estimated_actual_freight
FROM item_groups

{# 
Realistic Shipping Cost Formula

freight_value + (freight_value * incremental_rate * (quantity - 1))
-- This formula calculates the total freight cost based on a base freight value and an incremental rate for additional items.
-- The base freight value is the cost for the first item, and the incremental rate applies to

Where:

incremental_rate = 0.3 (meaning 30% of base freight per additional item)

Example for quantity=3:

text
Base freight: 17.63
Additional items: 2
Incremental cost: 17.63 × 0.3 × 2 = 10.58
Total freight: 17.63 + 10.58 = 28.21 
#}