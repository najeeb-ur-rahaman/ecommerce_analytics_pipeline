SELECT
    -- Order columns
    ord.order_id,
    ord.order_status,
    ord.order_purchase_ts,
    ord.order_approved_at,
    ord.order_delivered_carrier_date,
    ord.order_delivered_customer_date,
    ord.order_estimated_delivery_date,
    
    -- Customer columns
    cust.customer_unique_id,
    cust.customer_zip_code_prefix,
    cust.customer_city,
    cust.customer_state,
    
    -- Calculated fields
    TIMESTAMPDIFF(day, ord.order_purchase_ts, ord.order_delivered_customer_date) AS delivery_days,
    TIMESTAMPDIFF(day, ord.order_delivered_customer_date, ord.order_estimated_delivery_date) AS delivery_days_vs_estimate

FROM {{ ref('stg_orders') }} AS ord
INNER JOIN {{ ref('stg_customers') }} AS cust
    ON ord.customer_id = cust.customer_id