WITH ranked_reviews AS (
    SELECT 
        order_id,
        review_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_ts,
        COUNT(review_id) OVER (PARTITION BY order_id) AS review_count,
        AVG(review_score) OVER (PARTITION BY order_id) AS avg_review_score,
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY review_creation_date DESC
        ) AS review_rank
    FROM {{ ('stg_order_reviews') }}
)

SELECT 
    ord.order_id,
    ord.customer_id,
    ord.order_status,
    ord.order_purchase_ts,
    ord.order_approved_at,
    ord.order_delivered_carrier_date,
    ord.order_delivered_customer_date,
    ord.order_estimated_delivery_date,
    
    -- Review fields with NULL handling
    COALESCE(rr.review_id, 'no_review') AS review_id,
    COALESCE(rr.review_score, -1) AS review_score,
    COALESCE(rr.avg_review_score, -1) AS avg_review_score,
    COALESCE(rr.review_count, 0) AS review_count,
    COALESCE(rr.review_comment_title, '') AS review_comment_title,
    COALESCE(rr.review_comment_message, '') AS review_comment_message,
    rr.review_creation_date,
    rr.review_answer_ts,
    
    -- Time calculation with NULL protection
    CASE 
        WHEN ord.order_delivered_customer_date IS NOT NULL 
             AND rr.review_creation_date IS NOT NULL
        THEN TIMESTAMPDIFF(day, ord.order_delivered_customer_date, rr.review_creation_date)
        ELSE NULL 
    END AS delivery_to_review_days
    
FROM {{ ('stg_orders') }} AS ord
LEFT JOIN {{ ('ranked_reviews') }} AS rr
    ON ord.order_id = rr.order_id
    AND rr.review_rank = 1  -- Critical: Get only latest review