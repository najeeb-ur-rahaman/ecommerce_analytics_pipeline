WITH order_items_agg AS (
    SELECT
        order_id,
        SUM(quantity) AS total_items,
        SUM(total_item_value) AS total_order_value,
        COUNT(DISTINCT product_category_english) AS distinct_categories,
        MODE(product_category_english) AS most_common_category
    FROM {{ ref('int_orders_with_items') }}
    GROUP BY order_id
)

SELECT
    -- Order and customer details
    owc.order_id,
    owc.customer_id,
    owc.order_status,
    owc.order_purchase_ts,
    owc.order_approved_at,
    owc.order_delivered_carrier_date,
    owc.order_delivered_customer_date,
    owc.order_estimated_delivery_date,
    owc.customer_unique_id,
    owc.customer_zip_code_prefix,
    owc.customer_city,
    owc.customer_state,
    owc.delivery_days,
    owc.delivery_days_vs_estimate AS delivery_delay_days,
    
    -- Aggregated item details
    oa.total_items,
    oa.total_order_value,
    oa.distinct_categories,
    oa.most_common_category,
    
    -- Payment details
    owp.total_payment_amount,
    owp.payment_count,
    owp.max_payment_sequential,
    owp.credit_card_amount,
    owp.boleto_amount,
    owp.voucher_amount,
    owp.debit_card_amount,
    owp.has_credit_card_payment,
    owp.has_voucher_payment,
    owp.total_installments,
    owp.approval_days,
    owp.payment_accuracy,
    
    -- Review details
    owr.review_id,
    owr.review_score,
    owr.avg_review_score,
    owr.review_count,
    owr.review_comment_title,
    owr.review_comment_message,
    owr.review_creation_date,
    owr.review_answer_ts,
    owr.delivery_to_review_days,

FROM {{ ref('int_orders_with_customers') }} owc
LEFT JOIN order_items_agg oa
    ON owc.order_id = oa.order_id
LEFT JOIN {{ ref('int_orders_with_payments') }} owp
    ON owc.order_id = owp.order_id
LEFT JOIN {{ ref('int_orders_with_reviews') }} owr
    ON owc.order_id = owr.order_id