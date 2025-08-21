WITH order_payments_aggregated AS (
    SELECT
        op.order_id,
        
        -- Payment aggregation
        SUM(op.payment_value) AS total_payment_amount,
        COUNT(op.payment_sequential) AS payment_count,
        MAX(op.payment_sequential) AS max_payment_sequential,
        
        -- Payment method breakdown
        SUM(CASE WHEN op.payment_type = 'credit_card' THEN op.payment_value ELSE 0 END) AS credit_card_amount,
        SUM(CASE WHEN op.payment_type = 'boleto' THEN op.payment_value ELSE 0 END) AS boleto_amount,
        SUM(CASE WHEN op.payment_type = 'voucher' THEN op.payment_value ELSE 0 END) AS voucher_amount,
        SUM(CASE WHEN op.payment_type = 'debit_card' THEN op.payment_value ELSE 0 END) AS debit_card_amount,
        
        -- Payment method flags
        MAX(CASE WHEN op.payment_type = 'credit_card' THEN 1 ELSE 0 END) AS has_credit_card_payment,
        MAX(CASE WHEN op.payment_type = 'voucher' THEN 1 ELSE 0 END) AS has_voucher_payment,
        
        -- Installment analysis
        SUM(op.payment_installments) AS total_installments
    FROM {{ ref('stg_order_payments') }} AS op
    GROUP BY op.order_id
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
    
    -- Payment details
    pay.total_payment_amount,
    pay.payment_count,
    pay.max_payment_sequential,
    
    -- Payment method amounts
    pay.credit_card_amount,
    pay.boleto_amount,
    pay.voucher_amount,
    pay.debit_card_amount,
    
    -- Payment method flags
    pay.has_credit_card_payment,
    pay.has_voucher_payment,
    
    -- Installment details
    pay.total_installments,
    
    -- Payment timing analysis
    TIMESTAMPDIFF(day, ord.order_purchase_ts, ord.order_approved_at) AS approval_days,
    CASE 
        WHEN pay.total_payment_amount < ord_total.order_total 
            THEN 'underpaid'
        WHEN pay.total_payment_amount > ord_total.order_total 
            THEN 'overpaid'
        ELSE 'exact'
    END AS payment_accuracy
    
FROM {{ ref('stg_orders') }} AS ord
LEFT JOIN order_payments_aggregated AS pay
    ON ord.order_id = pay.order_id
LEFT JOIN (
    SELECT 
        order_id,
        SUM(price + freight_value) AS order_total
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
) AS ord_total
    ON ord.order_id = ord_total.order_id