{{
    config(
        materialized='table',
        unique_key='product_key',
        tags=['dimension']
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['p.product_id']) }} AS product_key,
    p.product_id,
    COALESCE(t.product_category_name_english, 'Unknown') AS product_category,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    CURRENT_TIMESTAMP() AS dbt_created_or_updated_at
FROM {{ ref('stg_products') }} p
LEFT JOIN {{ ref('stg_product_categories') }} t
    ON p.product_category_name = t.product_category_name