WITH ranked_reviews AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY review_id 
            ORDER BY review_answer_timestamp ASC
        ) AS row_rank
    FROM {{ source('brazilian_ecommerce', 'order_reviews') }}
)

SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    cast(review_creation_date as date) as review_creation_date,
    review_answer_timestamp as review_answer_ts
FROM ranked_reviews
WHERE row_rank = 1