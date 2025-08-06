1. ###### olist_orders_dataset.csv

- 99442 rows, 8 columns

```
columns:
    order_id (string)
    customer_id (string)
    order_status (string)
    order_purchase_timestamp (timestamp)
    order_approved_at (timestamp)
    order_delivered_carrier_date (timestamp)
    order_delivered_customer_date (timestamp)
    order_estimated_delivery_date (timestamp)
```

- order_id (primary key)
- customer_id (foreign key)
- order_delivered_customer_date, order_delivered_carrier_date columns have missing values

###### 2. olist_customers_dataset.csv

- 99442 rows, 5 columns

```
columns:
    customer_id (string)
    customer_unique_id (string)
    customer_zip_code_prefix (int)
    customer_city (string)
    customer_state (string)
```

- customer_id (primary key)
- No issues

###### 3. olist_order_items_dataset.csv

- 112651 rows, 7 columns

```
columns:
    order_id (string)
    order_item_id (int)
    product_id (string)
    seller_id (string)
    shipping_limit_date (timestamp)
    price (float)
    freight_value (float)
```

- order_id (primary key)
- product_id, seller_id (foreign key)
- No issues

###### 4. olist_order_payments_dataset.csv

- 103887 rows, 5 columns

```
columns:
    order_id (string)
    payment_sequential (int)
    payment_type (string)
    payment_installments (int)
    payment_value (float)
```

- order_id (primary key)
- No issues

###### 5. olist_order_reviews_dataset.csv

- 99225 rows, 7 columns

```
columns:
    review_id (string)
    order_id (string)
    review_score (int)
    review_comment_title (string)
    review_comment_message (string)
    review_creation_date (timestamp)
    review_answer_timestamp (timestamp)
```

- review_id (primary key)
- order_id (foreign key)
- Some review comments are blank and not in English language

###### 6. olist_products_dataset.csv

- 32952 rows, 9 columns

```
columns:
    product_id (string)
    product_category_name (string)
    product_name_length (int)
    product_description_length (int)
    product_photos_qty (int)
    product_weight_g (int)
    product_length_cm (int)
    product_height_cm (int)
    product_width_cm (int)
```

- product_id (primary key)
- product_category_name has missing values

###### 7. product_category_name_translation.csv

- 72 rows, 2 columns

```
columns:
product_category_name (string),
product_category_name_english (string)
```

- product_category_name (primary key)
- product_category_name not in english

###### 8. olist_sellers_dataset.csv

- 3096 rows, 4 columns

```
columns:
    seller_id (string),
    seller_zip_code_prefix (int),
    seller_city  (string),
    seller_state  (string)
```

- seller_id (primary key)
- No issues

###### 9. olist_geolocation_dataset.csv

- 1000164 rows, 5 columns

```
columns:
    geolocation_zip_code_prefix (int)
    geolocation_lat (float)
    geolocation_lng (float)
    geolocation_city (string)
    geolocation_state (string)
```

- geolocation_zip_code_prefix (primary key)
- No issues
