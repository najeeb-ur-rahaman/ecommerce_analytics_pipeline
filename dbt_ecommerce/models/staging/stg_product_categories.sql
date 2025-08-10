select *

from {{ source('brazilian_ecommerce', 'product_categories') }}