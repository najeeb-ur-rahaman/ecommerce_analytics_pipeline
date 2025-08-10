select *

from {{ source('brazilian_ecommerce', 'order_payments') }}