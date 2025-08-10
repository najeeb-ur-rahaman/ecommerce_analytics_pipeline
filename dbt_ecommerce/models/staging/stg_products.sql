select *

from {{ source('brazilian_ecommerce', 'products') }}