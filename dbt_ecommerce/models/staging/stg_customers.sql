select *

from {{ source('brazilian_ecommerce', 'customers') }}