select *

from {{ source('brazilian_ecommerce', 'sellers') }}