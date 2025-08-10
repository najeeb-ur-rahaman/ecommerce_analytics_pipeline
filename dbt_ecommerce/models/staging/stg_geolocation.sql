select *

from {{ source('brazilian_ecommerce', 'geolocation') }}