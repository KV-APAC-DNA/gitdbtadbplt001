{{
    config(
        post_hook= "{{column_encryption(customerid)}}"
    )
}}

with source as(
    select * from {{ source('jpndclsdl_raw', 'sfmc_kokya_subskey_mapping') }}
),
updated_customers AS (
    SELECT 
       *
       -- {{ column_encryption(CustomerId) }} AS encrypted_customer_id
    FROM 
        source
)--,
-- final as(
--     select * from source
-- )
select * from updated_customers