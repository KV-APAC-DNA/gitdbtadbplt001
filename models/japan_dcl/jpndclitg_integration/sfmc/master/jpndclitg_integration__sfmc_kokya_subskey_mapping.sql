with source as(
    select * from {{ source('jpndclsdl_raw', 'sfmc_kokya_subskey_mapping') }}
),
updated_customers AS (
    SELECT AccountId,
            CASE WHEN LENGTH(customerid)::string = 10 THEN 
                    SUBSTR(customerid, 7, 1) || SUBSTR(customerid, 5, 1) || SUBSTR(customerid, 8, 1) || SUBSTR(customerid, 6, 1) || SUBSTR(customerid, 2, 1) || SUBSTR(customerid, 4, 1) || SUBSTR(customerid, 1, 1) || SUBSTR(customerid, 3, 1) || SUBSTR(customerid, 9, 1) || SUBSTR(customerid, 10, 1)
                WHEN LENGTH(customerid) = 9 THEN
                SUBSTR(customerid, 6, 1) || SUBSTR(customerid, 4, 1) || SUBSTR(customerid, 7, 1) || SUBSTR(customerid, 5, 1) || SUBSTR(customerid, 1, 1) || SUBSTR(customerid, 3, 1) || '0' || SUBSTR(customerid, 2, 1) || SUBSTR(customerid, 8, 1) || SUBSTR(customerid, 9, 1)
                WHEN LENGTH(customerid) = 8 THEN
                    SUBSTR(customerid, 5, 1) || SUBSTR(customerid, 3, 1) || SUBSTR(customerid, 6, 1) || SUBSTR(customerid, 4, 1) || '0' || SUBSTR(customerid, 2, 1) || '0' || SUBSTR(customerid, 1, 1) || SUBSTR(customerid, 7, 1) || SUBSTR(customerid, 8, 1)
                WHEN LENGTH(customerid) = 7 THEN
                    SUBSTR(customerid, 4, 1) || SUBSTR(customerid, 2, 1) || SUBSTR(customerid, 5, 1) || SUBSTR(customerid, 3, 1) || '0' || SUBSTR(customerid, 1, 1) || '0' || '0' || SUBSTR(customerid, 6, 1) || SUBSTR(customerid, 7, 1)
                WHEN LENGTH(customerid) = 6 THEN
                    SUBSTR(customerid, 3, 1) || SUBSTR(customerid, 1, 1) || SUBSTR(customerid, 4, 1) || SUBSTR(customerid, 2, 1) || '0' || '0' || '0' || '0' || SUBSTR(customerid, 5, 1) || SUBSTR(customerid, 6, 1)
                WHEN LENGTH(customerid) = 5 THEN
                    SUBSTR(customerid, 2, 1) || '0' || SUBSTR(customerid, 3, 1) || SUBSTR(customerid, 1, 1) || '0' || '0' || '0' || '0' || SUBSTR(customerid, 4, 1) || SUBSTR(customerid, 5, 1)
                WHEN LENGTH(customerid) = 4 THEN
                    SUBSTR(customerid, 1, 1) || '0' || SUBSTR(customerid, 2, 1) || '0' || '0' || '0' || '0' || '0' || SUBSTR(customerid, 3, 1) || SUBSTR(customerid, 4, 1)
                WHEN LENGTH(customerid) = 3 THEN
                '0' || '0' || SUBSTR(customerid, 1, 1) || '0' || '0' || '0' || '0' || '0' || SUBSTR(customerid, 2, 1) || SUBSTR(customerid, 3, 1)
                WHEN LENGTH(customerid) = 2 THEN
                    customerid='0' || '0' || '0' || '0' || '0' || '0' || '0' || '0' || SUBSTR(customerid, 1, 1) || SUBSTR(customerid, 2, 1)
                WHEN LENGTH(customerid) = 1 THEN
                    customerid='0' || '0' || '0' || '0' || '0' || '0' || '0' || '0' || '0' || SUBSTR(customerid, 1, 1)
                ELSE
                    ''
            END as customerid
    from source
)
select * from updated_customers



