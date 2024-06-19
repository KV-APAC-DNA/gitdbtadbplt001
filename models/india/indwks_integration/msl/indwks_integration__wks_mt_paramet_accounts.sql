with source as(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_key_accounts_mapping') }}
),
transformed as(
    SELECT 
        channel_name_code AS channel_name,
        account_name_name AS account_name,
        account_name_as_per_offtake_data_code 
    FROM source 
    WHERE account_name_as_per_offtake_data_code 
    IN ('DMART ALL INDIA', 'APOLLO ALL INDIA', 'RELIANCE ALL INDIA', 'ABRL ALL INDIA', 'Amazon', 'Bigbasket', 'Flipkart', 'Nykaa') 
    GROUP BY 1, 2, 3
)
select * from transformed