{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["cust_num"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}
with source as
(
    select * from {{ source('ntasdl_raw', 'sdl_kr_sales_target_am_cust_link') }}
),
final as
(
    SELECT
        customer_code::varchar(30) as cust_num,
        account_manager::varchar(50) as acct_mgr,
        country_code::varchar(10) as ctry_cd,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final