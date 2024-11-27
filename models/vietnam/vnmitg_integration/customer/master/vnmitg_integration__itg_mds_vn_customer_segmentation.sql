{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['cust_num']
    )
}}

with source as (
    select * from {{ source('vnmsdl_raw','sdl_mds_vn_customer_segmentation') }}
),

final as
(
    select
    code::varchar(500) as cust_num,
	customer_segmentation_level_1_code::varchar(500) as customer_segmentation_level_1_code,
	customer_segmentation_level_2_code::varchar(500) as customer_segmentation_level_2_code,
    accounttype_code::varchar(500) as accounttype_code,
    regionalre_code::varchar(500) as regionalre_code,
    channel_code::varchar(500) as channel_code,
    channel_name::varchar(500) as channel_name,
    sold_to_code_name::varchar(500) as sold_to_code_name,
	current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final