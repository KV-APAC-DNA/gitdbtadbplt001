with sdl_mds_ph_npi_sales_groupings as (
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_npi_sales_groupings') }}
),
final as (
SELECT 
code::varchar(50) as cust_cd,
name::varchar(255) as cust_name,
account_name_code::varchar(255) as account_name,
sales_grouping::varchar(255) as sls_grping,
trade_type::varchar(255) as trade_type,
prefix::varchar(50) as prefix,
dist_code_code::varchar(30) as dist_grp_cd,
dist_code_name::varchar(255) as dist_grp_nm,
current_timestamp()::timestamp_ntz(9) as crtd_dttm,
null::timestamp_ntz(9) as updt_dttm 
FROM sdl_mds_ph_npi_sales_groupings
)
select * from final

       


       

       




      