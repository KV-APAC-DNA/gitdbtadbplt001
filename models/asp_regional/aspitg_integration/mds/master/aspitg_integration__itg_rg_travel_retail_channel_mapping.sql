--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_apac_dcl_customers') }}
),

--Final CTE
final as (
    select 
        trim (sales_channel_name) :: varchar(100) as sales_channel,
        trim(sales_location_name) :: varchar(100) as sales_location,
        trim(cust_num) :: varchar(30) as cust_num,
        trim(name) :: varchar(255) as cust_nm,
        upper(trim(ctry_key_code)) :: varchar(5) as ctry_cd,
        trim(ctry_key_name) :: varchar(30) as country_name,
        upper(trim(retailer_name_code)) :: varchar(100) as retailer_name,
        upper(trim(location_name)) :: varchar(100) as door_name,
        trim(remarks) :: varchar(100) as remarks,
        trim(enterdatetime) :: timestamp_ntz(9) as crt_dttm,
        current_timestamp() :: timestamp_ntz(9) as updt_dttm
    from source
)

--Final select
select * from final
