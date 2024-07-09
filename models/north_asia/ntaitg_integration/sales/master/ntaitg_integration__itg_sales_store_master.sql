{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "delete
                    from {{this}} itg_sales_store_master using {{ ref('ntawks_integration__wks_itg_sales_store_master') }} wks_itg_sales_store_master
                    where  wks_itg_sales_store_master.cust_store_cd = itg_sales_store_master.cust_store_cd
                    and   wks_itg_sales_store_master.chng_flg = 'U';"
    )
}}
with wks_itg_sales_store_master as (
    select * from {{ ref('ntawks_integration__wks_itg_sales_store_master') }}
),
transformed as 
(SELECT 
		channel,
		store_type,
		sales_grp_cd ,
		sold_to ,
		store_nm  ,
		cust_store_cd  ,
		ctry_cd  ,
		CASE WHEN CHNG_FLG = 'I' THEN current_timestamp() ELSE TGT_CRT_DTTM END AS CRT_DTTM,
		current_timestamp() AS UPDT_DTTM 
  FROM wks_itg_sales_store_master
  ),
final as (
select 
channel::varchar(25) as channel,
store_type::varchar(25) as store_type,
sales_grp_cd::varchar(18) as sales_grp_cd,
sold_to::varchar(25) as sold_to,
store_nm::varchar(100) as store_nm,
cust_store_cd::varchar(18) as cust_store_cd,
ctry_cd::varchar(10) as ctry_cd,
crt_dttm::timestamp_ntz(9) as crt_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final