{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "delete
                    from {{this}} itg_sales_cust_prod_master using {{ ref('ntawks_integration__wks_itg_sales_cust_prod_master') }} wks_itg_sales_cust_prod_master
                    where wks_itg_sales_cust_prod_master.src_sys_cd = itg_sales_cust_prod_master.src_sys_cd
                    and wks_itg_sales_cust_prod_master.cust_prod_cd = itg_sales_cust_prod_master.cust_prod_cd
                    and wks_itg_sales_cust_prod_master.chng_flg = 'U';"
    )
}}
with wks_itg_sales_cust_prod_master as (
    select * from {{ ref('ntawks_integration__wks_itg_sales_cust_prod_master') }}
),
transformed as 
(select sales_grp_cd,
  src_sys_cd,
  product_nm,
  cust_prod_cd,
  ean_cd,
  ctry_cd,
  case 
    when chng_flg = 'I'
      then current_timestamp()
    else tgt_crt_dttm
    end as crt_dttm,
  current_timestamp() as updt_dttm 
  from wks_itg_sales_cust_prod_master
  ),
final as (
select 
sales_grp_cd::varchar(18) as sales_grp_cd,
src_sys_cd::varchar(100) as src_sys_cd,
product_nm::varchar(100) as product_nm,
cust_prod_cd::varchar(25) as cust_prod_cd,
ean_cd::varchar(25) as ean_cd,
ctry_cd::varchar(10) as ctry_cd,
crt_dttm::timestamp_ntz(9) as crt_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final