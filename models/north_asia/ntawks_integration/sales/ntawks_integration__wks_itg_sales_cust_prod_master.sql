
{{
    config(
        pre_hook= '{{build_itg_sales_cust_prod_master_temp()}}'
    )
}}

with sdl_mds_kr_sales_cust_prod_master as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_sales_cust_prod_master') }}
),
sdl_sales_cust_prod_reject_master as (
    select * from {{ source('ntasdl_raw', 'sdl_sales_cust_prod_reject_master') }}
),
itg_sales_cust_prod_master_temp as (
    select * from {{ source('ntaitg_integration', 'itg_sales_cust_prod_master_temp') }}
),
transformed as (
SELECT sales_grp_cd,
    src.src_sys_cd,
    product_nm,
    ltrim(src.cust_prod_cd, 0) as cust_prod_cd,
    ltrim(ean_cd, 0) as ean_cd,
    ctry_cd,
    tgt.crt_dttm as tgt_crt_dttm,
    null as updt_dttm,
    case 
      when tgt.crt_dttm is null
        THEN 'I'
      ELSE 'U'
      end as chng_flg from (
    select sales_grp_cd,
      src_sys_cd,
      product_nm,
      cust_prod_cd,
      ean_cd,
      ctry_cd
    from sdl_mds_kr_sales_cust_prod_master minus
    select sales_grp_cd,
      src_sys_cd,
      product_nm,
      cust_prod_cd,
      ean_cd,
      ctry_cd
    from sdl_sales_cust_prod_reject_master
    ) src left outer join (
    select distinct src_sys_cd,
      cust_prod_cd,
      crt_dttm
    from itg_sales_cust_prod_master_temp
    ) tgt on src.src_sys_cd = tgt.src_sys_cd
    and ltrim(src.cust_prod_cd, 0) = tgt.cust_prod_cd
),
final as (
select 
sales_grp_cd::varchar(200) as sales_grp_cd,
src_sys_cd::varchar(200) as src_sys_cd,
product_nm::varchar(200) as product_nm,
cust_prod_cd::varchar(200) as cust_prod_cd,
ean_cd::varchar(200) as ean_cd,
ctry_cd::varchar(200) as ctry_cd,
tgt_crt_dttm::timestamp_ntz(9) as tgt_crt_dttm,
updt_dttm::varchar(1) as updt_dttm,
chng_flg::varchar(1) as chng_flg
from transformed
)
select * from final
