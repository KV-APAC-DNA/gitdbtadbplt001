
{{
    config(
        pre_hook= '{{build_itg_sales_store_master_temp()}}'
    )
}}

with sdl_mds_kr_sales_store_master as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_sales_store_master') }}
),
itg_sales_store_master_temp as (
    select * from {{ source('ntaitg_integration', 'itg_sales_store_master_temp') }}
),
transformed as (
SELECT distinct channel,
        store_type,
        src.sales_grp_cd ,
        sold_to ,
        store_nm  ,
        src.cust_store_cd  ,
        ctry_cd  ,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        null as UPDT_DTTM,
       CASE
         WHEN TGT.CRT_DTTM IS NULL THEN 'I'
         ELSE 'U'
       END AS CHNG_FLG
FROM sdl_mds_kr_sales_store_master SRC
  LEFT OUTER JOIN (SELECT distinct 
                          cust_store_cd,
                          CRT_DTTM
                   FROM itg_sales_store_master_temp) TGT
               ON 
               SRC.cust_store_cd = TGT.cust_store_cd
),
final as (
select 
channel::varchar(200) as channel,
store_type::varchar(200) as store_type,
sales_grp_cd::varchar(200) as sales_grp_cd,
sold_to::varchar(200) as sold_to,
store_nm::varchar(200) as store_nm,
cust_store_cd::varchar(200) as cust_store_cd,
ctry_cd::varchar(200) as ctry_cd,
tgt_crt_dttm::timestamp_ntz(9) as tgt_crt_dttm,
updt_dttm::varchar(1) as updt_dttm,
chng_flg::varchar(1) as chng_flg
from transformed
)
select * from final
