

with sdl_mds_kr_sales_store_master as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_sales_store_master') }}
)
final as (
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
                   FROM na_itg.itg_sales_store_master) TGT
               ON 
               SRC.cust_store_cd = TGT.cust_store_cd
)
select * from final
