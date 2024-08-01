{{
    config(
        pre_hook="{{build_itg_pos_prom_prc_map_temp()}}"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw', 'sdl_mds_tw_pos_promotional_price_ean_map') }}
),
itg_pos_prom_prc_map_temp as(
    select * from {{ source('ntaitg_integration', 'itg_pos_prom_prc_map_temp') }}
),
transformed as(
    SELECT 
       SRC.customer as customer,
       rtrim(SRC.barcode, ' ') as barcode,
       rtrim(SRC.cust_prod_cd, ' ') as cust_prod_cd,
       SRC.promotional_price,
       SRC.promotion_start_date,
       SRC.promotion_end_date,
       TGT.CRT_DTTM AS TGT_CRT_DTTM,
       null as UPDT_DTTM,
       CASE
         WHEN TGT.CRT_DTTM IS NULL THEN 'I'
         ELSE 'U'
       END AS CHNG_FLG
FROM source SRC
  LEFT OUTER JOIN (SELECT distinct cust,cust_prod_cd,barcd, CRT_DTTM FROM itg_pos_prom_prc_map_temp) TGT
  ON 
  rtrim(SRC.barcode, ' ')=rtrim(TGT.barcd, ' ')
  AND RTRIM(SRC.cust_prod_cd,' ')=rtrim(TGT.cust_prod_cd, ' ')
  AND SRC.customer=TGT.cust
)
select * from transformed
