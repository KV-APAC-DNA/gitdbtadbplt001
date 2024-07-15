{{
    config(
        pre_hook= '{{build_edw_product_attr_dim_temp()}}'
    )
}}
with v_prod_product as (
    select * from {{ ref('aspitg_integration__v_prod_product') }}
),
v_prodbu_productbusinessunit as (
    select * from {{ ref('aspitg_integration__v_prodbu_productbusinessunit') }}
),
v_prodtr_producttranslation as (
    select * from {{ ref('aspitg_integration__v_prodtr_producttranslation') }}
),
edw_vw_pop6_products as (
    select * from {{ ref('aspedw_integration__edw_vw_pop6_products') }}
),
edw_material_sales_dim as (
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_product_attr_dim as (
    select * from {{ source('aspedw_integration', 'edw_product_attr_dim_temp') }}
),
final as 
(SELECT DISTINCT SRC.eannumber AS aw_remote_key,
       SRC.remotekey AS awrefs_prod_remotekey,
       SRC.hier1 AS awrefs_buss_unit,
       SRC.sap_matl_num AS sap_matl_num,
       CASE
         WHEN SRC.cntry IS NULL THEN TGT.cntry
         ELSE SRC.cntry
       END AS cntry,
       SRC.eannumber AS ean,
       SRC.hier1 AS prod_hier_l1,
       SRC.hier2 AS prod_hier_l2,
       SRC.hier3 AS prod_hier_l3,
       SRC.hier4 AS prod_hier_l4,
       SRC.hier5 AS prod_hier_l5,
       SRC.hier6 AS prod_hier_l6,
       SRC.hier7 AS prod_hier_l7,
       SRC.hier8 AS prod_hier_l8,
       SRC.productname AS prod_hier_l9,
       TRIM(SUBSTRING(CAST(SRC.product_local_name AS VARCHAR(100)),1,100)) AS lcl_prod_nm,
       TGT.CRT_DTTM AS TGT_CRT_DTTM,
       SRC.UPDT_DTTM,
       CASE
         WHEN TGT.CRT_DTTM IS NULL THEN 'I'
         ELSE 'U'
       END AS CHNG_FLG
FROM (SELECT DISTINCT k.productid,
             REPLACE(REPLACE(k.eannumber,'-',''),' ','') AS eannumber,
             k.productname,
             k.materialnumber,
             REPLACE(REPLACE(k.remotekey,'-',''),' ','') AS remotekey,
             k.hier1,
             k.hier2,
             k.hier3,
             k.hier4,
             k.hier5,
             k.hier6,
             k.hier7,
             k.hier8,
             k.cntry,
             k.product_local_name,
             k.UPDT_DTTM,
             m.sap_matl_num
      FROM (SELECT productdb_id AS productid,
                   barcode AS eannumber,
                   sku_english AS productname,
                   NULL AS materialnumber,
                   barcode AS remotekey,
                   sku AS product_local_name,
                   CASE 
                        WHEN country_l1 = 'HongKong' 
                        THEN 'Hong Kong'
                        ELSE country_l1
                   END AS hier1,
                   regional_franchise_l2 AS hier2,
                   franchise_l3 AS hier3,
                   brand_l4 AS hier4,
                   sub_category_l5 AS hier5,
                   platform_l6 AS hier6,
                   variance_l7 AS hier7,
                   pack_size_l8 AS hier8,
                   cntry_cd AS cntry,
                   convert_timezone('UTC',current_timestamp()::timestamp_ntz(9)) as UPDT_DTTM
            FROM EDW_VW_POP6_PRODUCTS where nvl(cntry_cd,'#') not in ('JP','SG'))k
        LEFT OUTER JOIN (SELECT MAX(REPLACE(LTRIM(REPLACE(b.matl_num,'0',' ')),' ','0')) AS sap_matl_num,
                                a.ean_num,
                                a.cntry,
                                a.crt_dttm
                         FROM (SELECT * FROM edw_material_sales_dim) b,
                              (SELECT ean_num,
                                      sls_org,
                                      DECODE(UPPER(TRIM(sls_org)),
                                            '1110','HK',
                                            '320S','KR',
                                            '1200','TW',
                                            ''
                                      ) AS cntry,
                                      MAX(crt_dttm) AS crt_dttm
                               FROM edw_material_sales_dim
                               WHERE TRIM(ean_num) <> ''
                               AND   sls_org IN ('1110','320S','1200')
                               GROUP BY ean_num,
                                        sls_org) a
                         WHERE b.ean_num = a.ean_num
                         AND   b.sls_org = a.sls_org
                         AND   b.crt_dttm = a.crt_dttm
                         GROUP BY a.ean_num,
                                  a.cntry,
                                  a.crt_dttm) m
                     ON k.eannumber = m.ean_num
                    AND k.cntry = m.cntry) SRC
  LEFT OUTER JOIN (SELECT DISTINCT * FROM edw_product_attr_dim) TGT
               ON SRC.eannumber = TGT.ean
              AND SRC.cntry = TGT.cntry)
select * from final
