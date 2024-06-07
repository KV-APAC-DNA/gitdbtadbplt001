{{
    config(
        pre_hook= '{{build_edw_product_attr_dim_temp()}}'
    )
}}
with v_prod_product as (
    select * from DEV_DNA_LOAD.SNAPASPSDL_RAW.V_PROD_PRODUCT
),
v_prodbu_productbusinessunit as (
    select * from DEV_DNA_LOAD.SNAPASPSDL_RAW.V_PRODBU_PRODUCTBUSINESSUNIT
),
v_prodtr_producttranslation as (
    select * from DEV_DNA_LOAD.SNAPASPSDL_RAW.V_PRODTR_PRODUCTTRANSLATION
),
EDW_VW_POP6_PRODUCTS as (
    select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_PRODUCTS
),
edw_material_sales_dim as (
    select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_MATERIAL_SALES_DIM
),
edw_product_attr_dim as (
    select * from {{ source('aspitg_integration', 'aspedw_integration__edw_product_attr_dim_temp') }}
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
       trim(SUBSTRING(CAST(SRC.product_local_name AS VARCHAR(100)),1,100)) AS lcl_prod_nm,
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
      FROM (SELECT  RES.productid,
					RES.eannumber,
					RES.productname,
					RES.materialnumber,
					RES.remotekey,
					RES.product_local_name,
					RES.hier1,
					RES.hier2,
					RES.hier3,
					RES.hier4,
					RES.hier5,
					RES.hier6,
					RES.hier7,
					RES.hier8,
					RES.cntry,
					RES.updt_dttm
 FROM
(SELECT a.productid,
                   a.eannumber,
                   a.productname,
                   a.materialnumber,
                   a.remotekey,
                   b.producttranslationname product_local_name,
                   --c.productbusinessunitid,c.productid,c.productremotekey,c.businessunitid,c.businessunitremotekey,
                   CASE
                     WHEN c.hier1 = 'HongKong' THEN 'Hong Kong'
                     ELSE c.hier1
                   END AS hier1,
                   c.hier2,
                   c.hier3,
                   c.hier4,
                   c.hier5,
                   c.hier6,
                   c.hier7,
                   c.hier8,
                   CASE
                     WHEN c.hier1 = 'Taiwan' THEN 'TW'
                     WHEN c.hier1 = 'HongKong' THEN 'HK'
                     WHEN c.hier1 = 'Hong Kong' THEN 'HK'
                     WHEN c.hier1 = 'Korea' THEN 'KR'
                     ELSE c.hier1
                   END AS cntry,
                   convert_timezone('UTC',current_timestamp()::timestamp_ntz(9)) UPDT_DTTM
            FROM (SELECT * FROM v_prod_product WHERE RANK = 1) a
              INNER JOIN (SELECT *
                          FROM v_prodbu_productbusinessunit
                          WHERE RANK = 1
                          AND   hier1 <> '') c
                      ON a.productid = c.productid
                     AND a.eannumber = c.productremotekey
              LEFT JOIN (SELECT *
                         FROM v_prodtr_producttranslation
                         WHERE RANK = 1) b
                     ON a.productid = b.productid
                    AND a.eannumber = b.eannumber
                    AND (DECODE (c.hier1,'Taiwan','TW','Korea','KR','HongKong','HK','Hong Kong','HK',c.hier1)) = UPPER (SUBSTRING (b.language,4,5))) RES
                    WHERE trim(eannumber)||trim(cntry)
                    NOT in (SELECT trim(barcode)||trim(cntry_cd) from EDW_VW_POP6_PRODUCTS where nvl(cntry_cd,'#') not in ('JP','SG'))) k
        LEFT OUTER JOIN (SELECT MAX(REPLACE(LTRIM(REPLACE(b.matl_num,'0',' ')),' ','0')) AS sap_matl_num,
                                a.ean_num,
                                a.cntry,
                                a.crt_dttm
                         FROM (SELECT * FROM edw_material_sales_dim) b,
                              (SELECT ean_num,
                                      sls_org,
                                      DECODE(UPPER(trim(sls_org)),
                                            '1110','HK',
                                            '320S','KR',
                                            '1200','TW',
                                            ''
                                      ) AS cntry,
                                      MAX(crt_dttm) AS crt_dttm
                               FROM edw_material_sales_dim
                               WHERE trim(ean_num) <> ''
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
