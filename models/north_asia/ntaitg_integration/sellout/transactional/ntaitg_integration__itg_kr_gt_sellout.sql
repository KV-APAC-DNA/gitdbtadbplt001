{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= ["{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE (
              IMS_TXN_DT,
              UPPER(DSTR_NM),
              CUST_CD,
              EAN_NUM
              ) IN (
              
              SELECT DISTINCT TO_DATE(IMS_TXN_DT || '15', 'YYYYMMDD') AS IMS_TXN_DT,
                     UPPER(DSTR_NM),
                     CUST_CD,
                     EAN
              FROM {{ source('ntasdl_raw','sdl_kr_hyundai_gt_sellout') }}
              
              UNION ALL
              
              SELECT DISTINCT TO_DATE(IMS_TXN_DT || '15', 'YYYYMMDD') AS IMS_TXN_DT,
                     CASE 
                            WHEN SUB_CUSTOMER_NAME LIKE '%AK%'
                                   THEN UPPER(SPLIT_PART(DSTR_NM, '_', 2))
                            ELSE UPPER(SPLIT_PART(DSTR_NM, '_', 1))
                            END AS DSTR_NM,
                     CUST_CD,
                     EAN
              FROM {{ source('ntasdl_raw','sdl_kr_lotte_ak_gt_sellout') }}
              
            --   UNION ALL
              
            --   SELECT TO_DATE(replace(IMS_TXN_DT,'.','-'), 'YYYY-MM-DD'),
            --          UPPER(DSTR_NM),
            --          CUST_CD,
            --          EAN
            --   FROM {{ source('ntasdl_raw','sdl_kr_ju_hj_life_gt_sellout') }}
              
              UNION ALL
              
              SELECT TO_DATE(IMS_TXN_DT, 'YYYY-MM-DD'),
                     UPPER(DSTR_NM),
                     CUST_CD,
                     EAN
              FROM {{ source('ntasdl_raw','sdl_kr_bo_young_jong_hap_logistics_gt_sellout') }}
              
              UNION ALL
              
              SELECT TO_DATE(replace(IMS_TXN_DT,'.','-'), 'YY-MM-DD'),
                     UPPER(DSTR_NM),
                     CUST_CD,
                     EAN
              FROM {{ source('ntasdl_raw','sdl_kr_da_in_sang_sa_gt_sellout') }}
              
              UNION ALL
              
              SELECT TO_DATE(SPLIT_PART(IMS_TXN_DT, ' ', 6) || '-' || SPLIT_PART(IMS_TXN_DT, ' ', 2) || '-' || SPLIT_PART(IMS_TXN_DT, ' ', 3), 'YYYY-MON-DD'),
                     UPPER(DSTR_NM),
                     CUST_CD,
                     EAN
              FROM {{ source('ntasdl_raw','sdl_kr_dongbu_lsd_gt_sellout') }}
              
              UNION ALL
              
              SELECT TO_DATE(IMS_TXN_DT, 'YYYY-MM-DD'),
                     UPPER(DSTR_NM),
                     CUST_CD,
                     EAN
              FROM {{ source('ntasdl_raw','sdl_kr_du_bae_ro_yu_tong_gt_sellout') }}
              
              UNION ALL
              
              SELECT TO_DATE(IMS_TXN_DT, 'YYYY-MM-DD'),
                     UPPER(DSTR_NM),
                     CUST_CD,
                     EAN
              FROM {{ source('ntasdl_raw','sdl_kr_il_dong_hu_di_s_deok_seong_sang_sa_gt_sellout') }}
              
              UNION ALL
              
              SELECT DISTINCT TO_DATE(IMS_TXN_DT || '15', 'YYYYMMDD') AS IMS_TXN_DT,
                     UPPER(DSTR_NM),
                     CUST_CD,
                     EAN
              FROM {{ source('ntasdl_raw','sdl_kr_jungseok_gt_sellout') }}
              
              UNION ALL
              
              SELECT TO_DATE(IMS_TXN_DT, 'YYYYMMDD'),
                     UPPER(DSTR_NM),
                     CUST_CD,
                     EAN
              FROM {{ source('ntasdl_raw','sdl_kr_nu_ri_zon_gt_sellout') }} 
              
              UNION ALL
              
              SELECT DISTINCT TO_DATE(IMS_TXN_DT || '15', 'YYYYMMDD') AS IMS_TXN_DT,
                     UPPER(DSTR_NM),
                     CUST_CD,
                     EAN
              FROM {{ source('ntasdl_raw','sdl_kr_lotte_logistics_yang_ju_gt_sellout') }})
              {% endif %}",
              "{% if is_incremental() %}
              DELETE FROM {{this}}
              WHERE (
              IMS_TXN_DT,
              UPPER(DSTR_NM),
              EAN_NUM
              ) IN (          
              SELECT TO_DATE(IMS_TXN_DT, 'YYYY-MM-DD'),
                     UPPER(DSTR_NM),
                     EAN
              FROM {{ source('ntasdl_raw','sdl_kr_nacf_gt_sellout') }}
              )
            {% endif %}","
            {% if is_incremental() %}
            DELETE
FROM {{this}}
WHERE (IMS_TXN_DT,UPPER(DSTR_CD),EAN_NUM,cust_cd) IN (SELECT CASE WHEN (SNG.IMS_TXN_DT IS NULL OR SNG.IMS_TXN_DT='') THEN CAL.CAL_DAY ELSE TO_DATE(replace(SNG.IMS_TXN_DT,'/','-'),'MM-DD-YY') END AS IMS_TXN_DT,
                                              UPPER(DSTR_NM) DSTR_NM,
                                              EAN,customer_code
                                       FROM {{ source('ntasdl_raw','sdl_kr_nh_gt_sellout') }} sng
                                        LEFT JOIN (SELECT FISC_PER,
                    MAX(CAL_DAY) CAL_DAY
             FROM aspedw_integration.edw_calendar_dim
             WHERE WKDAY = '7'
             GROUP BY FISC_PER) CAL ON SPLIT_PART(SPLIT_PART(SNG.FILE_NAME,'_',2),'.',1) = SUBSTRING (CAL.FISC_PER,1,4) ||SUBSTRING (CAL.FISC_PER,6,7) );
             {% endif %}",
             "{% if is_incremental() %}
             DELETE
             FROM {{this}}
                WHERE (IMS_TXN_DT,UPPER(DSTR_CD),EAN_NUM,cust_cd,SUB_CUSTOMER_CODE) IN (SELECT CASE WHEN (SNG.IMS_TXN_DT IS NULL OR SNG.IMS_TXN_DT='') THEN CAL.CAL_DAY ELSE TO_DATE(replace(SNG.IMS_TXN_DT,'/','-'),'MM-DD-YY') END AS IMS_TXN_DT,
                                                            UPPER(DSTR_NM) DSTR_NM,
                                                            EAN,customer_code,pcode
                                                    FROM {{ source('ntasdl_raw','sdl_kr_otc_sellout') }} SNG
                                                        LEFT JOIN (SELECT FISC_PER,
                                    MAX(CAL_DAY) CAL_DAY
                            FROM aspedw_integration.EDW_CALENDAR_DIM
                            WHERE WKDAY = '7'
                            GROUP BY FISC_PER) CAL ON SPLIT_PART(SPLIT_PART(SNG.FILE_NAME,'_',3),'.',1) = SUBSTRING (CAL.FISC_PER,1,4) ||SUBSTRING (CAL.FISC_PER,6,7) )
             {% endif %}"  ]
    )
}}



with 
sdl_kr_otc_sellout as (
select * from  {{ source('ntasdl_raw','sdl_kr_otc_sellout') }}
),
sdl_kr_ju_hj_life_gt_sellout as (
select * from {{ source('ntasdl_raw','sdl_kr_ju_hj_life_gt_sellout') }}
),
sdl_kr_jungseok_gt_sellout as (
select * from {{ source('ntasdl_raw','sdl_kr_jungseok_gt_sellout') }}
),
sdl_kr_nacf_gt_sellout as (
select * from {{ source('ntasdl_raw','sdl_kr_nacf_gt_sellout') }}
),
sdl_kr_hyundai_gt_sellout as (
select * from {{ source('ntasdl_raw','sdl_kr_hyundai_gt_sellout') }}
),
sdl_kr_bo_young_jong_hap_logistics_gt_sellout as (
select * from {{ source('ntasdl_raw','sdl_kr_bo_young_jong_hap_logistics_gt_sellout') }}
),
sdl_kr_lotte_logistics_yang_ju_gt_sellout as (
select * from {{ source('ntasdl_raw','sdl_kr_lotte_logistics_yang_ju_gt_sellout') }}
),
sdl_kr_nu_ri_zon_gt_sellout as (
select * from {{ source('ntasdl_raw','sdl_kr_nu_ri_zon_gt_sellout') }} 
),
sdl_kr_lotte_ak_gt_sellout as (
select * from {{ source('ntasdl_raw','sdl_kr_lotte_ak_gt_sellout') }}
),
sdl_kr_dongbu_lsd_gt_sellout as (
select * from {{ source('ntasdl_raw','sdl_kr_dongbu_lsd_gt_sellout') }}
),
sdl_kr_da_in_sang_sa_gt_sellout as (
select * from {{ source('ntasdl_raw','sdl_kr_da_in_sang_sa_gt_sellout') }}
),
sdl_kr_nh_gt_sellout as (
    select * from {{ source('ntasdl_raw','sdl_kr_nh_gt_sellout') }}
),
sdl_kr_il_dong_hu_di_s_deok_seong_sang_sa_gt_sellout as (
select * from {{ source('ntasdl_raw','sdl_kr_il_dong_hu_di_s_deok_seong_sang_sa_gt_sellout') }}
),
sdl_kr_du_bae_ro_yu_tong_gt_sellout as (
select * from {{ source('ntasdl_raw','sdl_kr_du_bae_ro_yu_tong_gt_sellout') }}
),
itg_kr_gt_food_ws as (
select * from {{ ref('ntaitg_integration__itg_kr_gt_food_ws') }}
),
itg_kr_gt_daiso_price as (
select * from {{ ref('ntaitg_integration__itg_kr_gt_daiso_price') }}
),
itg_kr_gt_nacf_cust_dim as (
select * from {{ ref('ntaitg_integration__itg_kr_gt_nacf_cust_dim') }}
),
itg_kr_gt_dpt_daiso as (
select * from {{ ref('ntaitg_integration__itg_kr_gt_dpt_daiso') }}
),
edw_calendar_dim as (
select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_customer_base_dim as (
select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
itg_pop6_products as (
select * from {{ ref('ntaitg_integration__itg_pop6_products') }}
),
edw_product_attr_dim as (
select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
),
itg_mds_kr_sub_customer_master as (
select * from {{ ref('ntaitg_integration__itg_mds_kr_sub_customer_master') }}
),
hyundai as (
SELECT TO_DATE(SHG.IMS_TXN_DT||'15','YYYYMMDD') AS IMS_TXN_DT,
       'NA' AS DSTR_CD,
       UPPER(SHG.DSTR_NM) AS DSTR_NM,
       SHG.CUST_CD,
       RCT.CUST_NM,
       RPT.MATERIALNUMBER AS PROD_CD,
       RPT.PRODUCTNAME AS PROD_NM,
       SHG.EAN AS EAN_NUM,
       CAST(SHG.UNIT_PRICE AS NUMERIC(21,5)) AS UNIT_PRC,
       (SHG.UNIT_PRICE*CAST(SHG.QTY AS NUMERIC(21,5))) AS SLS_AMT,
       CAST(SHG.QTY AS INTEGER) SLS_QTY,
       LCD.SUB_CUSTOMER_CODE,
       SHG.SUB_CUSTOMER_NAME,
       'KR' AS CTRY_CD,
       'KRW' AS CRNCY_CD,
       current_timestamp() AS CRT_DTTM,
       current_timestamp() AS UPDT_DTTM,
       null as SALES_PRIORITY,
       null as sales_stores,
       null as sales_rate
FROM SDL_KR_HYUNDAI_GT_SELLOUT SHG
  LEFT JOIN ITG_KR_GT_DPT_DAISO LCD
         ON SHG.CUST_CD = LCD.CUSTOMER_CODE
        AND SHG.SUB_CUSTOMER_NAME = LCD.SUB_CUSTOMER_NAME
  LEFT JOIN (SELECT DISTINCT CUST_NUM AS CUST_CD,
                    CUST_NM
             FROM EDW_CUSTOMER_BASE_DIM) RCT ON SHG.CUST_CD = LTRIM (RCT.CUST_CD,0)
  LEFT JOIN (SELECT DISTINCT PP.CNTRY_CD,
                    PP.BARCODE AS EANNUMBER,
                    PA.SAP_MATL_NUM AS MATERIALNUMBER,
                    PP.SKU_ENGLISH AS PRODUCTNAME
             FROM ITG_POP6_PRODUCTS PP
               LEFT JOIN EDW_PRODUCT_ATTR_DIM PA
                      ON PP.BARCODE = PA.EAN
                     AND UPPER (PP.CNTRY_CD) = UPPER (PA.CNTRY)
             WHERE UPPER(PP.CNTRY_CD) = 'KR'
             AND   UPPER(PP.ACTIVE) = 'Y') RPT ON SHG.EAN = RPT.EANNUMBER
),
lotte as (
SELECT TO_DATE(SLA.IMS_TXN_DT||'15','YYYYMMDD') AS IMS_TXN_DT,
       'NA' AS DSTR_CD,
       CASE
         WHEN SLA.SUB_CUSTOMER_NAME LIKE '%AK%' THEN UPPER(SPLIT_PART(SLA.DSTR_NM,'_',2))
         ELSE UPPER(SPLIT_PART(SLA.DSTR_NM,'_',1))
       END AS DSTR_NM,
       SLA.CUST_CD,
       RCT.CUST_NM,
       RPT.MATERIALNUMBER AS PROD_CD,
       RPT.PRODUCTNAME AS PROD_NM,
       SLA.EAN AS EAN_NUM,
       CAST(SLA.UNIT_PRICE AS NUMERIC(21,5)) AS UNIT_PRC,
       (SLA.UNIT_PRICE*CAST(SLA.QTY AS NUMERIC(21,5))) AS SLS_AMT,
       CAST(SLA.QTY AS INTEGER) SLS_QTY,
       LCD.SUB_CUSTOMER_CODE,
       SLA.SUB_CUSTOMER_NAME,
       'KR' AS CTRY_CD,
       'KRW' AS CRNCY_CD,
       current_timestamp() AS CRT_DTTM,
       current_timestamp() AS UPDT_DTTM,
       null as SALES_PRIORITY,
       null as sales_stores,
       null as sales_rate
FROM SDL_KR_LOTTE_AK_GT_SELLOUT SLA
  LEFT JOIN ITG_KR_GT_DPT_DAISO LCD
         ON SLA.CUST_CD = LCD.CUSTOMER_CODE
        AND SLA.SUB_CUSTOMER_NAME = LCD.SUB_CUSTOMER_NAME
  LEFT JOIN (SELECT DISTINCT CUST_NUM AS CUST_CD,
                    CUST_NM
             FROM EDW_CUSTOMER_BASE_DIM) RCT ON SLA.CUST_CD = LTRIM (RCT.CUST_CD,0)
  LEFT JOIN (SELECT DISTINCT PP.CNTRY_CD,
                    PP.BARCODE AS EANNUMBER,
                    PA.SAP_MATL_NUM AS MATERIALNUMBER,
                    PP.SKU_ENGLISH AS PRODUCTNAME
             FROM ITG_POP6_PRODUCTS PP
               LEFT JOIN EDW_PRODUCT_ATTR_DIM PA
                      ON PP.BARCODE = PA.EAN
                     AND UPPER (PP.CNTRY_CD) = UPPER (PA.CNTRY)
             WHERE UPPER(PP.CNTRY_CD) = 'KR'
             AND   UPPER(PP.ACTIVE) = 'Y') RPT ON SLA.EAN = RPT.EANNUMBER
),
jong_hap as (
SELECT TO_DATE(SBY.IMS_TXN_DT, 'YYYY-MM-DD') AS IMS_TXN_DT,
  'NA' AS DSTR_CD,
  DSTR_NM,
  SBY.CUST_CD,
  RCT.CUST_NM,
  RPT.MATERIALNUMBER AS PROD_CD,
  RPT.PRODUCTNAME AS PROD_NM,
  SBY.EAN AS EAN_NUM,
  CAST(SBY.UNIT_PRICE AS NUMERIC(21, 5)) AS UNIT_PRC,
  (SBY.UNIT_PRICE * CAST(SBY.QTY AS NUMERIC(21, 5))) AS SLS_AMT,
  CAST(SBY.QTY AS INTEGER) SLS_QTY,
  IFS.SUB_CUSTOMER_CODE,
  SBY.SUB_CUSTOMER_NAME,
  'KR' AS CTRY_CD,
  'KRW' AS CRNCY_CD,
  current_timestamp() AS CRT_DTTM,
  current_timestamp() AS UPDT_DTTM,
  null as SALES_PRIORITY,
       null as sales_stores,
       null as sales_rate
FROM SDL_KR_BO_YOUNG_JONG_HAP_LOGISTICS_GT_SELLOUT SBY
LEFT JOIN ITG_KR_GT_FOOD_WS IFS ON SBY.CUST_CD = IFS.CUSTOMER_CODE
  AND SBY.SUB_CUSTOMER_NAME = IFS.SUB_CUSTOMER_NAME
LEFT JOIN (
  SELECT DISTINCT CUST_NUM AS CUST_CD,
    CUST_NM
  FROM EDW_CUSTOMER_BASE_DIM
  ) RCT ON SBY.CUST_CD = LTRIM(RCT.CUST_CD, 0)
LEFT JOIN (
  SELECT DISTINCT PP.CNTRY_CD,
    PP.BARCODE AS EANNUMBER,
    PA.SAP_MATL_NUM AS MATERIALNUMBER,
    PP.SKU_ENGLISH AS PRODUCTNAME
  FROM ITG_POP6_PRODUCTS PP
  LEFT JOIN EDW_PRODUCT_ATTR_DIM PA ON PP.BARCODE = PA.EAN
    AND UPPER(PP.CNTRY_CD) = UPPER(PA.CNTRY)
  WHERE UPPER(PP.CNTRY_CD) = 'KR'
    AND UPPER(PP.ACTIVE) = 'Y'
  ) RPT ON SBY.EAN = RPT.EANNUMBER
  ),
sang_sa as
(SELECT TO_DATE(replace(SDIG.IMS_TXN_DT,'.','-'), 'YY-MM-DD') AS IMS_TXN_DT,
  'NA' AS DSTR_CD,
  DSTR_NM,
  SDIG.CUST_CD,
  RCT.CUST_NM,
  RPT.MATERIALNUMBER AS PROD_CD,
  RPT.PRODUCTNAME AS PROD_NM,
  SDIG.EAN AS EAN_NUM,
  SDIG.PRICE AS UNIT_PRC,
  (SDIG.PRICE * CAST(SDIG.QTY AS NUMERIC(21, 5))) AS SLS_AMT,
  CAST(SDIG.QTY AS INTEGER) SLS_QTY,
  IFS.SUB_CUSTOMER_CODE,
  SDIG.SUB_CUSTOMER_NAME,
  'KR' AS CTRY_CD,
  'KRW' AS CRNCY_CD,
  current_timestamp() AS CRT_DTTM,
  current_timestamp() AS UPDT_DTTM,
  null as SALES_PRIORITY,
       null as sales_stores,
       null as sales_rate
FROM SDL_KR_DA_IN_SANG_SA_GT_SELLOUT SDIG
LEFT JOIN ITG_KR_GT_FOOD_WS IFS ON SDIG.CUST_CD = IFS.CUSTOMER_CODE
  AND SDIG.SUB_CUSTOMER_NAME = IFS.SUB_CUSTOMER_NAME
LEFT JOIN (
  SELECT DISTINCT CUST_NUM AS CUST_CD,
    CUST_NM
  FROM EDW_CUSTOMER_BASE_DIM
  ) RCT ON SDIG.CUST_CD = LTRIM(RCT.CUST_CD, 0)
LEFT JOIN (
  SELECT DISTINCT PP.CNTRY_CD,
    PP.BARCODE AS EANNUMBER,
    PA.SAP_MATL_NUM AS MATERIALNUMBER,
    PP.SKU_ENGLISH AS PRODUCTNAME
  FROM ITG_POP6_PRODUCTS PP
  LEFT JOIN EDW_PRODUCT_ATTR_DIM PA ON PP.BARCODE = PA.EAN
    AND UPPER(PP.CNTRY_CD) = UPPER(PA.CNTRY)
  WHERE UPPER(PP.CNTRY_CD) = 'KR'
    AND UPPER(PP.ACTIVE) = 'Y'
  ) RPT ON SDIG.EAN = RPT.EANNUMBER
  ),
dongbu_lsd as ( 
SELECT TO_DATE(SPLIT_PART(SDL.IMS_TXN_DT, ' ', 6) || '-' || SPLIT_PART(SDL.IMS_TXN_DT, ' ', 2) || '-' || SPLIT_PART(SDL.IMS_TXN_DT, ' ', 3), 'YYYY-MON-DD'),
  'NA' AS DSTR_CD,
  DSTR_NM,
  SDL.CUST_CD,
  RCT.CUST_NM,
  RPT.MATERIALNUMBER AS PROD_CD,
  RPT.PRODUCTNAME AS PROD_NM,
  SDL.EAN AS EAN_NUM,
  SDL.UNIT_PRICE AS UNIT_PRC,
  (SDL.UNIT_PRICE * CAST(SDL.QTY AS NUMERIC(21, 5))) AS SLS_AMT,
  CAST(SDL.QTY AS INTEGER) SLS_QTY,
  IFS.SUB_CUSTOMER_CODE,
  SDL.SUB_CUSTOMER_NAME,
  'KR' AS CTRY_CD,
  'KRW' AS CRNCY_CD,
  current_timestamp() AS CRT_DTTM,
  current_timestamp() AS UPDT_DTTM,
  null as SALES_PRIORITY,
       null as sales_stores,
       null as sales_rate
FROM SDL_KR_DONGBU_LSD_GT_SELLOUT SDL
LEFT JOIN ITG_KR_GT_FOOD_WS IFS ON SDL.CUST_CD = IFS.CUSTOMER_CODE
  AND SDL.SUB_CUSTOMER_NAME = IFS.SUB_CUSTOMER_NAME
LEFT JOIN (
  SELECT DISTINCT CUST_NUM AS CUST_CD,
    CUST_NM
  FROM EDW_CUSTOMER_BASE_DIM
  ) RCT ON SDL.CUST_CD = LTRIM(RCT.CUST_CD, 0)
LEFT JOIN (
  SELECT DISTINCT PP.CNTRY_CD,
    PP.BARCODE AS EANNUMBER,
    PA.SAP_MATL_NUM AS MATERIALNUMBER,
    PP.SKU_ENGLISH AS PRODUCTNAME
  FROM ITG_POP6_PRODUCTS PP
  LEFT JOIN EDW_PRODUCT_ATTR_DIM PA ON PP.BARCODE = PA.EAN
    AND UPPER(PP.CNTRY_CD) = UPPER(PA.CNTRY)
  WHERE UPPER(PP.CNTRY_CD) = 'KR'
    AND UPPER(PP.ACTIVE) = 'Y'
  ) RPT ON SDL.EAN = RPT.EANNUMBER
  ),
DU_BAE_RO_YU_TONG as (
SELECT TO_DATE(SDB.IMS_TXN_DT, 'YYYY-MM-DD') AS IMS_TXN_DT,
  'NA' AS DSTR_CD,
  DSTR_NM,
  SDB.CUST_CD,
  RCT.CUST_NM,
  RPT.MATERIALNUMBER AS PROD_CD,
  RPT.PRODUCTNAME AS PROD_NM,
  SDB.EAN AS EAN_NUM,
  CAST(SDB.UNIT_PRICE AS NUMERIC(21, 5)) AS UNIT_PRC,
  (SDB.UNIT_PRICE * CAST(SDB.QTY AS NUMERIC(21, 5))) AS SLS_AMT,
  CAST(SDB.QTY AS INTEGER) SLS_QTY,
  IFS.SUB_CUSTOMER_CODE,
  SDB.SUB_CUSTOMER_NAME,
  'KR' AS CTRY_CD,
  'KRW' AS CRNCY_CD,
  current_timestamp() AS CRT_DTTM,
  current_timestamp() AS UPDT_DTTM,
  null as SALES_PRIORITY,
       null as sales_stores,
       null as sales_rate
FROM SDL_KR_DU_BAE_RO_YU_TONG_GT_SELLOUT SDB
LEFT JOIN ITG_KR_GT_FOOD_WS IFS ON SDB.CUST_CD = IFS.CUSTOMER_CODE
  AND SDB.SUB_CUSTOMER_NAME = IFS.SUB_CUSTOMER_NAME
LEFT JOIN (
  SELECT DISTINCT CUST_NUM AS CUST_CD,
    CUST_NM
  FROM EDW_CUSTOMER_BASE_DIM
  ) RCT ON SDB.CUST_CD = LTRIM(RCT.CUST_CD, 0)
LEFT JOIN (
  SELECT DISTINCT PP.CNTRY_CD,
    PP.BARCODE AS EANNUMBER,
    PA.SAP_MATL_NUM AS MATERIALNUMBER,
    PP.SKU_ENGLISH AS PRODUCTNAME
  FROM ITG_POP6_PRODUCTS PP
  LEFT JOIN EDW_PRODUCT_ATTR_DIM PA ON PP.BARCODE = PA.EAN
    AND UPPER(PP.CNTRY_CD) = UPPER(PA.CNTRY)
  WHERE UPPER(PP.CNTRY_CD) = 'KR'
    AND UPPER(PP.ACTIVE) = 'Y'
  ) RPT ON SDB.EAN = RPT.EANNUMBER
),
il_dong_hu as (
SELECT TO_DATE(SIDH.IMS_TXN_DT, 'YYYY-MM-DD') AS IMS_TXN_DT,
  'NA' AS DSTR_CD,
  SIDH.DSTR_NM,
  SIDH.CUST_CD,
  RCT.CUST_NM,
  RPT.MATERIALNUMBER AS PROD_CD,
  RPT.PRODUCTNAME AS PROD_NM,
  SIDH.EAN AS EAN_NUM,
  CAST(REPLACE(SIDH.UNIT_PRICE, ',', '') AS NUMERIC(21, 5)) AS UNIT_PRC,
  (REPLACE(SIDH.UNIT_PRICE, ',', '') * CAST(REPLACE(REPLACE(SIDH.QTY, '(', ''), ')', '') AS NUMERIC(21, 5))) AS SLS_AMT,
  CAST(REPLACE(REPLACE(SIDH.QTY, '(', ''), ')', '') AS INTEGER) SLS_QTY,
  IFS.SUB_CUSTOMER_CODE,
  SIDH.SUB_CUSTOMER_NAME,
  'KR' AS CTRY_CD,
  'KRW' AS CRNCY_CD,
  current_timestamp() AS CRT_DTTM,
  current_timestamp() AS UPDT_DTTM,
  null as SALES_PRIORITY,
       null as sales_stores,
       null as sales_rate
FROM SDL_KR_IL_DONG_HU_DI_S_DEOK_SEONG_SANG_SA_GT_SELLOUT SIDH
LEFT JOIN ITG_KR_GT_FOOD_WS IFS ON SIDH.CUST_CD = IFS.CUSTOMER_CODE
  AND SIDH.SUB_CUSTOMER_NAME = IFS.SUB_CUSTOMER_NAME
LEFT JOIN (
  SELECT DISTINCT CUST_NUM AS CUST_CD,
    CUST_NM
  FROM EDW_CUSTOMER_BASE_DIM
  ) RCT ON SIDH.CUST_CD = LTRIM(RCT.CUST_CD, 0)
LEFT JOIN (
  SELECT DISTINCT PP.CNTRY_CD,
    PP.BARCODE AS EANNUMBER,
    PA.SAP_MATL_NUM AS MATERIALNUMBER,
    PP.SKU_ENGLISH AS PRODUCTNAME
  FROM ITG_POP6_PRODUCTS PP
  LEFT JOIN EDW_PRODUCT_ATTR_DIM PA ON PP.BARCODE = PA.EAN
    AND UPPER(PP.CNTRY_CD) = UPPER(PA.CNTRY)
  WHERE UPPER(PP.CNTRY_CD) = 'KR'
    AND UPPER(PP.ACTIVE) = 'Y'
  ) RPT ON SIDH.EAN = RPT.EANNUMBER
  ),
jungseok as (
SELECT TRY_TO_DATE(SJG.IMS_TXN_DT||'15','YYYYMMDD') AS IMS_TXN_DT,
       'NA' AS DSTR_CD,
       UPPER(DSTR_NM) AS DSTR_NM,
       SJG.CUST_CD,
       RCT.CUST_NM,
       RPT.MATERIALNUMBER AS PROD_CD,
       RPT.PRODUCTNAME AS PROD_NM,
       SJG.EAN AS EAN_NUM,
       --(CAST(SJG.AMOUNT AS NUMERIC(21,5)) / CAST(SJG.QTY AS NUMERIC(21,5))) AS UNIT_PRC,
	   CASE
        WHEN CAST(SJG.QTY AS NUMERIC(21,5)) = 0 THEN 0
        ELSE (CAST(SJG.AMOUNT AS NUMERIC(21,5)) / CAST(SJG.QTY AS NUMERIC(21,5))) 
        END AS UNIT_PRC,
       CAST(SJG.AMOUNT AS NUMERIC(21,5)) AS SLS_AMT,
       CAST(SJG.QTY AS INTEGER) SLS_QTY,
       IFS.SUB_CUSTOMER_CODE,
       SJG.SUB_CUSTOMER_NAME,
       'KR' AS CTRY_CD,
       'KRW' AS CRNCY_CD,
       current_timestamp() AS CRT_DTTM,
       current_timestamp() AS UPDT_DTTM,
       null as SALES_PRIORITY,
       null as sales_stores,
       null as sales_rate
FROM SDL_KR_JUNGSEOK_GT_SELLOUT SJG
  LEFT JOIN ITG_KR_GT_FOOD_WS IFS
         ON SJG.CUST_CD = IFS.CUSTOMER_CODE
        AND SJG.SUB_CUSTOMER_NAME = IFS.SUB_CUSTOMER_NAME
  LEFT JOIN (SELECT DISTINCT CUST_NUM AS CUST_CD,
                    CUST_NM
             FROM EDW_CUSTOMER_BASE_DIM) RCT ON SJG.CUST_CD = LTRIM (RCT.CUST_CD,0)
  LEFT JOIN (SELECT DISTINCT PP.CNTRY_CD,
                    PP.BARCODE AS EANNUMBER,
                    PA.SAP_MATL_NUM AS MATERIALNUMBER,
                    PP.SKU_ENGLISH AS PRODUCTNAME
             FROM ITG_POP6_PRODUCTS PP
               LEFT JOIN EDW_PRODUCT_ATTR_DIM PA
                      ON PP.BARCODE = PA.EAN
                     AND UPPER (PP.CNTRY_CD) = UPPER (PA.CNTRY)
             WHERE UPPER(PP.CNTRY_CD) = 'KR'
             AND   UPPER(PP.ACTIVE) = 'Y') RPT ON SJG.EAN = RPT.EANNUMBER
) ,
nu_ri_zion as 
(SELECT TO_DATE(SNR.IMS_TXN_DT, 'YYYYMMDD') AS IMS_TXN_DT,
  'NA' AS DSTR_CD,
  DSTR_NM,
  SNR.CUST_CD,
  RCT.CUST_NM,
  RPT.MATERIALNUMBER AS PROD_CD,
  RPT.PRODUCTNAME AS PROD_NM,
  SNR.EAN AS EAN_NUM,
  CAST(SNR.UNIT_PRICE AS NUMERIC(21, 5)) AS UNIT_PRC,
  (SNR.UNIT_PRICE * CAST(SNR.QTY AS NUMERIC(21, 5))) AS SLS_AMT,
  CAST(SNR.QTY AS INTEGER) SLS_QTY,
  IFS.SUB_CUSTOMER_CODE,
  SNR.SUB_CUSTOMER_NAME,
  'KR' AS CTRY_CD,
  'KRW' AS CRNCY_CD,
  current_timestamp() AS CRT_DTTM,
  current_timestamp() AS UPDT_DTTM,
  null as SALES_PRIORITY,
       null as sales_stores,
       null as sales_rate
FROM SDL_KR_NU_RI_ZON_GT_SELLOUT SNR
LEFT JOIN ITG_KR_GT_FOOD_WS IFS ON SNR.CUST_CD = IFS.CUSTOMER_CODE
  AND SNR.SUB_CUSTOMER_NAME = IFS.SUB_CUSTOMER_NAME
LEFT JOIN (
  SELECT DISTINCT CUST_NUM AS CUST_CD,
    CUST_NM
  FROM EDW_CUSTOMER_BASE_DIM
  ) RCT ON SNR.CUST_CD = LTRIM(RCT.CUST_CD, 0)
LEFT JOIN (
  SELECT DISTINCT PP.CNTRY_CD,
    PP.BARCODE AS EANNUMBER,
    PA.SAP_MATL_NUM AS MATERIALNUMBER,
    PP.SKU_ENGLISH AS PRODUCTNAME
  FROM ITG_POP6_PRODUCTS PP
  LEFT JOIN EDW_PRODUCT_ATTR_DIM PA ON PP.BARCODE = PA.EAN
    AND UPPER(PP.CNTRY_CD) = UPPER(PA.CNTRY)
  WHERE UPPER(PP.CNTRY_CD) = 'KR'
    AND UPPER(PP.ACTIVE) = 'Y'
  ) RPT ON SNR.EAN = RPT.EANNUMBER
  ),
  yang_ju as (
SELECT TO_DATE(SLL.IMS_TXN_DT||'15','YYYYMMDD')  AS IMS_TXN_DT,
       'NA' AS DSTR_CD,
       UPPER(SLL.DSTR_NM) AS DSTR_NM,
       SLL.CUST_CD,
       RCT.CUST_NM,
       RPT.MATERIALNUMBER AS PROD_CD,
       RPT.PRODUCTNAME AS PROD_NM,
       SLL.EAN AS EAN_NUM,
       --CAST(SLL.SLS_AMT AS NUMERIC(21,5)) / CAST(SLL.SLS_QTY AS NUMERIC(21,5)) AS UNIT_PRC,
	   CASE
        WHEN CAST(SLL.SLS_QTY AS NUMERIC(21,5))=0 then 0
        ELSE CAST(SLL.SLS_AMT AS NUMERIC(21,5)) / CAST(SLL.SLS_QTY AS NUMERIC(21,5))
       END AS UNIT_PRC,
       CAST(SLL.SLS_AMT AS NUMERIC(21,5)) AS SLS_AMT,
       CAST(SLL.SLS_QTY AS INTEGER) SLS_QTY,
       'NA' AS SUB_CUSTOMER_CODE,
       'NA' AS SUB_CUSTOMER_NAME,
       'KR' AS CTRY_CD,
       'KRW' AS CRNCY_CD,
       current_timestamp() AS CRT_DTTM,
       current_timestamp() AS UPDT_DTTM,
       SLL.SALES_PRIORITY,
       CAST(SLL.SALES_STORES AS NUMERIC(21,5)) AS SALES_STORES,
       CAST(SLL.SALES_RATE AS NUMERIC(21,5)) AS SALES_RATE
FROM SDL_KR_LOTTE_LOGISTICS_YANG_JU_GT_SELLOUT SLL
  LEFT JOIN (SELECT DISTINCT CUST_NUM AS CUST_CD,
                    CUST_NM
             FROM EDW_CUSTOMER_BASE_DIM) RCT ON SLL.CUST_CD = LTRIM (RCT.CUST_CD,0)
  LEFT JOIN (SELECT DISTINCT PP.CNTRY_CD,PP.BARCODE AS EANNUMBER,
       PA.SAP_MATL_NUM AS MATERIALNUMBER,
       PP.SKU_ENGLISH AS PRODUCTNAME
FROM ITG_POP6_PRODUCTS PP
  LEFT JOIN EDW_PRODUCT_ATTR_DIM PA
         ON PP.BARCODE = PA.EAN
        AND UPPER (PP.CNTRY_CD) = UPPER (PA.CNTRY)
        WHERE UPPER (PP.CNTRY_CD)='KR' AND UPPER(PP.ACTIVE)='Y') RPT ON SLL.EAN = RPT.EANNUMBER
),
nacf as (
SELECT TO_DATE(SNG.IMS_TXN_DT, 'YYYY-MM-DD') AS IMS_TXN_DT,
  'NA' AS DSTR_CD,
  SNG.DSTR_NM,
  NCD.SAP_CUSTOMER_CODE AS CUST_CD,
  RCT.CUST_NM,
  RPT.MATERIALNUMBER AS PROD_CD,
  RPT.PRODUCTNAME AS PROD_NM,
  SNG.EAN AS EAN_NUM,
  SNG.UNIT_PRICE AS UNIT_PRC,
  (SNG.UNIT_PRICE * CAST(SNG.SALES_QTY AS NUMERIC(21, 5))) AS SLS_AMT,
  CAST(SNG.SALES_QTY AS INTEGER) SLS_QTY,
  SNG.SUB_CUSTOMER_CODE AS SUB_CUSTOMER_CODE,
  SNG.SUB_CUSTOMER_NAME AS SUB_CUSTOMER_NAME,
  'KR' AS CTRY_CD,
  'KRW' AS CRNCY_CD,
  current_timestamp() AS CRT_DTTM,
  current_timestamp() AS UPDT_DTTM,
  null as SALES_PRIORITY,
       null as sales_stores,
       null as sales_rate
FROM SDL_KR_NACF_GT_SELLOUT SNG
LEFT JOIN ITG_KR_GT_NACF_CUST_DIM NCD ON SNG.CUSTOMER_CODE = NCD.NACF_CUSTOMER_CODE
LEFT JOIN (
  SELECT DISTINCT CUST_NUM AS CUST_CD,
    CUST_NM
  FROM EDW_CUSTOMER_BASE_DIM
  ) RCT ON NCD.SAP_CUSTOMER_CODE = LTRIM(RCT.CUST_CD, 0)
LEFT JOIN (
  SELECT DISTINCT PP.CNTRY_CD,
    PP.BARCODE AS EANNUMBER,
    PA.SAP_MATL_NUM AS MATERIALNUMBER,
    PP.SKU_ENGLISH AS PRODUCTNAME
  FROM ITG_POP6_PRODUCTS PP
  LEFT JOIN EDW_PRODUCT_ATTR_DIM PA ON PP.BARCODE = PA.EAN
    AND UPPER(PP.CNTRY_CD) = UPPER(PA.CNTRY)
  WHERE UPPER(PP.CNTRY_CD) = 'KR'
    AND UPPER(PP.ACTIVE) = 'Y'
  ) RPT ON SNG.EAN = RPT.EANNUMBER
),
nh as (
SELECT CASE WHEN (SNG.IMS_TXN_DT IS NULL OR SNG.IMS_TXN_DT='') THEN CAL.CAL_DAY ELSE TO_DATE(replace(SNG.IMS_TXN_DT,'/','-'),'MM-DD-YY') 
END AS IMS_TXN_DT,
       SNG.DSTR_NM AS DSTR_CD,
       RCT.CUST_NM AS DSTR_NM,
       LTRIM(RCT.CUST_CD,0) AS CUST_CD,
       RCT.CUST_NM,
       RPT.MATERIALNUMBER AS PROD_CD,
       RPT.PRODUCTNAME AS PROD_NM,
       SNG.EAN AS EAN_NUM,
       CAST(SNG.AMOUNT AS NUMERIC(21,5)) / nullif(CAST(SNG.QUANTITY AS NUMERIC(21,5)),0) AS UNIT_PRC,
       SNG.AMOUNT AS SLS_AMT,
       CAST(SNG.QUANTITY AS INTEGER) SLS_QTY,
       'NA' AS SUB_CUSTOMER_CODE,
       SNG.ACCOUNT_NAME AS SUB_CUSTOMER_NAME,
       'KR' AS CTRY_CD,
       'KRW' AS CRNCY_CD,
       current_timestamp() AS CRT_DTTM,
       current_timestamp() AS UPDT_DTTM,
       null as SALES_PRIORITY,
       null as sales_stores,
       null as sales_rate
FROM SDL_KR_NH_GT_SELLOUT SNG
   LEFT JOIN (SELECT FISC_PER,
                    MAX(CAL_DAY) CAL_DAY
             FROM EDW_CALENDAR_DIM
             WHERE WKDAY = '7'
             GROUP BY FISC_PER) CAL ON SPLIT_PART(SPLIT_PART(SNG.FILE_NAME,'_',2),'.',1) = SUBSTRING (CAL.FISC_PER,1,4) ||SUBSTRING (CAL.FISC_PER,6,7)
  LEFT JOIN (SELECT DISTINCT CUST_NUM AS CUST_CD,
                    CUST_NM
             FROM EDW_CUSTOMER_BASE_DIM) RCT ON SNG.CUSTOMER_CODE = LTRIM (RCT.CUST_CD,0)
  LEFT JOIN (SELECT DISTINCT PP.CNTRY_CD,
                    PP.BARCODE AS EANNUMBER,
                    PA.SAP_MATL_NUM AS MATERIALNUMBER,
                    PP.SKU_ENGLISH AS PRODUCTNAME
             FROM ITG_POP6_PRODUCTS PP
               LEFT JOIN EDW_PRODUCT_ATTR_DIM PA
                      ON PP.BARCODE = PA.EAN
                     AND UPPER (PP.CNTRY_CD) = UPPER (PA.CNTRY)
             WHERE UPPER(PP.CNTRY_CD) = 'KR'
             AND   UPPER(PP.ACTIVE) = 'Y') RPT ON SNG.EAN = RPT.EANNUMBER
),
otc as (
SELECT CAST ((CASE WHEN (SNG.IMS_TXN_DT IS NULL OR SNG.IMS_TXN_DT='') THEN CAL.CAL_DAY ELSE TO_DATE(replace(SNG.IMS_TXN_DT,'/','-'),'MM-DD-YY') 
END) AS date) as  IMS_TXN_DT ,
       SNG.DSTR_NM AS DSTR_CD,
       RCT.CUST_NM AS DSTR_NM,
	   LTRIM(RCT.CUST_CD,0) AS CUST_CD,
       RCT.CUST_NM,
       RPT.MATERIALNUMBER AS PROD_CD,
       RPT.PRODUCTNAME AS PROD_NM,
       SNG.EAN AS EAN_NUM,
       CAST(SNG.AMOUNT AS NUMERIC(21,5)) / nullif(CAST(SNG.QUANTITY AS NUMERIC(21,5)),0) AS UNIT_PRC,
       CAST(SNG.AMOUNT AS NUMERIC(21,5)) AS SLS_AMT,
       CAST(SNG.QUANTITY AS INTEGER) SLS_QTY,
       V2.OUTLET_CODE AS SUB_CUSTOMER_CODE,
       V2.NAME AS SUB_CUSTOMER_NAME,
       'KR' AS CTRY_CD,
       'KRW' AS CRNCY_CD,
       current_timestamp() AS CRT_DTTM,
       current_timestamp() AS UPDT_DTTM,
        null as SALES_PRIORITY,
       null as sales_stores,
       null as sales_rate
FROM SDL_KR_OTC_SELLOUT SNG
   LEFT JOIN (SELECT FISC_PER,
                    MAX(CAL_DAY) CAL_DAY
             FROM EDW_CALENDAR_DIM
             WHERE WKDAY = '7'
             GROUP BY FISC_PER) CAL ON SPLIT_PART(SPLIT_PART(SNG.FILE_NAME,'_',3),'.',1) = SUBSTRING (CAL.FISC_PER,1,4) ||SUBSTRING (CAL.FISC_PER,6,7)
  LEFT JOIN (SELECT DISTINCT CUST_NUM AS CUST_CD,
                    CUST_NM
             FROM EDW_CUSTOMER_BASE_DIM) RCT ON SNG.CUSTOMER_CODE = LTRIM (RCT.CUST_CD,0)
  LEFT JOIN (SELECT DISTINCT PP.CNTRY_CD,
                    PP.BARCODE AS EANNUMBER,
                    PA.SAP_MATL_NUM AS MATERIALNUMBER,
                    PP.SKU_ENGLISH AS PRODUCTNAME
             FROM ITG_POP6_PRODUCTS PP
               LEFT JOIN EDW_PRODUCT_ATTR_DIM PA
                      ON PP.BARCODE = PA.EAN
                     AND UPPER (PP.CNTRY_CD) = UPPER (PA.CNTRY)
             WHERE UPPER(PP.CNTRY_CD) = 'KR'
             AND   UPPER(PP.ACTIVE) = 'Y') RPT ON SNG.EAN = RPT.EANNUMBER
	LEFT JOIN (SELECT DISTINCT OUTLET_CODE,NAME,PHARMACY_NAME,SAP_CUSTOMER_CODE FROM ITG_MDS_KR_SUB_CUSTOMER_MASTER) V2
	ON V2.PHARMACY_NAME = SNG.ACCOUNT_NAME and V2.SAP_CUSTOMER_CODE = SNG.CUSTOMER_CODE and V2.outlet_code = SNG.pcode
WHERE SNG.QUANTITY not like '%.%'
),
final as (

select * from hyundai
union all
select * from lotte
union all
select * from sang_sa
union all
select * from jong_hap
union all
select * from dongbu_lsd
union all
select * from du_bae_ro_yu_tong
union all
select * from il_dong_hu
union all
select * from nu_ri_zion
union all
select * from yang_ju
union all
select * from jungseok
union all
select * from nacf
union all
select * from nh
union all
select * from otc
)
select * from final