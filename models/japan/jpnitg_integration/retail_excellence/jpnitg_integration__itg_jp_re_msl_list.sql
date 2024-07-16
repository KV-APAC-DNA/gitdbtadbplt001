 with WKS_JAPAN_BASE_RETAIL_EXCELLENCE as (
    select * from {{ ref('jpnwks_integration__wks_japan_base_retail_excellence') }}
 ),
 edw_vw_cal_Retail_excellence_Dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
 ),
 EDW_CALENDAR_DIM as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
 ),
 ITG_RE_MSL_INPUT_DEFINITION as (
    select * from {{ source('aspitg_integration', 'itg_re_msl_input_definition') }}
 ),
 EDI_STORE_M as (
    select * from {{ source('jpnedw_integration', 'edi_store_m') }}
 ),
  EDI_CHN_M as (
    select * from {{ source('jpnedw_integration', 'edi_chn_m') }}
 ),
  MT_SGMT as (
    select * from {{ source('jpnedw_integration', 'mt_sgmt') }}
 ),
  MT_PRF as (
    select * from {{ source('jpnedw_integration', 'mt_prf') }}
 ),
  JP_INV_COVERAGE_AREA_REGION_MAPPING as (
    select * from {{ source('jpnedw_integration', 'jp_inv_coverage_area_region_mapping') }}
 ),
  SDL_MDS_JP_C360_ENG_TRANSLATION as (
    select * from {{ source('jpnsdl_raw', 'sdl_mds_jp_c360_eng_translation') }}
 ),

transformation as (
    SELECT DISTINCT MSL.YEAR,
       MSL.JJ_MNTH_ID AS MNTH_ID,
       SELL_OUT_PARENT_CUSTOMER_L1,
       SELL_OUT_CHILD_CUSTOMER_L2,
       PARENT_NAME1,
       CHILD_NAME2,
       SEGMENT,
       SELL_OUT_RE,
       DISTRIBUTOR_CODE,
       STORE_DET.Distributor_Name||'#'||LTRIM(STORE_DET.Distributor_CODE,'0') AS Distributor_Name,
       soldto_code as SOLD_TO_CODE,
       STORE_CODE,
       STORE_DET.STORE_Name||'#'||ltrim(STORE_DET.STORE_CODE,'0') AS STORE_NAME,
       REGION_CODE,
       REGION_NAME,
       REGION_NAME_ENG,
       ZONE_NAME,
       ZONE_NAME_ENG,
       STORE_ADDRESS,
       POST_CODE,
       MSL.RETAIL_ENVIRONMENT,
       MSL.CHANNEL,
       MSL.SKU_UNIQUE_IDENTIFIER AS SKU_CD,
       SYSDATE() AS CRT_DTTM
FROM (SELECT DISTINCT CAL.FISC_YR AS YEAR,
             CAL.CAL_MO_1 AS CAL_MNTH_ID,
             SUBSTRING(CAL.FISC_PER,1,4) ||SUBSTRING(CAL.FISC_PER,6,7) AS JJ_MNTH_ID,
             A.RETAIL_ENVIRONMENT,
             A.CHANNEL,
             A.SKU_UNIQUE_IDENTIFIER
      FROM ITG_RE_MSL_INPUT_DEFINITION A
        LEFT JOIN EDW_CALENDAR_DIM CAL
               ON TO_CHAR (TO_DATE (A.START_DATE,'DD/MM/YYYY'),'YYYYMM') <= CAL.CAL_MO_1
              AND TO_CHAR (TO_DATE (A.END_DATE,'DD/MM/YYYY'),'YYYYMM') >= CAL.CAL_MO_1
      WHERE MARKET = 'Japan') MSL
  LEFT JOIN (SELECT DISTINCT --SO.MNTH_ID,
                    CHN.CHN_OFFC_CD AS SELL_OUT_PARENT_CUSTOMER_L1,
                    CHN.CHN_CD AS SELL_OUT_CHILD_CUSTOMER_L2,
                    CHN.CMMN_NM AS PARENT_NAME1,
                    CHN_HQ.CMMN_NM AS CHILD_NAME2,
                    CHN.SGMT AS SEGMENT,
                    ENG.NAME_ENG AS SELL_OUT_RE,
                    SO.DISTRIBUTOR_CODE,
                    SO.DISTRIBUTOR_NAME,
                    SO.SOLDTO_CODE,
                    SO.STORE_CODE,
                    SO.STORE_NAME,
                    SO.SKU_CODE as MSL_PRODUCT_CODE,
                    STORE.JIS_PRFCT_C AS REGION_CODE,
                    PRF.PRF_NM_KNJ AS REGION_NAME,
                    REG_JAPAN.REGION AS REGION_NAME_ENG,
                    REG_JAPAN.PREFECTURE_NAME_KANA AS ZONE_NAME,
                    REG_JAPAN.PREFECTURE_NAME_ENG AS ZONE_NAME_ENG,
                    STORE.ADRS_KNJ1 AS STORE_ADDRESS,
                    STORE.PST_CO AS POST_CODE
             FROM (SELECT DISTINCT --SO_COMBINE.MNTH_ID, 
                          SO_COMBINE.DISTRIBUTOR_CODE,
                         SO_COMBINE.distributor_name,
                          SO_COMBINE.SOLD_TO_CODE AS SOLDTO_CODE,
                          SO_COMBINE.STORE_CODE,
                          SO_COMBINE.store_name,
                          SO_COMBINE.SKU_CODE
                     FROM  
                         
       WKS_JAPAN_BASE_RETAIL_EXCELLENCE SO_COMBINE
       --WHERE 
        --MNTH_ID >= (select last_18mnths from rg_edw.edw_vw_cal_Retail_excellence_Dim)
                           
                   ) SO
               LEFT JOIN EDI_STORE_M STORE ON SO.STORE_CODE = STORE.STR_CD
               LEFT JOIN EDI_CHN_M CHN ON STORE.CHN_CD = CHN.CHN_CD
               LEFT JOIN EDI_CHN_M CHN_HQ ON CHN_HQ.CHN_CD = CHN.CHN_OFFC_CD
               LEFT JOIN MT_SGMT SGMT ON CHN.SGMT = SGMT.SGMT
               LEFT JOIN MT_PRF PRF ON PRF.PRF_CD = STORE.JIS_PRFCT_C
               LEFT JOIN JP_INV_COVERAGE_AREA_REGION_MAPPING REG_JAPAN ON PRF.PRF_CD = REG_JAPAN.JIS_CD
               LEFT JOIN SDL_MDS_JP_C360_ENG_TRANSLATION ENG ON SGMT.SGMT_NM_REP = ENG.NAME_JP) STORE_DET
         ON MSL.CHANNEL = STORE_DET.SEGMENT
       AND LTRIM(MSL.SKU_UNIQUE_IDENTIFIER,'0') =ltrim(STORE_DET.MSL_PRODUCT_CODE,'0')
        --AND MSL.JJ_MNTH_ID = STORE_DET.MNTH_ID
WHERE MSL.JJ_MNTH_ID >= (select last_16mnths from edw_vw_cal_Retail_excellence_Dim)
    and MSL.JJ_MNTH_ID <= (select prev_mnth from edw_vw_cal_Retail_excellence_Dim)
 --MSL.JJ_MNTH_ID > TO_CHAR(ADD_MONTHS(SYSDATE,-18),'YYYYMM')
--AND   MSL.JJ_MNTH_ID <= (SELECT TO_CHAR(SYSDATE,'YYYYMM'))
AND  DISTRIBUTOR_CODE is NOT NULL 
AND  STORE_CODE IS NOT NULL
),

final as (
    select 
    *
    from transformation
)

select * from final