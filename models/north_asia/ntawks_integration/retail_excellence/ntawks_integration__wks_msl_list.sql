with itg_re_msl_input_definition as (
    select * from {{ source('aspitg_integration', 'itg_re_msl_input_definition') }}
),
edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
wrk_itg_re_msl_input_definition as 
(

    SELECT DISTINCT CAL.FISC_YR AS YEAR,		
       --CAL.CAL_MO_1 AS MNTH_ID,
       MARKET,
       CAL.JJ_MNTH_ID as JJ_MNTH_ID,		
       MSL_DEF.SUB_CHANNEL,		
       LTRIM(MSL_DEF.SKU_UNIQUE_IDENTIFIER,'0') AS sku_unique_identifier,		
       UPPER(MSL_DEF.RETAIL_ENVIRONMENT) AS RETAIL_ENVIRONMENT		
FROM itg_re_msl_input_definition MSL_DEF		
  LEFT JOIN (SELECT DISTINCT FISC_YR,
                    --CAL_MO_1,
                    SUBSTRING(FISC_PER,1,4) ||SUBSTRING(FISC_PER,6,7) AS JJ_MNTH_ID
             FROM EDW_CALENDAR_DIM) CAL		--//              
         ON TO_CHAR (TO_DATE (MSL_DEF.START_DATE,'DD/MM/YYYY'),'YYYYMM') <= CAL.JJ_MNTH_ID		
        AND TO_CHAR (TO_DATE (MSL_DEF.END_DATE,'DD/MM/YYYY'),'YYYYMM') >= CAL.JJ_MNTH_ID		
WHERE market = 'Korea'
)
 select * from wrk_itg_re_msl_input_definition
