--import cte

with itg_re_msl_input_definition as (
    select * from {{ ref('aspitg_integration__itg_re_msl_input_definition') }}
),

edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim')}}
),

--final cte
MSL as 
(
    SELECT DISTINCT CAL.FISC_YR AS YEAR,
       CAL.JJ_MNTH_ID,
       MSL_DEF.SUB_CHANNEL,
       MSL_DEF.SKU_UNIQUE_IDENTIFIER,
	   MSL_DEF.RETAIL_ENVIRONMENT
    FROM itg_re_msl_input_definition MSL_DEF
    LEFT JOIN (SELECT DISTINCT FISC_YR,
                    SUBSTRING(FISC_PER,1,4)||SUBSTRING(FISC_PER,6,7) AS JJ_MNTH_ID
             FROM edw_calendar_dim) CAL
         ON TO_CHAR (TO_DATE (MSL_DEF.START_DATE,'DD/MM/YYYY'),'YYYYMM') <= CAL.JJ_MNTH_ID
        AND TO_CHAR (TO_DATE (MSL_DEF.END_DATE,'DD/MM/YYYY'),'YYYYMM') >= CAL.JJ_MNTH_ID
    WHERE market = 'Philippines'
AND   active_status_code = 'Y'
)

select * from MSl