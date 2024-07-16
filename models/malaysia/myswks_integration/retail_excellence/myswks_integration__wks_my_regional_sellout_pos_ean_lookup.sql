--import cte
with wks_MY_REGIONAL_SELLOUT_POS_SKU_EAN_BASE as (
    select * from {{ ref('myswks_integration__wks_my_regional_sellout_pos_sku_ean_base') }}
),
edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
itg_re_msl_input_definition as (
    select * from {{ ref('aspitg_integration__itg_re_msl_input_definition') }}
),

MY_REGIONAL_SELLOUT_POS_EAN_LOOKUP  as (
select mnth_id,sku_code, ean
from
(
SELECT MNTH_ID,SKU_CODE,NVL(MSL.EAN,BASEEAN.EAN) as ean,		
row_number() over (partition by mnth_id,sku_code order by length(nvl(msl.ean,baseean.ean)) desc) as rn
from wks_MY_REGIONAL_SELLOUT_POS_SKU_EAN_BASE baseean		
left join
(SELECT DISTINCT
     	 jj_mnth_id,
	   ltrim(sku_unique_identifier,'0') as ean
FROM ITG_RE_MSL_INPUT_DEFINITION A		
LEFT JOIN (SELECT DISTINCT FISC_YR,
                  SUBSTRING(FISC_PER,1,4)||SUBSTRING(FISC_PER,6,7) AS JJ_MNTH_ID
             FROM EDW_CALENDAR_DIM		
			 where jj_mnth_id >= (select last_17mnths from EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)		
						   and jj_mnth_id <= (select last_2mnths from EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)) CAL		
           ON TO_CHAR(TO_DATE (A.START_DATE,'MM/DD/YYYY'),'YYYYMM') <=CAL.JJ_MNTH_ID		
           AND TO_CHAR(TO_DATE (A.END_DATE,'MM/DD/YYYY'),'YYYYMM') >= CAL.JJ_MNTH_ID		
 WHERE MARKET = 'MALAYSIA')MSL ON BASEEAN.MNTH_ID = MSL.JJ_MNTH_ID AND BASEEAN.EAN = MSL.EAN)where rn= 1
),

--final select
final as (
select 
mnth_id::varchar(23) as mnth_id,	
sku_code::varchar(40) as sku_code,	
ean::varchar(500) as ean
from MY_REGIONAL_SELLOUT_POS_EAN_LOOKUP
)
select * from final