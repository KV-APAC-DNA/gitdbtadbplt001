--import cte   
with edw_calendar_dim as (
    select * from {{ source('aspedw_integration', 'edw_calendar_dim') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ source('aspedw_integration', 'edw_vw_cal_retail_excellence_dim') }}
),
itg_re_msl_input_definition as (
    select * from {{ source('aspitg_integration', 'itg_re_msl_input_definition') }}
),
wks_my_base_re as (
    select * from {{ ref('myswks_integration__wks_my_base_re') }}
),
itg_my_material_dim as (
    select * from {{ ref('mysitg_integration__itg_my_material_dim') }}
),
wks_my_regional_sellout_mapped_sku_cd as (
    select * from {{ ref('myswks_integration__wks_my_regional_sellout_mapped_sku_cd') }}
),

MY_RE_MSL_LIST	  as (
SELECT distinct left(jj_mnth_id,4) as JJ_YEAR,MSL.JJ_MNTH_ID,CUSTHIER.SOLDTO_CODE,CUSTHIER.DISTRIBUTOR_CODE,		
custhier.distributor_name,custhier.store_code,custhier.store_name,custhier.store_type,
ltrim(msl.sku_unique_identifier,0) as ean, MSL.STORE_GRADE,upper(msl.retail_environment) as retail_environment ,upper(msl.channel) as channel ,upper(custhier.channel_name) as Sell_Out_Channel,msl.market,prodhier.prod_hier_l1,prodhier.prod_hier_l2,prodhier.prod_hier_l3,prodhier.prod_hier_l4,nvl(custhier.sku_code,pd.sku_code) as sku_code,nvl(custhier.msl_product_desc,pd.msl_product_desc) as product_name		
 FROM
(SELECT DISTINCT
      jj_mnth_id,
     market,
     retail_environment,
	   Channel,
	   sub_channel,
     Customer,
     store_grade,
	   sku_unique_identifier
FROM ITG_RE_MSL_INPUT_DEFINITION A		
LEFT JOIN (SELECT DISTINCT FISC_YR,
                  SUBSTRING(FISC_PER,1,4)||SUBSTRING(FISC_PER,6,7) AS JJ_MNTH_ID
             FROM EDW_CALENDAR_DIM		
			 where jj_mnth_id >= (select last_19mnths from EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)		
						   and jj_mnth_id <= (select last_2mnths from EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)) CAL		
           ON TO_CHAR(TO_DATE (A.START_DATE,'DD/MM/YYYY'),'YYYYMM') <=CAL.JJ_MNTH_ID
           AND TO_CHAR(TO_DATE (A.END_DATE,'DD/MM/YYYY'),'YYYYMM') >= CAL.JJ_MNTH_ID		
 WHERE MARKET = 'Malaysia')MSL
left join (select ean,prod_hier_l1,prod_hier_l2,prod_hier_l3,prod_hier_l4
			from
				(	select ltrim(item_bar_cd,'0') as ean, 'Malaysia' as prod_hier_l1, null::varchar as prod_hier_l2,		
							frnchse_desc AS prod_hier_l3,
							brnd_desc AS prod_hier_l4,
							row_number() over (partition by ltrim(item_bar_cd,'0') order by frnchse_desc,brnd_desc desc) as rno
					from ITG_MY_MATERIAL_DIM) where rno = 1 )prodhier on ltrim(msl.sku_unique_identifier,'0') = ltrim(prodhier.ean,'0')		
left join wks_my_regional_sellout_mapped_sku_cd pd on ltrim(msl.sku_unique_identifier,'0') = ltrim(pd.ean,'0')		
left join (select distinct soldto_code,distributor_Code,distributor_name,retail_environment,store_type,channel_name,store_code,store_name,ean,sku_code,msl_product_desc
		  from wks_my_base_re WHERE CNTRY_CD = 'MY'		
			AND DATA_SRC IN ('POS','SELL-OUT')) CUSTHIER ON UPPER(MSL.CHANNEL) = UPPER(CUSTHIER.CHANNEL_NAME) and		
															case when MSL.RETAIL_ENVIRONMENT = 'GT' then upper(nvl(msl.store_grade,custhier.store_type))		
																 when MSL.RETAIL_ENVIRONMENT = 'MT' then upper(msl.store_grade) end = upper(custhier.store_type)		
															AND  MSL.SKU_UNIQUE_IDENTIFIER = CUSTHIER.EAN
)

--final select
select * from MY_RE_MSL_LIST