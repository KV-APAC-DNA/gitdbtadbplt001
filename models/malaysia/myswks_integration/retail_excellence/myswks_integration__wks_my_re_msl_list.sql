--import cte   
with edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
itg_re_msl_input_definition as (
    select * from {{ ref('aspitg_integration__itg_re_msl_input_definition') }}
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
			 where jj_mnth_id >= (select last_17mnths from EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)		
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
),

--final select
final as (
select 
jj_year::varchar(22) as jj_year ,
jj_mnth_id::varchar(22) as jj_mnth_id ,
soldto_code::varchar(255) as soldto_code ,
distributor_code::varchar(100) as distributor_code ,
distributor_name::varchar(356) as distributor_name ,
store_code::varchar(100) as store_code ,
store_name::varchar(601) as store_name ,
store_type::varchar(255) as store_type ,
ean::varchar(500) as ean ,
store_grade::varchar(20) as store_grade ,
retail_environment::varchar(75) as retail_environment ,
channel::varchar(75) as channel ,
sell_out_channel::varchar(573) as sell_out_channel ,
market::varchar(50) as market ,
prod_hier_l1::varchar(8) as prod_hier_l1 ,
prod_hier_l2::varchar(1) as prod_hier_l2 ,
prod_hier_l3::varchar(100) as prod_hier_l3 ,
prod_hier_l4::varchar(100) as prod_hier_l4 ,
sku_code::varchar(40) as sku_code ,
product_name::varchar(300) as product_name 
 from MY_RE_MSL_LIST
)
select * from final