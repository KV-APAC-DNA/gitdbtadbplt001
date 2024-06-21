--import cte
with edw_rpt_regional_sellout_offtake as (   
    select * from {{ source('aspedw_integration','edw_rpt_regional_sellout_offtake') }}
),
wks_my_regional_sellout_pos_ean_lookup as (
    select * from {{ ref('myswks_integration__wks_my_regional_sellout_pos_ean_lookup') }}
),

MY_BASE_RE_RAW  as (
SELECT country_code,
             country_name,
             data_source,
             DISTRIBUTOR_CODE,
             DISTRIBUTOR_NAME,
             SOLDTO_CODE,
             cal_date,
             ltrim(STORE_CODE,0) as store_code,
		    case when data_source = 'SELL-OUT' then msl_product_code
		         
			      when data_source = 'POS' then LTRIM(EANLKUP.EAN,0)	
		     END as EAN,
		     store_name,
		     list_price,
		     msl_product_desc,
		     BASE.SKU_CODE,		
 			 Region,zone_or_area,
 			 store_type,
			 store_grade,
             SAP_PARENT_CUSTOMER_KEY,
             SAP_PARENT_CUSTOMER_DESCRIPTION,
             SAP_CUSTOMER_CHANNEL_KEY,
             SAP_CUSTOMER_CHANNEL_DESCRIPTION,
             SAP_CUSTOMER_SUB_CHANNEL_KEY,
             SAP_SUB_CHANNEL_DESCRIPTION,
             SAP_GO_TO_MDL_KEY,
             SAP_GO_TO_MDL_DESCRIPTION,
             SAP_BANNER_KEY,
             SAP_BANNER_DESCRIPTION,
             SAP_BANNER_FORMAT_KEY,
             SAP_BANNER_FORMAT_DESCRIPTION,
             CUSTOMER_SEGMENT_KEY,
             CUSTOMER_SEGMENT_DESCRIPTION,
             GLOBAL_PRODUCT_FRANCHISE,
             GLOBAL_PRODUCT_BRAND,
             GLOBAL_PRODUCT_SUB_BRAND,
             GLOBAL_PRODUCT_VARIANT,
             GLOBAL_PRODUCT_SEGMENT,
             GLOBAL_PRODUCT_SUBSEGMENT,
             GLOBAL_PRODUCT_CATEGORY,
             GLOBAL_PRODUCT_SUBCATEGORY,
             MAX(GLOBAL_PUT_UP_DESCRIPTION) over ( PARTITION BY nvl(base.EAN,eanlkup.ean) ORDER BY base.mnth_id DESC  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS GLOBAL_PUT_UP_DESCRIPTION ,
             PKA_PRODUCT_KEY,
             PKA_PRODUCT_KEY_DESCRIPTION,
             YEAR,
             BASE.MNTH_ID,		
             sellout_value_list_price,
             SELLOUT_SALES_QUANTITY,
             SELLOUT_SALES_VALUE
      from  edw_rpt_regional_sellout_offtake base		
      LEFT JOIN (SELECT MNTH_ID,SKU_CODE,EAN FROM wks_my_regional_sellout_pos_ean_lookup) EANLKUP ON BASE.MNTH_ID = EANLKUP.MNTH_ID and		
																											BASE.SKU_CODE = EANLKUP.SKU_CODE		
	  WHERE COUNTRY_CODE = 'MY' and data_source in ('SELL-OUT','POS')
	  )
	  
	--final select
select * from MY_BASE_RE_RAW 