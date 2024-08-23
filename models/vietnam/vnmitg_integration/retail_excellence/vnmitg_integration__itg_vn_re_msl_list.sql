--import cte

with itg_re_msl_input_definition as (
    select * from {{ ref('aspitg_integration__itg_re_msl_input_definition') }}
),
edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim')}}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
wks_vn_base_retail_excellence as (
    select * from {{ ref('vnmwks_integration__wks_vn_base_retail_excellence')}}
),
edw_product_key_attributes as (
    select * from {{ ref('aspedw_integration__edw_product_key_attributes')}}
),
itg_vn_product_mapping as (
    select * from {{ ref('vnmitg_integration__itg_vn_product_mapping')}}
),

MSL as 
(
    SELECT DISTINCT 
       CAL.FISC_YR AS YEAR,
       MARKET,
       CAL.JJ_MNTH_ID,
       MSL_DEF.RETAIL_ENVIRONMENT,
       MSL_DEF.CHANNEL,
       LTRIM(MSL_DEF.SKU_UNIQUE_IDENTIFIER,'0') AS sku_unique_identifier
    FROM ITG_RE_MSL_INPUT_DEFINITION MSL_DEF
    LEFT JOIN (SELECT DISTINCT FISC_YR,
                    SUBSTRING(FISC_PER,1,4)||SUBSTRING(FISC_PER,6,7) AS JJ_MNTH_ID
             FROM edw_calendar_dim) CAL
         ON TO_CHAR (TO_DATE (MSL_DEF.START_DATE,'DD/MM/YYYY'),'YYYYMM') <= CAL.JJ_MNTH_ID
        AND TO_CHAR (TO_DATE (MSL_DEF.END_DATE,'DD/MM/YYYY'),'YYYYMM') >= CAL.JJ_MNTH_ID
    WHERE UPPER(market) = 'VIETNAM'
    --AND   active_status_code = 'Y'
),

--final cte

itg_vn_re_msl_list
as 
(
    SELECT DISTINCT MSL.YEAR AS fisc_yr,
       msl.jj_mnth_id as fisc_per,
	   offtake.data_src,
       offtake.soldto_code,
       offtake.distributor_code,
       offtake.distributor_name,
       offtake.store_code,
       offtake.store_name,
       offtake.store_type,
       LTRIM(msl.sku_unique_identifier,0) AS EAN,
       UPPER(msl.retail_environment) AS retail_environment,
       UPPER(msl.channel) AS channel,
       UPPER(offtake.channel) AS Sell_Out_Channel,
	   offtake.sku_code as sku_code,
	   offtake.sku_description as sku_description,
       msl.market,
       prodhier.prod_hier_l1,
       prodhier.prod_hier_l2,
       prodhier.prod_hier_l3,
       prodhier.prod_hier_l4,
       SYSDATE() AS CRTD_DTTM
       FROM MSL
  LEFT JOIN (SELECT matl_num,
                    ean_code,
                    prod_hier_l1,
                    prod_hier_l2,
                    prod_hier_l3,
                    prod_hier_l4
             FROM (SELECT LTRIM(matl_num,'0') AS matl_num,
                          ean_upc AS ean_code,
                          gcph_franchise AS prod_hier_l1,
                          gcph_needstate AS prod_hier_l2,
                          gcph_category AS prod_hier_l3,
                          gcph_subcategory AS prod_hier_l4,
                          ROW_NUMBER() OVER (PARTITION BY ean_upc ORDER BY lst_nts DESC) AS rn
                   FROM edw_product_key_attributes
                   WHERE ctry_nm = 'Vietnam'
                   AND   matl_type_cd = 'FERT'
                   AND   (lst_nts IS NOT NULL)) a
               JOIN itg_vn_product_mapping c ON a.ean_code = c.barcode
             WHERE rn = 1) prodhier ON LTRIM (msl.sku_unique_identifier,'0') = LTRIM (prodhier.matl_num)
  LEFT JOIN (SELECT Data_Src,
                    CNTRY_CD,
                    CNTRY_NM,
                    soldto_code,
                    Distributor_code,
                    Region,
                    ZONE,
                    Distributor_Name,
                    Channel,
                    store_code,
                    store_name,
                    store_type,
                    ean,
					sku_description,
					sku_code,
                    RETAIL_ENVIRONMENT AS retail_env
             FROM WKS_VN_BASE_RETAIL_EXCELLENCE) offtake
         ON UPPER (offtake.retail_env) = UPPER (msl.Retail_Environment)
        AND LTRIM (offtake.EAN, '0') = LTRIM (msl.sku_unique_identifier, '0')
WHERE MSL.JJ_MNTH_ID >= (SELECT last_17mnths
                         FROM edw_vw_cal_Retail_excellence_Dim)
AND   MSL.JJ_MNTH_ID <= (SELECT last_2mnths FROM edw_vw_cal_Retail_excellence_Dim)
AND   STORE_CODE IS NOT NULL
AND   DISTRIBUTOR_CODE IS NOT NULL
),

final as 
(
    select fisc_yr :: numeric(18,0) as fisc_yr,
    fisc_per :: numeric(18,0) as fisc_per,
    data_src :: varchar(20) as data_src,
    soldto_code :: varchar(255) as soldto_code,
    distributor_code :: varchar(150) as distributor_code,
    distributor_name :: varchar(356) as distributor_name,
    store_code :: varchar(100) as store_code,
    store_name :: varchar(601) as store_name,
    store_type :: varchar(382) as store_type,
    ean :: varchar(200) as ean,
    retail_environment :: varchar(200) as retail_environment,
    channel :: varchar(200) as channel,
    sell_out_channel :: varchar(225) as sell_out_channel,
    sku_code :: varchar(150) as sku_code,
    sku_description :: varchar(300) as sku_description,
    market :: varchar(50) as market,
    prod_hier_l1 :: varchar(255) as prod_hier_l1,
    prod_hier_l2 :: varchar(255) as prod_hier_l2,
    prod_hier_l3 :: varchar(255) as prod_hier_l3,
    prod_hier_l4 :: varchar(255) as prod_hier_l4,
    crtd_dttm :: timestamp without time zone as crtd_dttm
    from itg_vn_re_msl_list
)

--final select

select * from final