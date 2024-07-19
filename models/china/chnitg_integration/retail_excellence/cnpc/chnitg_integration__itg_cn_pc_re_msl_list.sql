--import cte
with itg_re_msl_input_definition as (
 --select * from {{ ref('aspitg_integration__itg_re_msl_input_definition') }}
 select * from {{ source('aspitg_integration', 'itg_re_msl_input_definition') }}
),
edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim')}}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
itg_mds_master_mother_code_mapping as (
    select * from {{ source('aspitg_integration', 'itg_mds_master_mother_code_mapping') }}
),
cnpc_regional_sellout_mapped_sku_cd as (
    select * from {{ ref('chnwks_integration__wks_china_personal_care_regional_sellout_mapped_sku_cd') }}
),
cnpc_regional_sellout_ean as (
    select * from {{ ref('chnwks_integration__wks_china_personal_care_regional_sellout_ean') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ source('aspedw_integration', 'edw_vw_cal_retail_excellence_dim') }}
),
wks_china_personal_care_base_retail_excellence as (
    select * from {{ ref('chnwks_integration__wks_china_personal_care_base_retail_excellence') }}
),

cn_pc_re_msl_list
as
(
   SELECT distinct final.fisc_yr as fisc_yr,
       final.JJ_MNTH_ID AS fisc_per,
       final.market AS Market,
       nvl(final.store_type,final.retail_environment) AS Retail_Environment,
	   final.soldto_code as soldto_code,
       final.Distributor_Code AS Distributor_Code,
       final.distributor_name as distributor_name,
       final.store_code as store_code,
       final.store_name as store_name,
       final.store_type as store_type,
       --final.pka_product_key_description as sku_description,
	   --final.ean_code as product_code,
	   --replace by ean and use mother code as product_code
	   final.ean as ean,
	   nvl(final.mother_code,'NA') as product_code,
	   final.msl_product_desc as product_name,
	   final.mapped_sku_code as mapped_sku_code,
       final.region as region,
       final.zone,
       final.city,
       --final.province,
       final.data_src,
   --    final.pka_product_key_description as product_code,
       'China Personal Care' as prod_hier_l1,
       sysdate() as created_date
FROM (SELECT distinct base.start_date,
             base.end_date,
             base.market,
             base.retail_environment,
             base.jj_mnth_id,
             base.fisc_yr,
--             base.product_key,
       --base.ean_code,
	         noo.ean,
			 base.mother_code,
             noo.Data_Src,
             noo.cntry_cd,
             noo.cntry_nm,
			 noo.soldto_code,
             noo.distributor_code,
             noo.distributor_name,
             noo.store_code,
             noo.store_name,
             noo.store_type,
             noo.region,
             noo.zone,
             --noo.province,
             noo.city,
			 nvl(noo.msl_product_desc,pd.msl_product_desc) as msl_product_desc,
			 nvl(noo.mapped_sku_code,pd.sku_code) as mapped_sku_code
			 --,
--             noo.pka_product_key_description
       FROM (SELECT DISTINCT start_date,
                      END_DATE,
                      msl.market,
                      retail_environment,
                      --product_key,
					   ltrim(ean_code,'0') as ean_Code,
					   -----Include mother code as per new changes - June'2024
					   mother_code,
                      jj_mnth_id,
                      fisc_yr
               FROM (SELECT DISTINCT 
                            TO_CHAR (TO_DATE ( start_date,'DD/MM/YYYY'),'YYYYMM')::NUMERIC as start_date,
                            TO_CHAR (TO_DATE ( END_DATE,'DD/MM/YYYY'),'YYYYMM')::NUMERIC as END_DATE,
                            market,
                            upper(retail_environment) as retail_environment,
							ltrim(sku_unique_identifier,'0') as ean_code
                            --upper(sku_unique_identifier) as product_key 
                     FROM itg_re_msl_input_definition
                     WHERE market = 'China Personal Care') msl
                    LEFT JOIN 
                       (SELECT DISTINCT fisc_yr, cal_mo_1,
                                        (SUBSTRING(FISC_PER,1,4) ||SUBSTRING(FISC_PER,6,7))::NUMERIC AS JJ_MNTH_ID
                         FROM edw_calendar_dim
                         WHERE jj_mnth_id >= (select last_17mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
						   and jj_mnth_id <= (select last_2mnths from edw_vw_cal_Retail_excellence_Dim)::numeric  
                        ) cal
                     on start_date <= cal.jj_mnth_id
                    and end_date >= cal.jj_mnth_id
					LEFT JOIN  itg_mds_master_mother_code_mapping mc on ltrim(msl.ean_code,'0') = ltrim(mc.sku_unique_identifier,'0')
             ) base
        LEFT JOIN (SELECT distinct Data_Src,
                          'CN' as CNTRY_CD,
                          'China Personal Care' as CNTRY_NM,
                          Region ,
                          city,
                          zone,
                          Distributor_Name,
                          Distributor_code,
						  soldto_code,
                          store_code,
                          store_name,
                          store_type,
                          --PKA_PRODUCT_KEY_DESC as pka_product_key_description
						  EAN,
						  --use mother code as per latest changes - June'2024
						  --Mother_code,
						  product_code,
						  msl_product_desc,
						  mapped_sku_code
                   from wks_china_personal_care_base_retail_excellence
                  ) noo
               ON upper(noo.store_type) = upper(base.Retail_Environment)
              --AND noo.pka_product_key_description = base.product_key
			   --AND noo.EAN = base.EAN_code
			     AND noo.product_code = base.mother_code
		left join cnpc_regional_sellout_mapped_sku_cd pd on base.ean_code = pd.ean 
		left join cnpc_regional_sellout_ean ed
ON --upper(main.PKA_PRODUCT_KEY_DESC)=PD.PKA_PRODUCT_KEY_DESCRIPTION
upper(base.mother_code) = ed.mother_code  
              ) final 

),
msl_final as 
(
select fisc_yr :: numeric(18,0) as fisc_yr,
        fisc_per :: numeric(18,0) as fisc_per,
        market :: varchar(50) as market,
        retail_environment :: varchar(382) as retail_environment,
        soldto_code :: varchar(255) as soldto_code,
        distributor_code :: varchar(100) as distributor_code,
        distributor_name :: varchar(356) as distributor_name,
        store_code :: varchar(100) as store_code,
        store_name :: varchar(601) as store_name,
        store_type :: varchar(382) as store_type,
        ean :: varchar(50) as ean,
        product_code :: varchar(200) as product_code,
        product_name :: varchar(300) as product_name,
        mapped_sku_code :: varchar(40) as mapped_sku_code,
        region :: varchar(150) as region,
        zone :: varchar(150) as zone,
        city :: varchar(150) as city,
        data_src :: varchar(14) as data_src,
        prod_hier_l1 :: varchar(19) as prod_hier_l1,
        created_date :: timestamp without time zone as created_date
        from cn_pc_re_msl_list
)

select * from msl_final