--Overwriding default SQL header as we dont want to change timezone to Singapore
{{
    config(
        sql_header= ""
    )
}}


--Import CTE
with itg_otif_consumer_attr as (
    select * from {{ ref('aspitg_integration__itg_otif_consumer_attr') }}
),
itg_otif_glbl_con_reporting as 
(
     select * from {{ ref('aspitg_integration__itg_otif_glbl_con_reporting') }}
),
itg_mds_ap_sales_ops_map as (
    select * from {{ ref('aspitg_integration__itg_mds_ap_sales_ops_map')}}
),


--Final CTE
final as (select region,RGN_MKT_CD, fiscal_yr_mo, segment_information, sum(numerator) as numerator, 
sum(denominator) as denominator,DIV0(sum(numerator),sum(denominator)) as otif
from (
SELECT
FISC_YR_NBR as fiscal_yr,
FISC_YR_MO_NUM as fiscal_yr_mo,
FISC_YR_WK_NUM as fiscal_yr_wk,
upper(RGN_CD) AS region,
SLORG_NUM as sls_org,
GCPH_GFO_DESC  as segment_information,
upper(trim(RGN_MKT_CD)) as RGN_MKT_CD,
ltrim(SOLD_TO_CUST_NUM, '0') as sold_to,
ltrim(MATL_NUM, '0') as matl_num,
sum(case when upper(trim(RGN_MKT_CD)) = 'JAPAN' and OTIF_EXCL_IND = 1 then NUMRTR_UNIT_QTY_SC
		when upper(trim(RGN_MKT_CD)) <> 'JAPAN' and OTIF_EXCL_IND = 0 then NUMRTR_UNIT_QTY_DELV else 0
	end) as numerator,
sum(case when upper(trim(RGN_MKT_CD)) = 'JAPAN' and OTIF_EXCL_IND = 1 then DENOM_UNIT_QTY_SC
		when upper(trim(RGN_MKT_CD)) <> 'JAPAN' and OTIF_EXCL_IND = 0 then DENOM_UNIT_QTY_DELV else 0
	end) as denominator
from itg_otif_consumer_attr		--// from DELV.otif_d_cnsmr_attr_detl
Where FISC_YR_MO_NUM >= '2023_08'
and AFFL_IND = '0'
and NO_CHRG_IND = '0'
and upper(RGN_CD) = 'APAC'
and (OTIF_EXCL_IND = 0 or OTIF_EXCL_IND = 1)
Group by FISC_YR_NBR,FISC_YR_MO_NUM,FISC_YR_WK_NUM,upper(RGN_CD),SLORG_NUM,segment_information,
upper(trim(RGN_MKT_CD)),ltrim(SOLD_TO_CUST_NUM, '0'),ltrim(MATL_NUM, '0')
) a Group by region,RGN_MKT_CD,fiscal_yr_mo,segment_information
 
UNION ALL
 
-- OLD: before 202308
select region,market,fiscal_yr_mo,segment_information, sum(numerator) as numerator, sum(denominator) as denominator,
DIV0(sum(numerator),sum(denominator)) as otif from (
SELECT upper(MAP.DESTINATION_MARKET) AS Market,		--// SELECT map.destination_market AS "market",
       upper(MAP.DESTINATION_CLUSTER) AS cluster,		--//        map.destination_cluster AS "cluster",
       fiscal_yr,
       fiscal_yr_mo,
       fiscal_yr_wk,
       region,
       salesorg as sls_org,
       worldwide_franchise as segment_information,
       ltrim(sold_to_nbr, '0') as sold_to,
       ltrim(material, '0') as matl_num,
       SUM(case when upper(country) = 'JP' then num_unit_otifd_ship_confirm else numerator_unit_otifd_delivery end) AS numerator,
       SUM(case when upper(country) = 'JP' then denom_unit_otifsc else denom_unit_otifd end) AS denominator
  FROM itg_otif_glbl_con_reporting trans
    JOIN itg_mds_ap_sales_ops_map map
   ON upper(TRANS.COUNTRY) = upper(MAP.SOURCE_MARKET)		--//    ON trans.country = map.source_market
   AND upper(TRANS.CLUSTER_NAME) = upper(MAP.SOURCE_CLUSTER)		--//    AND trans.cluster_name = map.source_cluster
   AND dataset = 'OTIF-D'
WHERE upper(region) = 'APAC'
AND   no_charge_ind = 'Revenue'
AND   affiliate_flag = '0'
AND   fiscal_yr_mo <= '2023_07'
GROUP BY 1,2,3,4,5,6,7,8,9,10
//ORDER BY 1,2,3,4,5,6,7,8,9
) group by 1,2,3,4
)
select * from final

