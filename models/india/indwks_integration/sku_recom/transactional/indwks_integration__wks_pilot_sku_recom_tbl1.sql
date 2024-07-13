with wks_pilot_itg_sku_recom_flag as 
(
    select * from {{ ref('indwks_integration__wks_pilot_itg_sku_recom_flag') }}
),
edw_retailer_calendar_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
edw_customer_dim as
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
wks_pilot_edw_retailer_dim as
(
    select * from {{ ref('indwks_integration__wks_pilot_edw_retailer_dim') }}
),
itg_in_mds_channel_mapping as 
(
    select * from {{ ref('inditg_integration__itg_in_mds_channel_mapping') }}
),
final as 
(
    SELECT sku.mnth_id,
       sku.cust_cd,
       sku.retailer_cd,
       sku.mother_sku_cd,
       sku.oos_flag,
       sku.ms_flag,
       sku.cs_flag,
       sku.soq,
       sku.reco_date,
       cd.mth_mm,
       cd.qtr,
       cd.fisc_yr,
       cd.month_nm_shrt AS "month",
       cud.customer_name,
       cud.region_name,
       cud.zone_name,
       cud.territory_name,
       rd.rtruniquecode,
       rd.class_desc,
       rd.channel_name,
       rd.business_channel,
       rd.status_desc,
       rd.actv_flg,
       rd.retailer_name,
       rd.retailer_category_name,
       rd.csrtrcode,
       COALESCE(CASE WHEN cmap.retailer_channel_level_1::TEXT = ''::CHARACTER VARYING::TEXT THEN NULL::CHARACTER VARYING 
                     ELSE cmap.retailer_channel_level_1 
                END,'Unknown'::CHARACTER VARYING) AS retailer_channel_level_1,
       COALESCE(CASE WHEN cmap.retailer_channel_level_2::TEXT = ''::CHARACTER VARYING::TEXT THEN NULL::CHARACTER VARYING
                     ELSE cmap.retailer_channel_level_2 
                END,'Unknown'::CHARACTER VARYING) AS retailer_channel_level_2,
       COALESCE(CASE WHEN cmap.retailer_channel_level_3::TEXT = ''::CHARACTER VARYING::TEXT THEN NULL::CHARACTER VARYING
                     ELSE cmap.retailer_channel_level_3
                END,'Unknown'::CHARACTER VARYING) AS retailer_channel_level_3

        FROM wks_pilot_itg_sku_recom_flag sku

        LEFT JOIN (SELECT edw_retailer_calendar_dim.mth_mm,
                  edw_retailer_calendar_dim.fisc_yr,
                  edw_retailer_calendar_dim.month_nm_shrt,
                  edw_retailer_calendar_dim.qtr
          FROM edw_retailer_calendar_dim
          GROUP BY edw_retailer_calendar_dim.mth_mm,
                   edw_retailer_calendar_dim.fisc_yr,
                   edw_retailer_calendar_dim.month_nm_shrt,
                   edw_retailer_calendar_dim.qtr) cd 
       ON cd.mth_mm::CHARACTER VARYING::TEXT = sku.mnth_id::TEXT

       LEFT JOIN edw_customer_dim cud
       ON COALESCE(cud.customer_code,'0'::CHARACTER VARYING)::TEXT = COALESCE(sku.cust_cd,'0'::CHARACTER VARYING)::TEXT

      LEFT JOIN wks_pilot_edw_retailer_dim rd
       ON COALESCE (rd.retailer_code,'0'::CHARACTER VARYING)::TEXT = COALESCE (sku.retailer_cd,'0'::CHARACTER VARYING)::TEXT
      AND COALESCE (rd.customer_code,'0'::CHARACTER VARYING)::TEXT = COALESCE (sku.cust_cd,'0'::CHARACTER VARYING)::TEXT

       LEFT JOIN itg_in_mds_channel_mapping cmap
       ON cmap.channel_name::TEXT = CASE WHEN CASE WHEN rd.channel_name::TEXT = ''::CHARACTER VARYING::TEXT THEN NULL::CHARACTER VARYING
                                                   ELSE rd.channel_name END IS NULL THEN 'Unknown'::CHARACTER VARYING 
                                        ELSE rd.channel_name END::TEXT 
      AND cmap.retailer_category_name::TEXT = CASE WHEN CASE WHEN rd.retailer_category_name::TEXT = ''::CHARACTER VARYING::TEXT THEN NULL::CHARACTER VARYING
                                                             ELSE rd.retailer_category_name END IS NULL THEN 'Unknown'::CHARACTER VARYING
                                                   ELSE rd.retailer_category_name END::TEXT
      AND cmap.retailer_class::TEXT = CASE WHEN CASE WHEN rd.class_desc::TEXT = ''::CHARACTER VARYING::TEXT THEN NULL::CHARACTER VARYING
                                                     ELSE rd.class_desc END IS NULL THEN 'Unknown'::CHARACTER VARYING
                                           ELSE rd.class_desc END::TEXT  
      AND cmap.territory_classification::TEXT = CASE WHEN CASE WHEN rd.territory_classification::TEXT = ''::CHARACTER VARYING::TEXT THEN NULL::CHARACTER VARYING
                                                               ELSE rd.territory_classification END IS NULL THEN 'Unknown'::CHARACTER VARYING
                                                     ELSE rd.territory_classification END::TEXT  
)

select * from final