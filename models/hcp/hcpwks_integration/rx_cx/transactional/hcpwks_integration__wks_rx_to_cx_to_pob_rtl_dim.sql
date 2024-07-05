with 
wks_rx_to_cx_to_pob_base_rtl as 
(
    select * from {{ ref('hcpwks_integration__wks_rx_to_cx_to_pob_base_rtl') }}
),
itg_in_mds_channel_mapping as 
(
    select * from snapinditg_integration.itg_in_mds_channel_mapping
),
edw_customer_dim as 
(
    select * from snapindedw_integration.edw_customer_dim
),
edw_retailer_dim as 
(
    select * from snapindedw_integration.edw_retailer_dim
),
final as 
(
    SELECT 
       rtruniquecode,
       customer_code,
       retailer_code,
       retailer_name,
       region_name,
       zone_name,
       territory_name,
       customer_name,
       urc_active_flag,
       channel_name,
       class_desc,
       retailer_category_name,
       retailer_channel_level1,
        retailer_channel_level2,
        retailer_channel_level3
FROM (SELECT rd.rtruniquecode,
             rd.customer_code,
             rd.retailer_code,
             rd.retailer_name,
             rd.region_name,
             rd.zone_name,
             rd.territory_name,
             rd.customer_name,
             rd.channel_name,
             rd.class_desc,
             rd.retailer_category_name,
             COALESCE(cmap.report_channel, 'Missing Channel Mapping'::character varying) AS report_channel, COALESCE(
        CASE
            WHEN cmap.retailer_channel_level_1::text = ''::character varying::text THEN NULL::character varying
            ELSE cmap.retailer_channel_level_1
        END, 'Unknown'::character varying) AS retailer_channel_level1, COALESCE(
        CASE
            WHEN cmap.retailer_channel_level_2::text = ''::character varying::text THEN NULL::character varying
            ELSE cmap.retailer_channel_level_2
        END, 'Unknown'::character varying) AS retailer_channel_level2, COALESCE(
        CASE
            WHEN cmap.retailer_channel_level_3::text = ''::character varying::text THEN NULL::character varying
            ELSE cmap.retailer_channel_level_3
        END, 'Unknown'::character varying) AS retailer_channel_level3,
             'Y' AS urc_active_flag,
             row_number() OVER (PARTITION BY rd.rtruniquecode ORDER BY rd.end_date DESC) AS rn
      FROM edw_retailer_dim rd
        INNER JOIN edw_customer_dim cd
                ON rd.customer_code = cd.customer_code
               AND cd.active_flag = 'Y'
               LEFT JOIN itg_in_mds_channel_mapping cmap ON cmap.channel_name::text = 
   CASE
       WHEN 
       CASE
           WHEN rd.channel_name::text = ''::character varying::text THEN NULL::character varying
           ELSE rd.channel_name
       END IS NULL THEN 'Unknown'::character varying
       ELSE rd.channel_name
   END::text AND cmap.retailer_category_name::text = 
   CASE
       WHEN 
       CASE
           WHEN rd.retailer_category_name::text = ''::character varying::text THEN NULL::character varying
           ELSE rd.retailer_category_name
       END IS NULL THEN 'Unknown'::character varying
       ELSE rd.retailer_category_name
   END::text AND cmap.retailer_class::text = 
   CASE
       WHEN 
       CASE
           WHEN rd.class_desc::text = ''::character varying::text THEN NULL::character varying
           ELSE rd.class_desc
       END IS NULL THEN 'Unknown'::character varying
       ELSE rd.class_desc
   END::text AND cmap.territory_classification::text = 
   CASE
       WHEN 
       CASE
           WHEN rd.territory_classification::text = ''::character varying::text THEN NULL::character varying
           ELSE rd.territory_classification
       END IS NULL THEN 'Unknown'::character varying
       ELSE rd.territory_classification
   END::text
      WHERE rd.status_desc = 'Active'
      AND   rd.actv_flg = 'Y'
      AND   rd.rtruniquecode IN (SELECT DISTINCT urc::TEXT
                                 FROM wks_rx_to_cx_to_pob_base_rtl))
WHERE rn = 1
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7,
         8,
         9,
         10,
         11,
         12,
         13,
         14,
         15
)
select * from final