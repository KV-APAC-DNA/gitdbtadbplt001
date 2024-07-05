with edw_rpt_sales_details as
(
    select * from {{ ref('indedw_integration__edw_rpt_sales_details') }}
),
itg_query_parameters as 
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
itg_program_store_target as
(
    select * from {{ source('inditg_integration', 'itg_program_store_target') }}
),
edw_customer_dim as
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
edw_rpt_cockpit_invoice_targets as
(
    select * from {{ ref('indedw_integration__edw_rpt_cockpit_invoice_targets') }}
),
edw_rpt_schemeutilize_cube as
(
    select * from {{ ref('indedw_integration__edw_rpt_schemeutilize_cube') }}
),
v_rpt_fran_chnl_actl as 
(
    select * from {{ ref('indedw_integration__v_rpt_fran_chnl_actl') }}
),
v_rpt_gt_target_vs_actuals as
(
    select * from {{ ref('indedw_integration__v_rpt_gt_target_vs_actuals') }}
),
cte1 as
(
SELECT 'Sales' as dataset,
    mth_mm,
    week,
    UPPER(region_name) AS region_name,
    UPPER(zone_name) AS zone_name,
    UPPER(territory_name) AS territory_name,
    sales.customer_code as customer_code,
    sales.retailer_code as retailer_code,
    sales.customer_name as customer_name,
    sales.retailer_name as retailer_name,
    channel_name,
    UPPER(brand_name) AS brand_name,
    UPPER(franchise_name) AS franchise_name,
    territory_classification,
    class_desc,
    retailer_category_name,
    salesman_name,
    salesman_code,
    uniquesalescode,
    CASE
        WHEN ((channel_name IN ('GT','Baby Speciality Store') AND tgt.retailer_code IS NOT NULL) OR (channel_name IS NULL AND tgt.retailer_code IS NOT NULL)) THEN 'Platinum'
        WHEN channel_name IN ('GT','Baby Speciality Store') AND tgt.retailer_code IS NULL THEN 'Non-Platinum'
        WHEN channel_name = 'Self Service Store' AND tgt.retailer_code IS NOT NULL THEN 'Signature & Signature Chain & Premium'
        WHEN channel_name IS NULL AND tgt.retailer_code IS NOT NULL THEN 'Signature & Signature Chain & Premium'
        WHEN channel_name = 'Self Service Store' AND tgt.retailer_code IS NULL THEN 'Non-Signature & Non-Signature Chain & Non-Premium'
        WHEN channel_name IN ('Sub-Stockiest','Urban SubD','Pharma Reseller SubD') AND tgt.retailer_code IS NOT NULL THEN 'Bhagidari & Daud'
        WHEN channel_name IS NULL AND tgt.retailer_code IS NOT NULL THEN 'Bhagidari & Daud'
        WHEN channel_name IN ('Sub-Stockiest','Urban SubD','Pharma Reseller SubD') AND tgt.retailer_code IS NULL THEN 'Non-Bhagidari & Non-Daud'
        WHEN channel_name IN ('Wholesale','Pharma Resellers') AND tgt.retailer_code IS NOT NULL THEN 'Umang & Directors Club'
        WHEN channel_name IS NULL AND tgt.retailer_code IS NOT NULL THEN 'Umang & Directors Club'
        WHEN channel_name IN ('Wholesale','Pharma Resellers') AND tgt.retailer_code IS NULL THEN 'Non-Umang & Directors Club'
    END AS sales_cube_program_name,
    CASE
        WHEN (rn_s = 1 OR (sales.customer_code IS NULL AND rn_t = 1)) THEN tgt.value
        ELSE NULL
    END AS target_value,
    achievement_nr,
    num_buying_retailers,
    num_bills,
    num_packs,
    num_lines,
    NULL AS customer_type,
    NULL AS closing_stock,
    NULL AS billing_value,
    NULL AS measure_type,
    NULL AS brand_focus_target,
    NULL AS business_plan_target,
    NULL AS schemeamt,
    nvl(report_channel,'Missing Channel Mapping') as franchise_channel,
    NULL AS franchise_datasource,
    NULL AS sls_actl_val,
    null as targetbills,
    null as product_category1,
    null as product_category2,
    null as product_category3,
    null as product_category4,
    nvl(NULLIF(retailer_channel_1,''),'Missing Channel Level1') as retailer_channel_level1,
    nvl(NULLIF(retailer_channel_2,''),'Missing Channel Level2') as retailer_channel_level2,
    fisc_yr as fiscyr,
    mth_yyyymm as fiscmonth,
    day,
    salesman_status,
    net_amt,
    null as crt_dttm,
    nvl(NULLIF(retailer_channel_3,''),'Missing Channel Level2') as retailer_channel_level3,
    rtruniquecode,
    mothersku_code

FROM (SELECT sales1.*,
             ROW_NUMBER() OVER (PARTITION BY mth_mm,customer_code,retailer_code ORDER BY week) rn_s
      FROM (SELECT fisc_yr,
                   mth_yyyymm,
                   d.mth_mm,
                   week,
                   DAY,
                   region_name,
                   zone_name,
                   territory_name,
                   d.customer_code,
                   retailer_code,
                   customer_name,
                   retailer_name,
                   channel_name,
                   brand_name,
                   franchise_name,
                   territory_classification,
                   class_desc,
                   retailer_category_name,
                   d.salesman_name,
                   salesman_code,
                   salesman_status,
                   unique_sales_code AS uniquesalescode,
                   achievement_nr,
                   net_amt,
                   num_buying_retailers,
                   num_bills,
                   num_packs,
                   num_lines,
                   NULL AS Retailer_Channel_Level2,
                   retailer_channel_1,
                   retailer_channel_2,
                   retailer_channel_3,
                   report_channel,
                   mothersku_code,
                   rtruniquecode
            FROM edw_rpt_sales_details d
            WHERE d.fisc_yr >= (DATE_PART(YEAR, current_timestamp()) -2)
            AND   channel_name NOT IN ('Institutional Sale','CSD')
            ) sales1
        ) sales
  LEFT JOIN (SELECT program_name,
                    UPPER(region) AS region,
                    UPPER(ZONE) AS ZONE,
                    UPPER(territory) AS territory,
                    customer_code,
                    retailer_code,
                    TO_CHAR(year_month,'YYYYMM') AS year_month,
                    ROW_NUMBER() OVER (PARTITION BY TO_CHAR(year_month,'YYYYMM'),customer_code,retailer_code ORDER BY program_name) AS rn_t, 
                    SUM(value) AS value
             FROM itg_Program_Store_Target
             GROUP BY program_name,
                      UPPER(region),
                      UPPER(ZONE),
                      UPPER(territory),
                      customer_code,
                      retailer_code,
                      TO_CHAR(year_month,'YYYYMM')) tgt
         ON sales.mth_mm = year_month
        AND TRIM (sales.customer_code) = TRIM (tgt.customer_code)
        AND TRIM (nvl (sales.retailer_code,'x')) = TRIM (nvl (tgt.retailer_code,'x'))
),
cte2 as
(
SELECT 'Sales' as dataset,
    CAST(TO_CHAR(year_month,'YYYYMM') AS INTEGER) as mth_mm,
    1 as week,
    nvl(UPPER(region_name),UPPER(region)) AS region_name,
    nvl(UPPER(zone_name),UPPER(zone)) AS zone_name,
    nvl(UPPER(territory_name),UPPER(territory)) AS territory_name,
    tgt.customer_code as customer_code,
    retailer_code,
    NULL AS customer_name,
    NULL AS retailer_name,
    CASE
        WHEN (nvl (TRIM(tgt.Program_Name),'X') = 'Platinum') THEN 'GT'
        WHEN (nvl (TRIM(tgt.Program_Name),'X') = 'Signature' OR nvl (TRIM(tgt.Program_Name),'X') = 'Signature Chain' OR nvl (TRIM(tgt.Program_Name),'X') = 'Premium' OR SUBSTRING(nvl (TRIM(tgt.Program_Name),'X'),1,9) = 'Signature') THEN 'Self Service Store'
        WHEN (nvl (TRIM(tgt.Program_Name),'X') = 'Bhagidari' OR nvl (TRIM(tgt.Program_Name),'X') = 'Daud') THEN 'Sub-Stockiest'
        WHEN (nvl (TRIM(tgt.Program_Name),'X') = 'Umang' OR SUBSTRING(nvl (TRIM(tgt.Program_Name),'X'),1,8) = 'Director' OR SUBSTRING(nvl (TRIM(tgt.Program_Name),'X'),1,5) = 'Umang') THEN 'Wholesale'
    END AS channel_name,
    NULL AS brand_name,
    NULL AS franchise_name,
    NULL AS territory_classification,
    NULL AS class_desc,
    null as retailer_category_name,
    NULL AS salesman_name,
    NULL AS salesman_code,
    NULL AS uniquesalescode,
    CASE
        WHEN (nvl (TRIM(tgt.Program_Name),'X') = 'Platinum') THEN 'Platinum'
        WHEN (nvl (TRIM(tgt.Program_Name),'X') = 'Signature' OR nvl (TRIM(tgt.Program_Name),'X') = 'Signature Chain' OR nvl (TRIM(tgt.Program_Name),'X') = 'Premium' OR SUBSTRING(nvl (TRIM(tgt.Program_Name),'X'),1,9) = 'Signature') THEN 'Signature & Signature Chain & Premium'
        WHEN (nvl (TRIM(tgt.Program_Name),'X') = 'Bhagidari' OR nvl (TRIM(tgt.Program_Name),'X') = 'Daud') THEN 'Bhagidari & Daud'
        WHEN (nvl (TRIM(tgt.Program_Name),'X') = 'Umang' OR SUBSTRING(nvl (TRIM(tgt.Program_Name),'X'),1,8) = 'Director' OR SUBSTRING(nvl (TRIM(tgt.Program_Name),'X'),1,5) = 'Umang') THEN 'Umang & Directors Club'
    END AS sales_cube_program_name,
    SUM(tgt.value) AS target_value,
    NULL AS achievement_nr,
    NULL AS num_buying_retailers,
    NULL AS num_bills,
    NULL AS num_packs,
    NULL AS num_lines,
    NULL AS customer_type,
    NULL AS closing_stock,
    NULL AS billing_value,
    NULL AS measure_type,
    NULL AS brand_focus_target,
    NULL AS business_plan_target,
    NULL AS schemeamt,
    NULL AS franchise_channel,
    NULL AS franchise_datasource,
    NULL AS sls_actl_val,
    null as targetbills,
    null as product_category1,
    null as product_category2,
    null as product_category3,
    null as product_category4,
    null as retailer_channel_level1,
    null as retailer_channel_level2,
    CAST(TO_CHAR(year_month,'YYYY') AS INTEGER) as fiscyr,
    CAST(TO_CHAR(year_month,'MM') AS INTEGER) as fiscmonth,
    null as day,
    null as salesman_status,
    null as net_amt,
    null as crt_dttm,
    null as retailer_channel_level3,
    null as rtruniquecode,
    null as mothersku_code
FROM itg_Program_Store_Target tgt,
     edw_customer_dim d
WHERE NOT EXISTS (SELECT 1
                  FROM edw_rpt_sales_details sales
                  WHERE fisc_yr >= (DATE_PART(YEAR, current_timestamp()) -2)
                  AND   TO_CHAR(year_month,'YYYYMM') = sales.mth_mm
                  AND   TRIM(tgt.customer_code) = TRIM(sales.customer_code)
                  AND   TRIM(nvl(tgt.retailer_code,'x')) = TRIM(sales.retailer_code))
AND   tgt.customer_code = d.customer_code (+)
GROUP BY program_name,
         nvl(UPPER(region_name),UPPER(region)),
         nvl(UPPER(zone_name),UPPER(zone)),
         nvl(UPPER(territory_name),UPPER(territory)),
         tgt.customer_code,
         retailer_code,
         TO_CHAR(year_month,'YYYY'),
         TO_CHAR(year_month,'MM'),
         TO_CHAR(year_month,'YYYYMM')
),
cte3 as
(
SELECT Dataset,
    mth_mm,
    week,
    UPPER(region_name) as region_name,
    UPPER(zone_name) as zone_name,
    UPPER(territory_name) as territory_name,
    customer_code,
    retailer_code,
    customer_name,
    retailer_name,
    channel_name,
    UPPER(brand_name) AS brand_name,
    UPPER(franchise) AS franchise_name,
    NULL AS territory_classification,
    NULL AS class_desc,
    null as retailer_category_name,
    NULL AS salesman_name,
    NULL AS salesman_code,
    NULL AS uniquesalescode,
    NULL AS sales_cube_program_name,
    NULL AS target_value,
    NULL AS achievement_nr,
    NULL AS num_buying_retailers,
    NULL AS num_bills,
    NULL AS num_packs,
    NULL AS num_lines,
    customer_type,
    closing_stock,
    billing_value,
    measure_type,
    brand_focus_target,
    business_plan_target,
    NULL AS schemeamt,
    NULL AS franchise_channel,
    NULL AS franchise_datasource,
    NULL AS sls_actl_val,
    null as targetbills,
    null as product_category1,
    null as product_category2,
    null as product_category3,
    null as product_category4,
    null as retailer_channel_level1,
    null as retailer_channel_level2,
    CAST(LEFT (mth_mm,4) AS INTEGER) as fiscyr,
    CAST(RIGHT (mth_mm,2) AS INTEGER) as fiscmonth,
    null as day,
    null as salesman_status,
    null as net_amt,
    null as crt_dttm,
    null as retailer_channel_level3,
    null as rtruniquecode,
    null as mothersku_code
FROM edw_rpt_cockpit_invoice_targets),
cte4 as
(
SELECT 'Scheme' as dataset,
    CAST(TO_CHAR(schdate,'YYYYMM') AS INTEGER) AS mth_mm,
    week,
    UPPER(region_name) AS region_name,
    UPPER(zone_name) AS zone_name,
    UPPER(territory_name) AS territory_name,
    NULL AS customer_code,
    NULL AS retailer_code,
    NULL AS customer_name,
    NULL AS retailer_name,
    NULL AS channel_name,
    NULL AS brand_name,
    NULL AS franchise_name,
    NULL AS territory_classification,
    NULL AS class_desc,
    null as retailer_category_name,
    NULL AS salesman_name,
    NULL AS salesman_code,
    NULL AS uniquesalescode,
    NULL AS sales_cube_program_name,
    NULL AS target_value,
    NULL AS achievement_nr,
    NULL AS num_buying_retailers,
    NULL AS num_bills,
    NULL AS num_packs,
    NULL AS num_lines,
    NULL AS customer_type,
    NULL AS closing_stock,
    NULL AS billing_value,
    NULL AS measure_type,
    NULL AS brand_focus_target,
    NULL AS business_plan_target,
    SUM(schemeutilizedamt) as schemeamt,
    NULL AS franchise_channel,
    NULL AS franchise_datasource,
    NULL AS sls_actl_val,
    null as targetbills,
    null as product_category1,
    null as product_category2,
    null as product_category3,
    null as product_category4,
    null as retailer_channel_level1,
    null as retailer_channel_level2,
    CAST(TO_CHAR(schdate,'YYYY') AS INTEGER) AS fiscyr,
    CAST(TO_CHAR(schdate,'MM') AS INTEGER) AS fiscmonth,
    null as day,
    null as salesman_status,
    null as net_amt,
    null as crt_dttm,
    null as retailer_channel_level3,
    null as rtruniquecode,
    null as mothersku_code
FROM edw_rpt_schemeutilize_cube sch
WHERE CAST(TO_CHAR(schdate,'YYYY') AS INT) >= (DATE_PART(YEAR,current_timestamp()))-1
GROUP BY TO_CHAR(schdate,'YYYY'),
         CAST(TO_CHAR(schdate,'MM') AS INTEGER),
         TO_CHAR(schdate,'YYYYMM'),
         week,
         UPPER(region_name),
         UPPER(zone_name),
         UPPER(territory_name)
),
cte5 as
(
SELECT 'FranchiseChannel' as dataset,
    CAST(fisc_yr || lpad (fisc_mnth,2,'0') AS INTEGER) as mth_mm,
    fisc_wk as week,
    UPPER(region_name) AS region_name,
    UPPER(zone_name) AS zone_name,
    UPPER(territory_name) AS territory_name,
    customer_code,
    NULL AS retailer_code,
    customer_name,
    NULL AS retailer_name,
    NULL AS channel_name,
    UPPER(brand_name) AS brand_name,
    UPPER(franchise_name) AS franchise_name,
    NULL AS territory_classification,
    NULL AS class_desc,
    NULL AS retailer_category_name,
    NULL AS salesman_name,
    NULL AS salesman_code,
    NULL AS uniquesalescode,
    NULL AS sales_cube_program_name,
    null as target_value,
    NULL AS achievement_nr,
    null as num_buying_retailers,
    NULL AS num_bills,
    NULL AS num_packs,
    NULL AS num_lines,
    NULL AS customer_type,
    NULL AS closing_stock,
    NULL AS billing_value,
    NULL AS measure_type,
    NULL AS brand_focus_target,
    NULL AS business_plan_target,
    NULL AS schemeamt,
    channel AS franchise_channel,
    datasource AS franchise_datasource,
    SUM(sls_actl_val) AS sls_actl_val,
    null as targetbills,
    null as product_category1,
    null as product_category2,
    null as product_category3,
    null as product_category4,
    null as retailer_channel_level1,
    null as retailer_channel_level2,
    CAST(fisc_yr AS INTEGER) AS fiscyr,
    CAST(fisc_mnth AS INTEGER) AS fiscmonth,
    null as day,
    null as salesman_status,
    null as net_amt,
    null as crt_dttm,
    null as retailer_channel_level3,
    null as rtruniquecode,
    null as mothersku_code
       
FROM v_rpt_fran_chnl_actl
GROUP BY fisc_yr,
         fisc_mnth,
         fisc_wk,
         UPPER(region_name),
         UPPER(zone_name),
         UPPER(territory_name),
         customer_code,
         customer_name,
         UPPER(brand_name),
         UPPER(franchise_name),
         product_category1,
         product_category2,
         product_category3,
         product_category4,
         channel,
         datasource
),
cte6 as
(
SELECT 'RDSSM' as dataset,
    CAST(CAST(year AS VARCHAR) ||to_char (TO_DATE(month,'mon'),'mm') AS INTEGER) as mth_mm,
    week,
    UPPER(region_name) AS region_name,
    UPPER(zone_name) AS zone_name,
    UPPER(territory_name) AS territory_name,
    customer_code,
    null as retailer_code,
    null as customer_name,
    null as retailer_name,
    null as channel_name,
    null as brand_name,
    null as franchise_name,
    null as territory_classification,
    null as class_desc,
    null as retailer_category_name,
    null as salesman_name,
    salesman_code,
    null as uniquesalescode,
    null as sales_cube_program_name,
    null as target_value,
    null as achievement_nr,
    null as num_buying_retailers,
    null as num_bills,
    null as num_packs,
    null as num_lines,
    null as customer_type,
    null as closing_stock,
    null as billing_value,
    null as measure_type,
    null as brand_focus_target,
    null as business_plan_target,
    null as schemeamt,
    null as franchise_channel,
    null as franchise_datasource,
    null as sls_actl_val,
    SUM(targetbills) as targetbills,
    null as product_category1,
    null as product_category2,
    null as product_category3,
    null as product_category4,
    null as retailer_channel_level1,
    null as retailer_channel_level2,
    year as fiscyr,
    CAST(TO_CHAR(TO_DATE(month,'mon'),'mm') AS INTEGER) as fiscmonth,
    null as day,
    null as salesman_status,
    null as net_amt,
    null as crt_dttm,
    null as retailer_channel_level3,
    null as rtruniquecode,
    null as mothersku_code
FROM v_rpt_gt_target_vs_actuals
WHERE (CAST(year AS VARCHAR) ||to_char(TO_DATE(month,'mon'),'mm')) IS NOT NULL
GROUP BY year,
         CAST(TO_CHAR(TO_DATE(month,'mon'),'mm') AS INTEGER),
         CAST(year AS VARCHAR) ||to_char(TO_DATE(month,'mon'),'mm'),
         week,
         UPPER(region_name),
         UPPER(zone_name),
         UPPER(territory_name),
         customer_code,
         salesman_code
),
transformed as
(
    select * from cte1
    union all
    select * from cte2
    union all
    select * from cte3
    union all
    select * from cte4
    union all
    select * from cte5
    union all
    select * from cte6
),
final as 
(   
    select
        dataset::varchar(30) as dataset,
        mth_mm::number(18,0) as mth_mm,
        week::number(18,0) as week,
        region_name::varchar(50) as region_name,
        zone_name::varchar(50) as zone_name,
        territory_name::varchar(50) as territory_name,
        customer_code::varchar(50) as customer_code,
        retailer_code::varchar(50) as retailer_code,
        customer_name::varchar(150) as customer_name,
        retailer_name::varchar(150) as retailer_name,
        channel_name::varchar(150) as channel_name,
        brand_name::varchar(50) as brand_name,
        franchise_name::varchar(50) as franchise_name,
        territory_classification::varchar(50) as territory_classification,
        class_desc::varchar(50) as class_desc,
        retailer_category_name::varchar(50) as retailer_category_name,
        salesman_name::varchar(100) as salesman_name,
        salesman_code::varchar(200) as salesman_code,
        uniquesalescode::varchar(20) as uniquesalescode,
        sales_cube_program_name::varchar(50) as sales_cube_program_name,
        target_value::number(38,4) as target_value,
        achievement_nr::number(38,4) as achievement_nr,
        num_buying_retailers::varchar(200) as num_buying_retailers,
        num_bills::varchar(250) as num_bills,
        num_packs::varchar(50) as num_packs,
        num_lines::number(18,0) as num_lines,
        customer_type::varchar(50) as customer_type,
        closing_stock::number(38,4) as closing_stock,
        billing_value::number(38,4) as billing_value,
        measure_type::varchar(50) as measure_type,
        brand_focus_target::number(38,4) as brand_focus_target,
        business_plan_target::number(38,4) as business_plan_target,
        schemeamt::number(38,6) as schemeamt,
        franchise_channel::varchar(50) as franchise_channel,
        franchise_datasource::varchar(50) as franchise_datasource,
        sls_actl_val::number(38,4) as sls_actl_val,
        targetbills::number(38,6) as targetbills,
        product_category1::varchar(50) as product_category1,
        product_category2::varchar(50) as product_category2,
        product_category3::varchar(50) as product_category3,
        product_category4::varchar(50) as product_category4,
        retailer_channel_level1::varchar(100) as retailer_channel_level1,
        retailer_channel_level2::varchar(100) as retailer_channel_level2,
        fiscyr::number(18,0) as fiscyr,
        fiscmonth::number(18,0) as fiscmonth,
        day::number(18,0) as day,
        salesman_status::varchar(20) as salesman_status,
        net_amt::number(38,6) as net_amt,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        retailer_channel_level3::varchar(100) as retailer_channel_level3,
        rtruniquecode::varchar(100) as rtruniquecode,
        mothersku_code::varchar(50) as mothersku_code
    from transformed
)
select * from final
