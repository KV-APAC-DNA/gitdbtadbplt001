{{
    config(
        materialized='view'
    )
}}

with edw_ka_sales_fact as
(
    select * from {{ ref('indedw_integration__edw_ka_sales_fact') }}
),
edw_key_account_dim as
(
    select * from {{ ref('indedw_integration__edw_key_account_dim') }}
),
v_product_dim as
(
    select * from {{ ref('indedw_integration__v_product_dim') }}
),
edw_retailer_calendar_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
itg_mds_in_businessplan_brand as
(
    select * from {{ ref('inditg_integration__itg_mds_in_businessplan_brand') }}
),
itg_mds_in_key_accounts_mapping as
(
    select * from {{ ref('inditg_integration__itg_mds_in_key_accounts_mapping') }}
),
itg_mds_msku_internal_product_mapping_ka as
(
    select * from {{ source('inditg_integration', 'itg_mds_msku_internal_product_mapping_ka') }}
),
final as
(
    SELECT 
    CASE WHEN((kdd.parent_code IS NULL) OR (trim((kdd.parent_code):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdd.parent_code END AS parent_code, 
    CASE WHEN((ksfd.customer_code IS NULL) OR (trim((ksfd.customer_code):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE ksfd.customer_code END AS customer_code, 
    CASE WHEN((ksfd.customer_code IS NULL) OR (trim((ksfd.customer_code):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE ksfd.customer_code END AS ka_code, 
    CASE WHEN((kdd.ka_name IS NULL) OR (trim((kdd.ka_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdd.ka_name END AS ka_name, 
    'Direct' AS ka_type, 
    CASE WHEN((kdd.distributor_name IS NULL) OR (trim((kdd.distributor_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdd.distributor_name END AS distributor_name, 
    CASE WHEN((kdd.parent_name IS NULL) OR (trim((kdd.parent_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdd.parent_name END AS parent_name, 
    CASE WHEN((kdd.region_name IS NULL) OR (trim((kdd.region_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdd.region_name END AS region_name, 
    CASE WHEN((kdd.zone_name IS NULL) OR (trim((kdd.zone_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdd.zone_name END AS zone_name, 
    CASE WHEN((kdd.territory_name IS NULL) OR (trim((kdd.territory_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdd.territory_name END AS territory_name, 
    CASE WHEN((kdd.state_name IS NULL) OR (trim((kdd.state_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdd.state_name END AS state_name, 
    CASE WHEN((kdd.town_name IS NULL) OR (trim((kdd.town_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdd.town_name END AS town_name, 
    CASE WHEN((kdd.plant IS NULL) OR (trim((kdd.plant):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdd.plant END AS plant, 
    CASE WHEN((kdd.abi_name IS NULL) OR (trim((kdd.abi_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdd.abi_name END AS abi_name, 
    CASE WHEN((ksfd.product_code IS NULL) OR (trim((ksfd.product_code):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE ksfd.product_code END AS product_code, 
    CASE WHEN((pd.product_name IS NULL) OR (trim((pd.product_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE pd.product_name END AS product_name, 
    CASE WHEN((pd.franchise_name IS NULL) OR (trim((pd.franchise_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE pd.franchise_name END AS franchise_name, 
    CASE WHEN((pd.brand_name IS NULL) OR (trim((pd.brand_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE pd.brand_name END AS brand_name, 
    CASE WHEN((pd.product_category_name IS NULL) OR (trim((pd.product_category_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE pd.product_category_name END AS product_category_name, 
    CASE WHEN((pd.variant_name IS NULL) OR (trim((pd.variant_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE pd.variant_name END AS variant_name, 
    CASE WHEN((pd.mothersku_name IS NULL) OR (trim((pd.mothersku_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE pd.mothersku_name END AS mothersku_name, 
    cd.day, 
    CASE WHEN (cd.mth_yyyymm = 1) THEN 'JANUARY' :: character varying 
        WHEN (cd.mth_yyyymm = 2) THEN 'FEBRUARY' :: character varying 
        WHEN (cd.mth_yyyymm = 3) THEN 'MARCH' :: character varying 
        WHEN (cd.mth_yyyymm = 4) THEN 'APRIL' :: character varying 
        WHEN (cd.mth_yyyymm = 5) THEN 'MAY' :: character varying 
        WHEN (cd.mth_yyyymm = 6) THEN 'JUNE' :: character varying 
        WHEN (cd.mth_yyyymm = 7) THEN 'JULY' :: character varying 
        WHEN (cd.mth_yyyymm = 8) THEN 'AUGUST' :: character varying 
        WHEN (cd.mth_yyyymm = 9) THEN 'SEPTEMBER' :: character varying 
        WHEN (cd.mth_yyyymm = 10) THEN 'OCTOBER' :: character varying 
        WHEN (cd.mth_yyyymm = 11) THEN 'NOVEMBER' :: character varying 
        WHEN (cd.mth_yyyymm = 12) THEN 'DECEMBER' :: character varying 
        ELSE 'UNKNOWN' :: character varying END AS "month", 
    ((('Q' :: character varying):: text || ((cd.qtr):: character varying):: text)):: character varying AS qtr, 
    ((('Week ' :: character varying):: text || ((cd.week):: character varying):: text)):: character varying AS week, 
    cd.fisc_yr AS "year", 
    CASE WHEN (ksfd.prdqty IS NULL) THEN 0 ELSE ksfd.prdqty END AS prdqty, 
    CASE WHEN (ksfd.prdtaxamt IS NULL) THEN ((0):: numeric):: numeric(18, 0) 
    ELSE ksfd.prdtaxamt END AS prdtaxamt, 
    CASE WHEN (ksfd.prdschdiscamt IS NULL) THEN ((0):: numeric):: numeric(18, 0) 
    ELSE ksfd.prdschdiscamt END AS prdschdiscamt, 
    CASE WHEN (ksfd.prddbdiscamt IS NULL) THEN ((0):: numeric):: numeric(18, 0) 
    ELSE ksfd.prddbdiscamt END AS prddbdiscamt, 
    CASE WHEN (ksfd.salwdsamt IS NULL) THEN ((0):: numeric):: numeric(18, 0) 
    ELSE ksfd.salwdsamt END AS salwdsamt, 
    CASE WHEN(ksfd.totalgrosssalesincltax IS NULL) THEN ((0):: numeric):: numeric(18, 0) 
    ELSE ksfd.totalgrosssalesincltax END AS totalgrosssalesincltax, 
    CASE WHEN (ksfd.totalsalesnr IS NULL) THEN ((0):: numeric):: numeric(18, 0) 
    ELSE ksfd.totalsalesnr END AS totalsalesnr, 
    CASE WHEN (ksfd.totalsalesconfirmed IS NULL) THEN ((0):: numeric):: numeric(18, 0) 
    ELSE ksfd.totalsalesconfirmed END AS totalsalesconfirmed, 
    CASE WHEN(ksfd.totalsalesnrconfirmed IS NULL) THEN ((0):: numeric):: numeric(18, 0) 
    ELSE ksfd.totalsalesnrconfirmed END AS totalsalesnrconfirmed, 
    CASE WHEN(ksfd.totalsalesunconfirmed IS NULL) THEN ((0):: numeric):: numeric(18, 0) 
    ELSE ksfd.totalsalesunconfirmed END AS totalsalesunconfirmed, 
    CASE WHEN(ksfd.totalsalesnrunconfirmed IS NULL) THEN ((0):: numeric):: numeric(18, 0) 
    ELSE ksfd.totalsalesnrunconfirmed END AS totalsalesnrunconfirmed, 
    CASE WHEN (ksfd.totalqtyconfirmed IS NULL) THEN 0 
    ELSE ksfd.totalqtyconfirmed END AS totalqtyconfirmed, 
    CASE WHEN (ksfd.totalqtyunconfirmed IS NULL) THEN 0 
    ELSE ksfd.totalqtyunconfirmed END AS totalqtyunconfirmed, 
    CASE WHEN((ksfd.buyingoutlets IS NULL) OR (trim((ksfd.buyingoutlets):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE ksfd.buyingoutlets END AS totalbuyingoutlets, 
    NULL AS abi_ntid, 
    NULL AS flm_ntid, 
    NULL AS bdm_ntid, 
    NULL AS rsm_ntid, 
    (CASE WHEN((mdsbu.variant_name_code IS NULL) OR (trim((mdsbu.variant_name_code):: text) = ('' :: character varying):: text)) THEN ('Unknown' :: character varying):: text ELSE upper((mdsbu.variant_name_code):: text) END):: character varying AS variant_name_code, 
    (CASE WHEN(  (mdsbu.brand_dsr_code IS NULL) OR (trim((mdsbu.brand_dsr_code):: text) = ('' :: character varying):: text)) THEN ('Unknown' :: character varying):: text ELSE upper((mdsbu.brand_dsr_code):: text) END):: character varying AS brand_dsr_code, 
    (CASE WHEN(  (mdsbu.franchise_dsr_code IS NULL) OR (trim((mdsbu.franchise_dsr_code):: text) = ('' :: character varying):: text)) THEN ('Unknown' :: character varying):: text ELSE upper((mdsbu.franchise_dsr_code):: text) END):: character varying AS franchise_dsr_code, 
    CASE WHEN((acc.channel_name_code IS NULL) OR (trim((acc.channel_name_code):: text) = ('' :: character varying):: text)) THEN 'Unknown' :: character varying ELSE acc.channel_name_code END AS channel_name, 
    CASE WHEN((mskuipm.internal_franchise_name IS NULL) OR (trim((mskuipm.internal_franchise_name):: text) = ('' :: character varying):: text)) THEN 'Unknown' :: character varying ELSE mskuipm.internal_franchise_name END AS internal_franchise_name, 
    CASE WHEN((mskuipm.internal_brand_name IS NULL) OR (trim((mskuipm.internal_brand_name):: text) = ('' :: character varying):: text)) THEN 'Unknown' :: character varying ELSE mskuipm.internal_brand_name END AS internal_brand_name, 
    CASE WHEN((mskuipm.internal_variant_name IS NULL) OR (trim((mskuipm.internal_variant_name):: text) = ('' :: character varying):: text)) THEN 'Unknown' :: character varying ELSE mskuipm.internal_variant_name END AS internal_variant_name 
    FROM ((((((edw_ka_sales_fact ksfd 
                LEFT JOIN (
                    SELECT 
                    edw_key_account_dim.ka_code, 
                    edw_key_account_dim.ka_name, 
                    edw_key_account_dim.ka_flag, 
                    edw_key_account_dim.parent_code, 
                    edw_key_account_dim.parent_name, 
                    edw_key_account_dim.distributor_code, 
                    edw_key_account_dim.distributor_name, 
                    edw_key_account_dim.ka_address1, 
                    edw_key_account_dim.ka_address2, 
                    edw_key_account_dim.ka_address3, 
                    edw_key_account_dim.region_code, 
                    edw_key_account_dim.region_name, 
                    edw_key_account_dim.zone_code, 
                    edw_key_account_dim.zone_name, 
                    edw_key_account_dim.territory_code, 
                    edw_key_account_dim.territory_name, 
                    edw_key_account_dim.state_code, 
                    edw_key_account_dim.state_name, 
                    edw_key_account_dim.town_code, 
                    edw_key_account_dim.town_name, 
                    edw_key_account_dim.active_flag, 
                    edw_key_account_dim.abi_code, 
                    edw_key_account_dim.abi_name, 
                    edw_key_account_dim.plant, 
                    edw_key_account_dim.crt_dttm, 
                    edw_key_account_dim.updt_dttm 
                    FROM 
                    edw_key_account_dim edw_key_account_dim 
                    WHERE (edw_key_account_dim.ka_flag = 'D' :: char)) kdd 
                    ON (((ksfd.customer_code):: text = (kdd.ka_code):: text))) 
                LEFT JOIN v_product_dim pd 
                ON (((ksfd.product_code):: text = (pd.product_code):: text))) 
            LEFT JOIN edw_retailer_calendar_dim cd ON ((ksfd.invoice_date = cd.day))
            ) 
            LEFT JOIN (
            SELECT 
                DISTINCT itg_mds_in_businessplan_brand.variant_tableau_code, 
                itg_mds_in_businessplan_brand.brand_dsr_code, 
                itg_mds_in_businessplan_brand.franchise_dsr_code, 
                itg_mds_in_businessplan_brand.variant_name_code 
            FROM 
                itg_mds_in_businessplan_brand
            ) mdsbu ON (((pd.variant_name):: text = (mdsbu.variant_tableau_code):: text))) 
        LEFT JOIN (SELECT 
            DISTINCT itg_mds_in_key_accounts_mapping.code, 
            itg_mds_in_key_accounts_mapping.account_name_name, 
            itg_mds_in_key_accounts_mapping.channel_name_code 
            FROM 
            itg_mds_in_key_accounts_mapping
        ) acc ON (((ksfd.customer_code):: text = (acc.code):: text))
        ) 
        LEFT JOIN (
        SELECT 
            itg_mds_msku_internal_product_mapping_ka.mothersku_name, 
            itg_mds_msku_internal_product_mapping_ka.internal_franchise_name, 
            itg_mds_msku_internal_product_mapping_ka.internal_brand_name, 
            itg_mds_msku_internal_product_mapping_ka.internal_variant_name, 
            row_number() OVER(PARTITION BY itg_mds_msku_internal_product_mapping_ka.mothersku_name ORDER BY 
                itg_mds_msku_internal_product_mapping_ka.internal_franchise_name, 
                itg_mds_msku_internal_product_mapping_ka.internal_brand_name, 
                itg_mds_msku_internal_product_mapping_ka.internal_variant_name
            ) AS rn 
        FROM 
            itg_mds_msku_internal_product_mapping_ka
        ) mskuipm ON ((((pd.mothersku_name):: text = (mskuipm.mothersku_name):: text) AND (mskuipm.rn = 1)))
    ) 
    WHERE ((ksfd.retailer_code IS NULL) OR (trim((ksfd.retailer_code):: text) = ('' :: character varying):: text)) 

    UNION ALL 

    SELECT 
    CASE WHEN((kdw.parent_code IS NULL) OR (trim((kdw.parent_code):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdw.parent_code END AS parent_code, 
    CASE WHEN((ksfw.customer_code IS NULL) OR (trim((ksfw.customer_code):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE ksfw.customer_code END AS customer_code, 
    CASE WHEN((ksfw.retailer_code IS NULL) OR (trim((ksfw.retailer_code):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE ksfw.retailer_code END AS ka_code, 
    CASE WHEN((ksfw.retailer_name IS NULL) OR (trim((ksfw.retailer_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE ksfw.retailer_name END AS ka_name, 
    'Wholesaler' AS ka_type, 
    CASE WHEN((kdw.distributor_name IS NULL) OR (trim((kdw.distributor_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdw.distributor_name END AS distributor_name, 
    CASE WHEN((kdw.parent_name IS NULL) OR (trim((kdw.parent_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdw.parent_name END AS parent_name, 
    CASE WHEN((kdw.region_name IS NULL) OR (trim((kdw.region_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdw.region_name END AS region_name, 
    CASE WHEN((kdw.zone_name IS NULL) OR (trim((kdw.zone_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdw.zone_name END AS zone_name, 
    CASE WHEN((kdw.territory_name IS NULL) OR (trim((kdw.territory_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdw.territory_name END AS territory_name, 
    CASE WHEN((kdw.state_name IS NULL) OR (trim((kdw.state_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdw.state_name END AS state_name, 
    CASE WHEN((kdw.town_name IS NULL) OR (trim((kdw.town_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdw.town_name END AS town_name, 
    CASE WHEN((kdw.plant IS NULL) OR (trim((kdw.plant):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdw.plant END AS plant, 
    CASE WHEN((kdw.abi_name IS NULL) OR (trim((kdw.abi_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE kdw.abi_name END AS abi_name, 
    CASE WHEN((ksfw.product_code IS NULL) OR (trim((ksfw.product_code):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE ksfw.product_code END AS product_code, 
    CASE WHEN((pd.product_name IS NULL) OR (trim((pd.product_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE pd.product_name END AS product_name, 
    CASE WHEN((pd.franchise_name IS NULL) OR (trim((pd.franchise_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE pd.franchise_name END AS franchise_name, 
    CASE WHEN((pd.brand_name IS NULL) OR (trim((pd.brand_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE pd.brand_name END AS brand_name, 
    CASE WHEN((pd.product_category_name IS NULL) OR (trim((pd.product_category_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE pd.product_category_name END AS product_category_name, 
    CASE WHEN((pd.variant_name IS NULL) OR (trim((pd.variant_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE pd.variant_name END AS variant_name, 
    CASE WHEN((pd.mothersku_name IS NULL) OR (trim((pd.mothersku_name):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE pd.mothersku_name END AS mothersku_name, 
    cd.day, 
    CASE WHEN (cd.mth_yyyymm = 1) THEN 'JANUARY' :: character varying 
        WHEN (cd.mth_yyyymm = 2) THEN 'FEBRUARY' :: character varying 
        WHEN (cd.mth_yyyymm = 3) THEN 'MARCH' :: character varying 
        WHEN (cd.mth_yyyymm = 4) THEN 'APRIL' :: character varying 
        WHEN (cd.mth_yyyymm = 5) THEN 'MAY' :: character varying 
        WHEN (cd.mth_yyyymm = 6) THEN 'JUNE' :: character varying 
        WHEN (cd.mth_yyyymm = 7) THEN 'JULY' :: character varying 
        WHEN (cd.mth_yyyymm = 8) THEN 'AUGUST' :: character varying 
        WHEN (cd.mth_yyyymm = 9) THEN 'SEPTEMBER' :: character varying 
        WHEN (cd.mth_yyyymm = 10) THEN 'OCTOBER' :: character varying 
        WHEN (cd.mth_yyyymm = 11) THEN 'NOVEMBER' :: character varying 
        WHEN (cd.mth_yyyymm = 12) THEN 'DECEMBER' :: character varying 
        ELSE 'UNKNOWN' :: character varying END AS "month", 
    ((('Q' :: character varying):: text || ((cd.qtr):: character varying):: text)):: character varying AS qtr, 
    ((('Week ' :: character varying):: text || ((cd.week):: character varying):: text)):: character varying AS week, 
    cd.fisc_yr AS "year", 
    CASE WHEN (ksfw.prdqty IS NULL) THEN 0 ELSE ksfw.prdqty END AS prdqty, 
    CASE WHEN (ksfw.prdtaxamt IS NULL) THEN ((0):: numeric):: numeric(18, 0) ELSE ksfw.prdtaxamt END AS prdtaxamt, 
    CASE WHEN (ksfw.prdschdiscamt IS NULL) THEN ((0):: numeric):: numeric(18, 0) ELSE ksfw.prdschdiscamt END AS prdschdiscamt, 
    CASE WHEN (ksfw.prddbdiscamt IS NULL) THEN ((0):: numeric):: numeric(18, 0) ELSE ksfw.prddbdiscamt END AS prddbdiscamt, 
    CASE WHEN (ksfw.salwdsamt IS NULL) THEN ((0):: numeric):: numeric(18, 0) ELSE ksfw.salwdsamt END AS salwdsamt, 
    CASE WHEN(ksfw.totalgrosssalesincltax IS NULL) THEN ((0):: numeric):: numeric(18, 0) 
    ELSE ksfw.totalgrosssalesincltax END AS totalgrosssalesincltax, 
    CASE WHEN (ksfw.totalsalesnr IS NULL) THEN ((0):: numeric):: numeric(18, 0) ELSE ksfw.totalsalesnr END AS totalsalesnr, 
    CASE WHEN (ksfw.totalsalesconfirmed IS NULL) THEN ((0):: numeric):: numeric(18, 0) ELSE ksfw.totalsalesconfirmed END AS totalsalesconfirmed, 
    CASE WHEN(ksfw.totalsalesnrconfirmed IS NULL) THEN ((0):: numeric):: numeric(18, 0) ELSE ksfw.totalsalesnrconfirmed END AS totalsalesnrconfirmed, 
    CASE WHEN(ksfw.totalsalesunconfirmed IS NULL) THEN ((0):: numeric):: numeric(18, 0) ELSE ksfw.totalsalesunconfirmed END AS totalsalesunconfirmed, 
    CASE WHEN(ksfw.totalsalesnrunconfirmed IS NULL) THEN ((0):: numeric):: numeric(18, 0) 
    ELSE ksfw.totalsalesnrunconfirmed END AS totalsalesnrunconfirmed, 
    CASE WHEN (ksfw.totalqtyconfirmed IS NULL) THEN 0 ELSE ksfw.totalqtyconfirmed END AS totalqtyconfirmed, 
    CASE WHEN (ksfw.totalqtyunconfirmed IS NULL) THEN 0 ELSE ksfw.totalqtyunconfirmed END AS totalqtyunconfirmed, 
    CASE WHEN((ksfw.buyingoutlets IS NULL) OR (trim((ksfw.buyingoutlets):: text) = ('' :: character varying):: text))
    THEN 'Unknown' :: character varying ELSE ksfw.buyingoutlets END AS totalbuyingoutlets, 
    NULL AS abi_ntid, 
    NULL AS flm_ntid, 
    NULL AS bdm_ntid, 
    NULL AS rsm_ntid, 
    (CASE WHEN((mdsbu.variant_name_code IS NULL) OR (trim((mdsbu.variant_name_code):: text) = ('' :: character varying):: text)) THEN ('Unknown' :: character varying):: text ELSE upper((mdsbu.variant_name_code):: text) END):: character varying AS variant_name_code, 
    (CASE WHEN((mdsbu.brand_dsr_code IS NULL) OR (trim((mdsbu.brand_dsr_code):: text) = ('' :: character varying):: text)) THEN ('Unknown' :: character varying):: text ELSE upper((mdsbu.brand_dsr_code):: text) END):: character varying AS brand_dsr_code, 
    (CASE WHEN(  (mdsbu.franchise_dsr_code IS NULL) OR (trim((mdsbu.franchise_dsr_code):: text) = ('' :: character varying):: text)) THEN ('Unknown' :: character varying):: text ELSE upper((mdsbu.franchise_dsr_code):: text) END):: character varying AS franchise_dsr_code, 
    CASE WHEN((acc.channel_name_code IS NULL) OR (trim((acc.channel_name_code):: text) = ('' :: character varying):: text)) THEN 'Unknown' :: character varying ELSE acc.channel_name_code END AS channel_name, 
    CASE WHEN((mskuipm.internal_franchise_name IS NULL) OR (trim((mskuipm.internal_franchise_name):: text) = ('' :: character varying):: text)) THEN 'Unknown' :: character varying ELSE mskuipm.internal_franchise_name END AS internal_franchise_name, 
    CASE WHEN((mskuipm.internal_brand_name IS NULL) OR (trim((mskuipm.internal_brand_name):: text) = ('' :: character varying):: text)) THEN 'Unknown' :: character varying ELSE mskuipm.internal_brand_name END AS internal_brand_name, 
    CASE WHEN((mskuipm.internal_variant_name IS NULL) OR (trim((mskuipm.internal_variant_name):: text) = ('' :: character varying):: text)) THEN 'Unknown' :: character varying ELSE mskuipm.internal_variant_name END AS internal_variant_name 
    FROM ((((((edw_ka_sales_fact ksfw 
                LEFT JOIN (
                    SELECT 
                    edw_key_account_dim.ka_code, 
                    edw_key_account_dim.ka_name, 
                    edw_key_account_dim.ka_flag, 
                    edw_key_account_dim.parent_code, 
                    edw_key_account_dim.parent_name, 
                    edw_key_account_dim.distributor_code, 
                    edw_key_account_dim.distributor_name, 
                    edw_key_account_dim.ka_address1, 
                    edw_key_account_dim.ka_address2, 
                    edw_key_account_dim.ka_address3, 
                    edw_key_account_dim.region_code, 
                    edw_key_account_dim.region_name, 
                    edw_key_account_dim.zone_code, 
                    edw_key_account_dim.zone_name, 
                    edw_key_account_dim.territory_code, 
                    edw_key_account_dim.territory_name, 
                    edw_key_account_dim.state_code, 
                    edw_key_account_dim.state_name, 
                    edw_key_account_dim.town_code, 
                    edw_key_account_dim.town_name, 
                    edw_key_account_dim.active_flag, 
                    edw_key_account_dim.abi_code, 
                    edw_key_account_dim.abi_name, 
                    edw_key_account_dim.plant, 
                    edw_key_account_dim.crt_dttm, 
                    edw_key_account_dim.updt_dttm 
                    FROM edw_key_account_dim edw_key_account_dim 
                    WHERE (edw_key_account_dim.ka_flag = 'W' :: char)) kdw 
                    ON ((((ksfw.retailer_code):: text = (kdw.ka_code):: text) 
                    AND ((ksfw.customer_code):: text = (kdw.distributor_code):: text)
                    ))) 
                LEFT JOIN v_product_dim pd ON (((ksfw.product_code):: text = (pd.product_code):: text))) 
                LEFT JOIN edw_retailer_calendar_dim cd ON ((ksfw.invoice_date = cd.day))) 
            LEFT JOIN (SELECT 
                DISTINCT itg_mds_in_businessplan_brand.variant_tableau_code, 
                itg_mds_in_businessplan_brand.brand_dsr_code, 
                itg_mds_in_businessplan_brand.franchise_dsr_code, 
                itg_mds_in_businessplan_brand.variant_name_code 
            FROM 
                itg_mds_in_businessplan_brand
            ) mdsbu ON (((pd.variant_name):: text = (mdsbu.variant_tableau_code):: text))
        ) 
        LEFT JOIN (
            SELECT 
            DISTINCT itg_mds_in_key_accounts_mapping.code, 
            itg_mds_in_key_accounts_mapping.account_name_name, 
            itg_mds_in_key_accounts_mapping.channel_name_code 
            FROM 
            itg_mds_in_key_accounts_mapping
        ) acc ON (((ksfw.customer_code):: text = (acc.code):: text))
        ) 
        LEFT JOIN (
        SELECT 
            itg_mds_msku_internal_product_mapping_ka.mothersku_name, 
            itg_mds_msku_internal_product_mapping_ka.internal_franchise_name, 
            itg_mds_msku_internal_product_mapping_ka.internal_brand_name, 
            itg_mds_msku_internal_product_mapping_ka.internal_variant_name, 
            row_number() OVER(PARTITION BY itg_mds_msku_internal_product_mapping_ka.mothersku_name ORDER BY 
                itg_mds_msku_internal_product_mapping_ka.internal_franchise_name, 
                itg_mds_msku_internal_product_mapping_ka.internal_brand_name, 
                itg_mds_msku_internal_product_mapping_ka.internal_variant_name
            ) AS rn 
        FROM 
            itg_mds_msku_internal_product_mapping_ka
        ) mskuipm ON ((((pd.mothersku_name):: text = (mskuipm.mothersku_name):: text) AND (mskuipm.rn = 1)))
    ) 
    WHERE ((ksfw.retailer_code IS NOT NULL) AND (trim((ksfw.retailer_code):: text) <> ('' :: character varying):: text))

)
select * from final