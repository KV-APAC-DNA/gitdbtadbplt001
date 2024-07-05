{{
    config(
        materialized='view'
    )
}}

with edw_dailysales_fact as
(
    select * from {{ ref('indedw_integration__edw_dailysales_fact') }}
),
edw_retailer_calendar_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
edw_customer_dim as
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
v_retail_frnch_categ_mapping as
(
    select * from {{ ref('indedw_integration__v_retail_frnch_categ_mapping') }}
),
v_retail_fran_chanl as
(
    select * from {{ ref('indedw_integration__v_retail_fran_chanl') }}
),
itg_mds_msku_internal_product_mapping_ka as
(
    select * from {{ source('inditg_integration', 'itg_mds_msku_internal_product_mapping_ka') }}
),
itg_mds_in_key_accounts_mapping as
(
    select * from {{ ref('inditg_integration__itg_mds_in_key_accounts_mapping') }}
),
edw_billing_fact as
(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),
final as
(
    SELECT derived_table3.fisc_yr,
        derived_table3.fisc_mnth,
        derived_table3.fisc_wk,
        derived_table3.region_name,
        derived_table3.zone_name,
        derived_table3.territory_name,
        derived_table3.channel,
        derived_table3.channel_name,
        derived_table3.retailer_channel_level1,
        derived_table3.retailer_channel_level2,
        derived_table3.retailer_channel_level3,
        derived_table3.customer_code,
        derived_table3.customer_name,
        derived_table3.product_category1,
        derived_table3.product_category2,
        derived_table3.product_category3,
        derived_table3.product_category4,
        derived_table3.franchise_name, derived_table3.brand_name, derived_table3.variant_name, derived_table3.mothersku_name,  
        derived_table3.datasource,
        derived_table3.achievement_nr AS sls_actl_val,
        derived_table3.Internal_Franchise_Name, derived_table3.Internal_Brand_Name, derived_table3.Internal_Variant_Name
    FROM (SELECT sf_master.fisc_yr,
                sf_master.mth_yyyymm AS fisc_mnth,
                sf_master.week AS fisc_wk,
                sf_master.region_name,
                sf_master.zone_name,
                sf_master.territory_name,
                sf_master.product_category1,
                sf_master.product_category2,
                sf_master.product_category3,
                sf_master.product_category4,
                sf_master.channel,
                sf_master.channel_name,
                sf_master.retailer_channel_level1,
                sf_master.retailer_channel_level2,
                sf_master.retailer_channel_level3,
                sf_master.customer_code,
                sf_master.customer_name,
                sf_master.franchise_name, sf_master.brand_name, sf_master.variant_name,sf_master.mothersku_name,'Sales' as datasource,
                SUM(sf_master.achievement_nr) AS achievement_nr,
                sf_master.Internal_Franchise_Name, sf_master.Internal_Brand_Name, sf_master.Internal_Variant_Name
        FROM   ( SELECT cal.fisc_yr,
                        cal.mth_yyyymm,
                        cal.week,
                        cd.region_name,
                        cd.zone_name,
                        cd.territory_name,
                        sf.customer_code,
                        rd.customer_name,
                        rd.channel_name,
                        rd.retailer_channel_level1,
                        rd.retailer_channel_level2,
                        rd.retailer_channel_level3,
                        rd.retailer_category_name,
                        rd.class_desc,
                        cd.territory_classification,
                        pd.product_category_name,
                        pd.franchise_name, pd.brand_name, pd.variant_name, pd.mothersku_name,
                        nvl(nullif(pd.product_category1,''),'Missing Product Category1') as product_category1,
                        nvl(nullif(pd.product_category2,''),'Missing Product Category2') as product_category2,
                        nvl(nullif(pd.product_category3,''),'Missing Product Category3') as product_category3,
                        nvl(nullif(pd.product_category4,''),'Missing Product Category4') as product_category4,
                        nvl(nullif(rd.report_channel,''),'Missing Channel Mapping') AS channel,
                        sf.achievement_nr,
                        CASE
                                WHEN MSKUIPM.Internal_Franchise_Name IS NULL OR trim(MSKUIPM.Internal_Franchise_Name::text) = ''::character varying::text THEN 'Unknown'::character varying
                        ELSE MSKUIPM.Internal_Franchise_Name
                            END AS Internal_Franchise_Name,
                        CASE
                            WHEN MSKUIPM.Internal_Brand_Name IS NULL OR trim(MSKUIPM.Internal_Brand_Name::text) = ''::character varying::text THEN 'Unknown'::character varying
                            ELSE MSKUIPM.Internal_Brand_Name
                        END AS Internal_Brand_Name,
                        CASE
                            WHEN MSKUIPM.Internal_Variant_Name IS NULL OR trim(MSKUIPM.Internal_Variant_Name::text) = ''::character varying::text THEN 'Unknown'::character varying
                            ELSE MSKUIPM.Internal_Variant_Name
                        END AS Internal_Variant_Name
                    FROM 
                        (SELECT edw_dailysales_fact.customer_code,
                                edw_dailysales_fact.invoice_no,
                                edw_dailysales_fact.invoice_date,
                                edw_dailysales_fact.retailer_code,
                                edw_dailysales_fact.product_code,
                                edw_dailysales_fact.retailer_name,
                                edw_dailysales_fact.achievement_nr_val AS achievement_nr
                            FROM  edw_dailysales_fact) sf
                        LEFT JOIN  edw_retailer_calendar_dim cal ON sf.invoice_date = cal.day
                        LEFT JOIN  edw_customer_dim cd ON sf.customer_code::TEXT = cd.customer_code::TEXT
                        LEFT JOIN v_retail_frnch_categ_mapping pd ON sf.product_code::TEXT = pd.product_code::TEXT
                        LEFT JOIN v_retail_fran_chanl rd
                            ON sf.retailer_code::TEXT = rd.retailer_code::TEXT
                            AND sf.customer_code::TEXT = rd.customer_code::TEXT
                            --AND sf.retailer_name::TEXT = rd.retailer_name::TEXT
                        LEFT JOIN ( --Retrive only the First appearance of each mothersku_name from itg_mds_msku_internal_product_mapping_ka
                                    SELECT *,
                                        ROW_NUMBER() OVER (PARTITION BY mothersku_name ORDER BY Internal_Franchise_Name, Internal_Brand_Name, Internal_Variant_Name) AS rn
                                    FROM itg_mds_msku_internal_product_mapping_ka
                                    ) AS MSKUIPM
                                ON pd.mothersku_name::text = MSKUIPM.mothersku_name::text 
                                AND MSKUIPM.rn = 1 
                        ) sf_master 
            
        WHERE sf_master.fisc_yr::DOUBLE precision >= (date_part(year,convert_timezone('UTC',current_timestamp())::date) - 2::DOUBLE precision)
        GROUP BY sf_master.fisc_yr,
                sf_master.mth_yyyymm,
                sf_master.week,
                sf_master.region_name,
                sf_master.zone_name,
                sf_master.territory_name,
                sf_master.product_category1,
                sf_master.product_category2,
                sf_master.product_category3,
                sf_master.product_category4,
                sf_master.channel,
                sf_master.channel_name,
                sf_master.retailer_channel_level1,
                sf_master.retailer_channel_level2,
                sf_master.retailer_channel_level3,
                sf_master.customer_code,
                sf_master.customer_name,
                sf_master.franchise_name, sf_master.brand_name, sf_master.variant_name, sf_master.mothersku_name,
                sf_master.Internal_Franchise_Name, sf_master.Internal_Brand_Name, sf_master.Internal_Variant_Name
        UNION ALL
        SELECT cal.fisc_yr,
                cal.mth_yyyymm AS fisc_mnth,
                cal.week AS fisc_wk,
                cd.region_name,
                cd.zone_name,
                cd.territory_name,
                nvl(nullif(pd.product_category1,''),'Missing Product Category1') as product_category1,
                nvl(nullif(pd.product_category2,''),'Missing Product Category2') as product_category2,
                nvl(nullif(pd.product_category3,''),'Missing Product Category3') as product_category3,
                nvl(nullif(pd.product_category4,''),'Missing Product Category4') as product_category4,
                CASE
                WHEN cd.type_name::TEXT = 'Cash And Carry'::CHARACTER VARYING::TEXT THEN 'Cash and Carry'::CHARACTER VARYING
                WHEN cd.type_name::TEXT = 'Key Account'::CHARACTER VARYING::TEXT THEN NVL(acc.channel_name_code, 'KAM+Ecom')::CHARACTER VARYING
                ELSE 'Others'::CHARACTER VARYING
                END AS channel,
                'NULL' AS channel_name,
                'NULL' AS retailer_channel_level1,
                'NULL' AS retailer_channel_level2,
                'NULL' AS retailer_channel_level3,
                bf.customer_code::CHARACTER VARYING AS customer_code,
                cd.customer_name,
                pd.franchise_name, pd.brand_name, pd.variant_name, pd.mothersku_name,'Invoice' as datasource,
                SUM(bf.achievement_nr) AS achievement_nr,
                CASE
                    WHEN MSKUIPM.Internal_Franchise_Name IS NULL OR trim(MSKUIPM.Internal_Franchise_Name::text) = ''::character varying::text THEN 'Unknown'::character varying
                ELSE MSKUIPM.Internal_Franchise_Name
                END AS Internal_Franchise_Name,
                CASE
                    WHEN MSKUIPM.Internal_Brand_Name IS NULL OR trim(MSKUIPM.Internal_Brand_Name::text) = ''::character varying::text THEN 'Unknown'::character varying
                    ELSE MSKUIPM.Internal_Brand_Name
                END AS Internal_Brand_Name,
                CASE
                    WHEN MSKUIPM.Internal_Variant_Name IS NULL OR trim(MSKUIPM.Internal_Variant_Name::text) = ''::character varying::text THEN 'Unknown'::character varying
                    ELSE MSKUIPM.Internal_Variant_Name
                END AS Internal_Variant_Name
        FROM (SELECT derived_table2.customer_code,
                    derived_table2.invoice_code,
                    derived_table2.invoice_date,
                    derived_table2.product_code,
                    derived_table2.achievement_nr
                FROM (SELECT LTRIM(edw_billing_fact.sold_to::TEXT,0::CHARACTER VARYING::TEXT) AS customer_code,
                            edw_billing_fact.bill_num AS invoice_code,
                            TO_CHAR(edw_billing_fact.created_on::TIMESTAMP WITHOUT TIME ZONE,'YYYYMMDD'::CHARACTER VARYING::TEXT)::INTEGER AS invoice_date,
                            LTRIM(edw_billing_fact.material::TEXT,0::CHARACTER VARYING::TEXT) AS product_code,
                            edw_billing_fact.subtotal_4 AS achievement_nr
                    FROM edw_billing_fact
                    WHERE edw_billing_fact.sls_org::TEXT = '5100'::CHARACTER VARYING::TEXT
                    AND   (edw_billing_fact.bill_type::TEXT LIKE ANY ('S1','S2','ZC22','ZC2D','ZF2D','ZG22','ZG2D','ZL2D','ZL22','ZRSM','ZSMD','ZF2E','ZG3D','ZC3D','ZL3D'))) derived_table2) bf
            LEFT JOIN  edw_retailer_calendar_dim cal ON bf.invoice_date = cal.day
            LEFT JOIN (SELECT edw_customer_dim.customer_code,
                            edw_customer_dim.region_code,
                            edw_customer_dim.region_name,
                            edw_customer_dim.zone_code,
                            edw_customer_dim.zone_name,
                            edw_customer_dim.territory_code,
                            edw_customer_dim.territory_name,
                            edw_customer_dim.type_code,
                            edw_customer_dim.type_name,
                            edw_customer_dim.customer_name
                    FROM  edw_customer_dim
                    WHERE edw_customer_dim.type_code = 4::NUMERIC::NUMERIC(18,0)
                    OR    edw_customer_dim.type_code = 7::NUMERIC::NUMERIC(18,0)) cd ON bf.customer_code = cd.customer_code::TEXT
            LEFT JOIN v_retail_frnch_categ_mapping pd ON bf.product_code = pd.product_code::TEXT
            LEFT JOIN itg_mds_in_key_accounts_mapping acc
                ON bf.customer_code::TEXT = acc.code::TEXT
            LEFT JOIN ( --Retrive only the First appearance of each mothersku_name from itg_mds_msku_internal_product_mapping_ka
                                    SELECT *,
                                        ROW_NUMBER() OVER (PARTITION BY mothersku_name ORDER BY Internal_Franchise_Name, Internal_Brand_Name, Internal_Variant_Name) AS rn
                                    FROM itg_mds_msku_internal_product_mapping_ka
                                    ) AS MSKUIPM
                                ON pd.mothersku_name::text = MSKUIPM.mothersku_name::text 
                                AND MSKUIPM.rn = 1 
        WHERE cal.fisc_yr::DOUBLE precision >= (date_part(year,convert_timezone('UTC',current_timestamp())::date) - 2::DOUBLE precision)
        GROUP BY cal.fisc_yr,
                cal.mth_yyyymm,
                cal.week,
                cd.region_name,
                cd.zone_name,
                cd.territory_name,
                CASE
                WHEN cd.type_name::TEXT = 'Cash And Carry'::CHARACTER VARYING::TEXT THEN 'Cash and Carry'::CHARACTER VARYING
                WHEN cd.type_name::TEXT = 'Key Account'::CHARACTER VARYING::TEXT THEN NVL(acc.channel_name_code, 'KAM+Ecom')::CHARACTER VARYING
                ELSE 'Others'::CHARACTER VARYING
                END,
                nvl(nullif(pd.product_category1,''),'Missing Product Category1') ,
                nvl(nullif(pd.product_category2,''),'Missing Product Category2') ,
                nvl(nullif(pd.product_category3,''),'Missing Product Category3') ,
                nvl(nullif(pd.product_category4,''),'Missing Product Category4') ,
                bf.customer_code,
                cd.customer_name,
                pd.franchise_name, pd.brand_name, pd.variant_name, pd.mothersku_name,
                MSKUIPM.Internal_Franchise_Name, MSKUIPM.Internal_Brand_Name, MSKUIPM.Internal_Variant_Name
    ) derived_table3
)
select * from final