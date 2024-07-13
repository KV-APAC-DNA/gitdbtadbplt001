with edw_retailer_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_dim') }}
),
itg_schemeutilization as
(
    select * from {{ ref('inditg_integration__itg_schemeutilization') }}
),
edw_retailer_calendar_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
itg_scheme_header as
(
    select * from {{ ref('inditg_integration__itg_scheme_header') }}
),
itg_tbl_schemewise_apno as
(
    select * from {{ ref('inditg_integration__itg_tbl_schemewise_apno') }}
),
edw_product_dim as
(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
v_retailer_udc_map as
(
    select * from {{ ref('indedw_integration__v_retailer_udc_map') }}
),
derived_table1 as
(
    SELECT          
                    edw_retailer_dim.retailer_code,
                    edw_retailer_dim.start_date,
                    edw_retailer_dim.end_date,
                    edw_retailer_dim.customer_code,
                    edw_retailer_dim.customer_name,
                    edw_retailer_dim.retailer_name,
                    edw_retailer_dim.retailer_address1,
                    edw_retailer_dim.retailer_address2,
                    edw_retailer_dim.retailer_address3,
                    edw_retailer_dim.region_code,
                    edw_retailer_dim.region_name,
                    edw_retailer_dim.zone_code,
                    edw_retailer_dim.zone_name,
                    edw_retailer_dim.zone_classification,
                    edw_retailer_dim.territory_code,
                    edw_retailer_dim.territory_name,
                    edw_retailer_dim.territory_classification,
                    edw_retailer_dim.state_code,
                    edw_retailer_dim.state_name,
                    edw_retailer_dim.town_code,
                    edw_retailer_dim.town_name,
                    edw_retailer_dim.town_classification,
                    edw_retailer_dim.class_code,
                    edw_retailer_dim.class_desc,
                    edw_retailer_dim.outlet_type,
                    edw_retailer_dim.channel_code,
                    edw_retailer_dim.channel_name,
                    edw_retailer_dim.business_channel,
                    edw_retailer_dim.loyalty_desc,
                    edw_retailer_dim.registration_date,
                    edw_retailer_dim.status_cd,
                    edw_retailer_dim.status_desc,
                    edw_retailer_dim.csrtrcode,
                    edw_retailer_dim.crt_dttm,
                    edw_retailer_dim.updt_dttm,
                    edw_retailer_dim.actv_flg,
                    edw_retailer_dim.retailer_category_cd,
                    edw_retailer_dim.retailer_category_name,
                    edw_retailer_dim.rtrlatitude,
                    edw_retailer_dim.rtrlongitude,
                    edw_retailer_dim.rtruniquecode,
                    edw_retailer_dim.createddate,
                    edw_retailer_dim.file_rec_dt,
                    edw_retailer_dim.type_name,
                    row_number() OVER (
                        PARTITION BY edw_retailer_dim.customer_code,
                        edw_retailer_dim.retailer_code
                        ORDER BY 1
                        ) AS rn
                FROM edw_retailer_dim
                WHERE (
                        edw_retailer_dim.file_rec_dt = (
                            SELECT max (edw_retailer_dim.file_rec_dt) AS max
                            FROM edw_retailer_dim
                            )
                        )
),
rd as 
(
     SELECT derived_table1.retailer_code,
                derived_table1.start_date,
                derived_table1.end_date,
                derived_table1.customer_code,
                derived_table1.customer_name,
                derived_table1.retailer_name,
                derived_table1.retailer_address1,
                derived_table1.retailer_address2,
                derived_table1.retailer_address3,
                derived_table1.region_code,
                derived_table1.region_name,
                derived_table1.zone_code,
                derived_table1.zone_name,
                derived_table1.zone_classification,
                derived_table1.territory_code,
                derived_table1.territory_name,
                derived_table1.territory_classification,
                derived_table1.state_code,
                derived_table1.state_name,
                derived_table1.town_code,
                derived_table1.town_name,
                derived_table1.town_classification,
                derived_table1.class_code,
                derived_table1.class_desc,
                derived_table1.outlet_type,
                derived_table1.channel_code,
                derived_table1.channel_name,
                derived_table1.business_channel,
                derived_table1.loyalty_desc,
                derived_table1.registration_date,
                derived_table1.status_cd,
                derived_table1.status_desc,
                derived_table1.csrtrcode,
                derived_table1.crt_dttm,
                derived_table1.updt_dttm,
                derived_table1.actv_flg,
                derived_table1.retailer_category_cd,
                derived_table1.retailer_category_name,
                derived_table1.rtrlatitude,
                derived_table1.rtrlongitude,
                derived_table1.rtruniquecode,
                derived_table1.createddate,
                derived_table1.file_rec_dt,
                derived_table1.type_name,
                derived_table1.rn
            FROM derived_table1
            WHERE (derived_table1.rn = 1)
),
final as
(
SELECT su.schdate,
    cal.day,
    cal.week,
    cal.qtr,
    cal.mth_yyyymm,
    cal.month_nm,
    cal.fisc_yr,
    su.billedprdccode,
    su.billedprdbatcode,
    CASE 
        WHEN (pd.product_name IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE pd.product_name
        END AS product_name,
    CASE 
        WHEN (pd.franchise_name IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE pd.franchise_name
        END AS franchise_name,
    CASE 
        WHEN (pd.brand_name IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE pd.brand_name
        END AS brand_name,
    CASE 
        WHEN (pd.product_category_name IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE pd.product_category_name
        END AS product_category_name,
    CASE 
        WHEN (pd.variant_name IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE pd.variant_name
        END AS variant_name,
    CASE 
        WHEN (pd.mothersku_name IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE pd.mothersku_name
        END AS mothersku_name,
    su.invoiceno,
    su.distcode,
    CASE 
        WHEN (rd.customer_name IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.customer_name
        END AS customer_name,
    su.schemecode,
    CASE 
        WHEN (su.schemedescription IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE su.schemedescription
        END AS schemedescription,
    sh.schtype,
    su.schemetype,
    sh.schvalidfrom,
    sh.schvalidtill,
    CASE 
        WHEN (sh.claimable = 1)
            THEN 'Yes'::CHARACTER VARYING
        WHEN (sh.claimable = 0)
            THEN 'No'::CHARACTER VARYING
        ELSE 'Unknown'::CHARACTER VARYING
        END AS claimable,
    su.rtrcode,
    CASE 
        WHEN (rd.retailer_name IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE (rd.retailer_name)
        END AS retailer_name,
    CASE 
        WHEN (rd.retailer_address1 IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.retailer_address1
        END AS retailer_address1,
    CASE 
        WHEN (rd.retailer_address2 IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.retailer_address2
        END AS retailer_address2,
    CASE 
        WHEN (rd.retailer_address3 IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.retailer_address3
        END AS retailer_address3,
    rd.region_code,
    CASE 
        WHEN (rd.region_name IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.region_name
        END AS region_name,
    rd.zone_code,
    CASE 
        WHEN (rd.zone_name IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.zone_name
        END AS zone_name,
    CASE 
        WHEN (rd.zone_classification IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.zone_classification
        END AS zone_classification,
    rd.territory_code,
    CASE 
        WHEN (rd.territory_name IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.territory_name
        END AS territory_name,
    CASE 
        WHEN (rd.territory_classification IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.territory_classification
        END AS territory_classification,
    rd.state_code,
    CASE 
        WHEN (rd.state_name IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.state_name
        END AS state_name,
    rd.town_code,
    CASE 
        WHEN (rd.town_name IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.town_name
        END AS town_name,
    CASE 
        WHEN (rd.town_classification IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.town_classification
        END AS town_classification,
    rd.class_code,
    CASE 
        WHEN (rd.class_desc IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.class_desc
        END AS class_desc,
    CASE 
        WHEN (rd.outlet_type IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.outlet_type
        END AS outlet_type,
    rd.channel_code,
    CASE 
        WHEN (rd.channel_name IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.channel_name
        END AS channel_name,
    CASE 
        WHEN (rd.business_channel IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.business_channel
        END AS business_channel,
    CASE 
        WHEN (rd.loyalty_desc IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.loyalty_desc
        END AS loyalty_desc,
    CASE 
        WHEN (rd.status_desc IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.status_desc
        END AS status_desc,
    rd.csrtrcode,
    rd.retailer_category_cd,
    CASE 
        WHEN (rd.retailer_category_name IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.retailer_category_name
        END AS retailer_category_name,
    rd.rtrlatitude,
    rd.rtrlongitude,
    rd.rtruniquecode,
    su.schemeutilizedamt,
    su.schemefreeproduct,
    su.schemeutilizedqty,
    su.companyschemecode,
    su.createddate,
    su.migrationflag,
    su.schememode,
    su.schlinecount,
    su.schvaluetype,
    su.slabid,
    su.billedqty,
    su.schdiscperc,
    su.freeprdbatcode,
    su.billedrate,
    su.modifieddate,
    su.servicecrnrefno,
    su.rtrurccode,
    sw.schcategorytype1code AS schemetype1,
    sw.schcategorytype2code AS schemetype2,
    CASE 
        WHEN (rd.type_name IS NULL)
            THEN 'Unknown'::CHARACTER VARYING
        ELSE rd.type_name
        END AS type_name,
    udc.udc_hsacounter,
    udc.udc_keyaccountname,
    udc.udc_pharmacychain,
    udc.udc_onedetailingbaby,
    udc.udc_sanprovisibilitysss,
    udc.udc_sssconsoffer,
    udc.udc_platinumclub2018,
    udc.udc_sssendcaps,
    udc.udc_platinumclub2019,
    udc.udc_signature2019,
    udc.udc_premium2019,
    udc.udc_gstn,
    udc.udc_platinumq22019new,
    udc.udc_sssprogram2019,
    udc.udc_umangq32019,
    udc.udc_sssscheme2019,
    udc.udc_ssspromoter2019,
    udc.udc_rtrtypeattr,
    udc.udc_bhagidariq32019,
    udc.udc_directorclubq32019,
    udc.udc_platinumq32019new,
    udc.udc_platinumq22019,
    udc.udc_platinumq32019,
    udc.udc_bbastore,
    udc.udc_avbabybodydocq42019,
    udc.udc_orslcac2019,
    udc.udc_babyprofesionalcac2019,
    udc.udc_platinumq42019,
    udc.udc_schemesss2020,
    udc.udc_platinumq12020,
    udc.udc_platinumq32020,
    udc.udc_sssq32020,
    udc.udc_bhagidariq32020,
    udc.udc_directorclubq32020,
    udc.udc_umangq32020,
    udc.udc_daudq32020,
    udc.udc_sssprogramq22021,
    udc.udc_newgtm,
    udc.udc_sssprogramq12022,
    udc.udc_ssspharmacystore,
    udc.udc_ssstotstores,
    udc.udc_platinumq12022,
    udc.udc_ecommerce,
    udc.udc_babytopstoreactivation,
    udc.udc_platinumq32022,
    udc.udc_hsacounterq32022,
    udc.udc_platinumq42022,
    udc.udc_sssprogramq42022,
    udc.udc_sssprogramq12023,
    udc.udc_hsacounterq12023,
    udc.udc_platinumq12023,
    udc.udc_aarogyam,
    udc.udc_sssprogramq22023,
    udc.udc_ssshyperstores2023,
    udc.udc_winbirth2023,
    udc.udc_winclinic2023,
    udc.udc_hsacounterq22023,
    udc.udc_hsacounterq32023,
    udc.udc_aveenosssstores,
    udc.udc_hsacounterq42023,
    udc.udc_hsacounterq12024,
    udc.udc_q124bssprogram,
    udc.udc_specialtyprofessional2024
FROM (
    (
        (
            (
                (
                    (
                        itg_schemeutilization su LEFT JOIN edw_retailer_calendar_dim cal ON ((su.schdate = cal.caldate))
                        ) LEFT JOIN itg_scheme_header sh ON (((su.companyschemecode)::TEXT = (sh.cmpschcode)::TEXT))
                    ) LEFT JOIN itg_tbl_schemewise_apno sw ON ((sh.schid = sw.schid))
                ) LEFT JOIN edw_product_dim pd ON (((su.billedprdccode)::TEXT = (pd.product_code)::TEXT))
            ) LEFT JOIN rd ON (((trim(su.distcode)::TEXT || trim(su.rtrcode)::TEXT) = (trim(rd.customer_code)::TEXT || trim(rd.retailer_code)::TEXT)))
        ) LEFT JOIN v_retailer_udc_map udc ON (
            (
                (trim(su.distcode)::TEXT = trim(udc.customer_code_udc)::TEXT)
                AND (trim(su.rtrcode)::TEXT = trim(udc.retailer_code_udc)::TEXT)
                )
            )
    )
WHERE ((cal.fisc_yr)::DOUBLE PRECISION >= (DATE_PART(YEAR,current_timestamp()))-2)
)
select * from final
