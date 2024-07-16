with itg_hcp360_in_iqvia_brand as
(
    select * from dev_dna_core.snapinditg_integration.itg_hcp360_in_iqvia_brand
),
itg_mds_hcp360_product_mapping as
(
    select * from dev_dna_core.snapinditg_integration.itg_mds_hcp360_product_mapping
),
iqvia as
(
    SELECT 'JBABY' AS Brand,
        nvl(M.Brand, CASE 
                WHEN position(' ' IN I.Product_description) > 0
                    THEN split_part(I.Product_description, ' ', 1)
                WHEN position('-' IN I.Product_description) > 0
                    THEN split_part(I.Product_description, '-', 1)
                ELSE I.Product_description
                END || '_COMP') AS Report_Brand,
        product_description,
        pack_description,
        pack_form,
        brand_category,
        zone,
        year_month,
        no_of_prescriptions,
        no_of_prescribers AS no_of_prescribers,
        pack_volume
    FROM ITG_HCP360_IN_IQVIA_BRAND I,
        (
            SELECT *
            FROM ITG_MDS_HCP360_PRODUCT_MAPPING
            WHERE Brand IN ('JBABY')
            ) M
    WHERE I.PACK_DESCRIPTION = M.IQVIA(+)
        AND I.data_source = 'Aveeno_baby'
        AND split_part(I.Product_description, ' ', 1) != 'AVEENO'
        AND upper(pack_description) LIKE '%BABY%'
),
transformed as 
(
SELECT 'IN' AS COUNTRY,
    'IQVIA' AS SOURCE_SYSTEM,
    'JBABY' AS BRAND,
    brand_category AS BRAND_CATEGORY,
    Report_Brand AS REPORT_BRAND_REFERENCE
    --,case when Report_Brand like'%COMP' then NULL else 'JBABY Total' end AS IQVIA_BRAND	
    ,
    'JBABY Total' AS IQVIA_BRAND,
    product_description,
    pack_description,
    pack_form,
    pack_volume,
    ZONE AS REGION,
    cast(Year_month AS DATE) AS ACTIVTY_DATE,
    no_of_prescriptions AS NoofPrescritions,
    no_of_prescribers AS NoofPrescribers
FROM iqvia
),
final as
(
    select
        country::varchar(2) as country,
        source_system::varchar(5) as source_system,
        brand::varchar(5) as brand,
        brand_category::varchar(50) as brand_category,
        report_brand_reference::varchar(200) as report_brand_reference,
        iqvia_brand::varchar(11) as iqvia_brand,
        product_description::varchar(50) as product_description,
        pack_description::varchar(100) as pack_description,
        pack_form::varchar(50) as pack_form,
        pack_volume::varchar(20) as pack_volume,
        region::varchar(20) as region,
        activty_date::date as activty_date,
        noofprescritions::number(18,5) as noofprescritions,
        noofprescribers::number(18,5) as noofprescribers
    from transformed
)
select * from final
