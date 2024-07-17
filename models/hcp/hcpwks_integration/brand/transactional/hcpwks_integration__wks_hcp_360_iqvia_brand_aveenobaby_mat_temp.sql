with wks_hcp_360_iqvia_brand_aveenobaby_temp as
(
    select * from DEV_DNA_CORE.VSHRIR01_WORKSPACE.HCPWKS_INTEGRATION__WKS_HCP_360_IQVIA_BRAND_AVEENOBABY_TEMP
    --snapindwks_integration.wks_hcp_360_iqvia_brand_aveenobaby_temp
),
final as
(
    SELECT base.country,
    base. source_system,
    base. brand,
    base. report_brand_reference,
    base. iqvia_brand,
    base. product_description,
    base. pack_description,
    base. region, 
    base. pack_volume,
    base.activty_date as activity_till_date,
    base.activty_date -360 as activity_from_date ,
    sum(derived.NoofPrescritions) as MAT_NoofPrescritions,
    avg(derived.NoofPrescribers) as MAT_NoofPrescribers
    FROM wks_hcp_360_iqvia_brand_aveenobaby_temp base,
        (SELECT  country,
        source_system,
        brand,
        report_brand_reference,
        iqvia_brand,
        product_description,
        pack_description,
        region,
        pack_volume,
        activty_date,
        NoofPrescritions,
        NoofPrescribers
        FROM wks_hcp_360_iqvia_brand_aveenobaby_temp  
    ) derived
    WHERE base.country = derived.country
    AND   base.source_system = derived.source_system
    AND   base.brand = derived.brand
    AND   base.report_brand_reference = derived.report_brand_reference
    AND   base.iqvia_brand = derived.iqvia_brand
    AND   base.region = derived.region
    AND   base.Product_Description = derived.Product_Description
    AND   base.Pack_Description = derived.Pack_Description
    AND   base.pack_volume = derived.pack_volume
    AND   derived. activty_date  between activity_from_date  and activity_till_date
    GROUP BY base.country,
    base. source_system,
    base. brand,
    base. report_brand_reference,
    base. iqvia_brand,
    base. product_description,
    base. pack_description,
    base. region, 
    base. pack_volume,
    base.activty_date,
    base.activty_date -360
)
select * from final