with wks_cockpit_billing_stock_values as
(
    select * from {{ ref('indwks_integration__wks_cockpit_billing_stock_values') }}
),
wks_cockpit_targets as
(
    select * from {{ ref('indwks_integration__wks_cockpit_targets') }}
),
edw_retailer_calendar_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
u1 as
(
    SELECT
        Dataset,
        mnth_mm,
        CAST(week AS INTEGER) as week,
        region_name,
        zone_name,
        territory_name,
        customer_code,
        customer_type,
        retailer_code,
        customer_name,
        retailer_name,
        channel_name,
        brand_name,
        franchise_name as franchise,
        cl_stck_value as closing_stock,
        billing_value,
        NULL AS measure_type,
        NULL AS brand_focus_target,
        NULL AS business_plan_target,
        mothersku_name,
        variant_name as variant
    FROM wks_cockpit_billing_stock_values
),
u2 as
(
    SELECT
        Dataset,
        CAST(TO_CHAR(year_month, 'YYYYMM') AS INTEGER),
        cal.week,
        region,
        zone,
        territory,
        NULL AS customer_code,
        NULL AS customer_type,
        NULL AS retailer_code,
        NULL AS customer_name,
        NULL AS retailer_name,
        channel,
        brand,
        franchise,
        NULL AS closing_stock,
        NULL AS billing_value,
        measure_type,
        brand_focus_target,
        business_plan_target,
        NULL AS mothersku_name,
        variant
FROM wks_cockpit_targets tgt,
     (SELECT DISTINCT week,
             mth_mm
      FROM edw_retailer_calendar_dim) cal
WHERE CAST(TO_CHAR(year_month,'YYYYMM') AS INTEGER) = cal.mth_mm
),
transformed as 
(
    select * from u1
    union all
    select * from u2
),
final as 
(   
    select
        dataset::varchar(50) as dataset,
        mnth_mm::number(18,0) as mth_mm,
        week::number(18,0) as week,
        region_name::varchar(75) as region_name,
        zone_name::varchar(75) as zone_name,
        territory_name::varchar(75) as territory_name,
        customer_code::varchar(50) as customer_code,
        customer_type::varchar(50) as customer_type,
        retailer_code::varchar(100) as retailer_code,
        customer_name::varchar(150) as customer_name,
        retailer_name::varchar(150) as retailer_name,
        channel_name::varchar(150) as channel_name,
        brand_name::varchar(50) as brand_name,
        franchise::varchar(50) as franchise,
        closing_stock::number(38,2) as closing_stock,
        billing_value::number(38,4) as billing_value,
        measure_type::varchar(50) as measure_type,
        brand_focus_target::number(38,2) as brand_focus_target,
        business_plan_target::number(38,2) as business_plan_target,
        mothersku_name::varchar(150) as mothersku_name,
        variant::varchar(150) as variant
    from transformed
)
select * from final
