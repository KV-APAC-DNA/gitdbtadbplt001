with itg_brand_focus_target as
(
    select * from {{ source('inditg_integration', 'itg_brand_focus_target') }}
),
itg_business_plan_target as
(
    select * from {{ source('inditg_integration', 'itg_business_plan_target') }}
),
cte as
(
    SELECT 
        'BrandFocusTarget' AS Dataset,
        Year_Month,
        UPPER(Region) AS Region,
        UPPER(Zone) AS Zone,
        NULL AS Territory,
        Franchise,
        Brand,
        Variant,
        Measure_Type,
        Channel,
        Target_value AS brand_Focus_Target,
        NULL AS Business_Plan_Target
    FROM itg_brand_focus_target
),
cte1 as
(
    SELECT 
        'BusinessPlanTarget' AS dataset,
        yearmonth,
        UPPER(Region),
        UPPER(Zone),
        UPPER(Territory),
        NULL AS Franchise,
        NULL AS Brand,
        NULL AS Variant,
        NULL AS Measure_Type,
        NULL AS Channel,
        NULL AS Brand_Focus_Target,
        value*1000 AS Business_Plan_Target
    FROM itg_business_plan_target
),
transformed as 
(
    select * from cte
    union all
    select * from cte1
),
final as
(
    select
        dataset::varchar(18) as dataset,
        year_month::date as year_month,
        region::varchar(75) as region,
        zone::varchar(75) as zone,
        territory::varchar(75) as territory,
        franchise::varchar(50) as franchise,
        brand::varchar(50) as brand,
        variant::varchar(50) as variant,
        measure_type::varchar(50) as measure_type,
        channel::varchar(50) as channel,
        brand_focus_target::number(38,2) as brand_focus_target,
        business_plan_target::number(38,2) as business_plan_target
    from transformed
)
select * from final
