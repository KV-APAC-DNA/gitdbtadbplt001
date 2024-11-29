with claimable as (
    select * from {{ ref('indwks_integration__wks_scheme_vs_sales_claimable') }}

),

scheme_vs_sales_actuals as (
    select * from {{ ref('indwks_integration__wks_scheme_vs_sales_actuals') }}
),

scheme_vs_sales_rtl_dim as (
    select * from {{ ref('indwks_integration__wks_scheme_vs_sales_rtl_dim') }}
),

salescube_master as (
    select *
    from {{ ref('indwks_integration__wks_scheme_vs_sales_salescube_master') }}
),

final as (
    select
        sch.year,
        sch.month,
        sch.rtruniquecode,
        sch.mothersku_name,
        -- sch.region_name AS scheme_region_name, 
        -- sch.zone_name AS scheme_zone_name, 
        -- sch.territory_name AS scheme_territory_name, 
        -- sch.channel_name AS scheme_channel_name, 
        -- sch.class_desc AS scheme_class_desc,
        sch.franchise_name,
        sch.brand_name,
        sch.variant_name,
        sch.schemeutilizedqty,
        sch.schemeutilizedamt,
        sal.quantity,
        sal.achievement_nr,
        current_timestamp as load_dttm,
        coalesce(rd.retailer_name, sd.retailer_name) as retailer_name,
        coalesce(rd.customer_code, sd.latest_customer_code) as customer_code,
        coalesce(rd.customer_name, sd.customer_name) as customer_name,
        coalesce(rd.region_name, sd.region_name) as region_name,
        coalesce(rd.zone_name, sd.zone_name) as zone_name,
        coalesce(rd.territory_name, sd.territory_name) as territory_name,
        coalesce(rd.channel_name, sd.channel_name) as channel_name,
        coalesce(rd.class_desc, sd.class_desc) as class_desc,
        coalesce(rd.retailer_category_name, sd.retailer_category_name)
            as retailer_category_name,
        coalesce(rd.retailer_channel_level1, sd.retailer_channel_1)
            as retailer_channel_1,
        coalesce(rd.retailer_channel_level2, sd.retailer_channel_2)
            as retailer_channel_2,
        coalesce(rd.retailer_channel_level3, sd.retailer_channel_3)
            as retailer_channel_3
    from claimable as sch
    left join scheme_vs_sales_actuals as sal
        on
            sch.year = sal.year
            and sch.month = sal.month
            and sch.rtruniquecode = sal.rtruniquecode
            and upper(sch.mothersku_name) = upper(sal.mothersku_name)
    left join (
        select *
        from scheme_vs_sales_rtl_dim
    ) as rd
        on sch.rtruniquecode = rd.rtruniquecode
    left join (
        select *
        from salescube_master
    ) as sd
        on sch.rtruniquecode = sd.rtruniquecode
)

select * from final
