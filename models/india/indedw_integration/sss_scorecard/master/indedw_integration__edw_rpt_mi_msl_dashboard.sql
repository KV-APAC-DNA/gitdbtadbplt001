with wks_mi_msl_sales_vs_reco_tbl as(
    select * from {{ ref('indwks_integration__wks_mi_msl_sales_vs_reco_tbl') }}
),
wks_mi_msl_agg_tbl as(
    select * from {{ ref('indwks_integration__wks_mi_msl_agg_tbl) }}
),
wks_mi_msl_reco_vs_sold as(
    select * from {{ ref('indwks_integration__wks_mi_msl_reco_vs_sold') }}
),
union1 as(
SELECT mth_mm as mth_mm
       ,customer_code as customer_code
       ,customer_name as customer_name
       ,retailer_code as retailer_code
       ,retailer_name as retailer_name
       ,rtruniquecode as rtruniquecode
       ,UPPER(region_name) as region_name
       ,UPPER(zone_name) as zone_name
       ,UPPER(territory_name) as territory_name
       ,channel_name as channel_name
       ,retailer_category_name as retailer_category_name
       ,unique_sales_code as unique_sales_code
       ,salesman_code as salesman_code
       ,salesman_name as salesman_name
       ,mothersku_code_recom as mothersku_code_recom
       ,mothersku_name_recom as mothersku_name_recom
       ,mothersku_code_sold as mothersku_code_sold
       ,mothersku_name_sold as mothersku_name_sold
       ,quantity as quantity
       ,achievement_nr as achievement_nr
       ,total_subd as total_subd
       ,qtr as qtr
       ,period as period
       ,msl_count as msl_count
       ,NULL as msl_sold_count
       ,ms_flag as ms_flag
       ,NULL as hit_ms_flag
       ,'MI_MSL_SALES_BASE' as dataset
       ,current_timestamp()::timestamp_ntz(9) as crtdtm
       ,current_timestamp()::timestamp_ntz(9) as upddtm
       ,latest_customer_code   as latest_customer_code
       ,latest_customer_name   as latest_customer_name
       ,latest_territory_code  as latest_territory_code
       ,UPPER(latest_territory_name) as latest_territory_name
       ,latest_salesman_code   as latest_salesman_code
       ,latest_salesman_name   as latest_salesman_name
       ,latest_uniquesalescode as latest_uniquesalescode
FROM  wks_mi_msl_sales_vs_reco_tbl
),
union2 as(
    SELECT mth_mm as mth_mm
       ,customer_code as customer_code
       ,customer_name as customer_name
       ,retailer_code as retailer_code
       ,retailer_name as retailer_name
       ,rtruniquecode as rtruniquecode
       ,UPPER(region_name) as region_name
       ,UPPER(zone_name) as zone_name
       ,UPPER(territory_name) as territory_name
       ,channel_name as channel_name
       ,retailer_category_name as retailer_category_name
       ,unique_sales_code as unique_sales_code
       ,salesman_code as salesman_code
       ,salesman_name as salesman_name
       ,NULL as mothersku_code_recom
       ,NULL as mothersku_name_recom
       ,NULL as mothersku_code_sold
       ,NULL as mothersku_name_sold
       ,quantity as quantity
       ,achievement_nr as achievement_nr
       ,total_subd as total_subd
       ,qtr as qtr
       ,period as period
       ,msl_count as msl_count
       ,msl_sold_count as msl_sold_count
       ,NULL as ms_flag
       ,NULL as hit_ms_flag
       ,'MI_MSL_AGG' as dataset
       ,current_timestamp()::timestamp_ntz(9) as crtdtm
       ,current_timestamp()::timestamp_ntz(9) as upddtm
       ,latest_customer_code   as latest_customer_code
       ,latest_customer_name   as latest_customer_name
       ,latest_territory_code  as latest_territory_code
       ,UPPER(latest_territory_name) as latest_territory_name
       ,latest_salesman_code   as latest_salesman_code
       ,latest_salesman_name   as latest_salesman_name
       ,latest_uniquesalescode as latest_uniquesalescode
FROM  wks_mi_msl_agg_tbl
),
union3 as(
SELECT mth_mm as mth_mm
       ,customer_code as customer_code
       ,customer_name as customer_name
       ,retailer_code as retailer_code
       ,retailer_name as retailer_name
       ,rtruniquecode as rtruniquecode
       ,UPPER(region_name) as region_name
       ,UPPER(zone_name) as zone_name
       ,UPPER(territory_name) as territory_name
       ,channel_name as channel_name
       ,retailer_category_name as retailer_category_name
       ,unique_sales_code as unique_sales_code
       ,salesman_code as salesman_code
       ,salesman_name as salesman_name
       ,mothersku_code_recom as mothersku_code_recom
       ,mothersku_name_recom as mothersku_name_recom
       ,mothersku_code_sold as mothersku_code_sold
       ,mothersku_name_sold as mothersku_name_sold
       ,quantity as quantity
       ,achievement_nr as achievement_nr
       ,total_subd as total_subd
       ,qtr as qtr
       ,period as period
       ,msl_count as msl_count
       ,msl_sold_count as msl_sold_count
       ,ms_flag as ms_flag
       ,hit_ms_flag as hit_ms_flag
       ,'MI_MSL_RECOM_BASE' as dataset
       ,current_timestamp()::timestamp_ntz(9) as crtdtm
       ,current_timestamp()::timestamp_ntz(9) as upddtm
       ,latest_customer_code   as latest_customer_code
       ,latest_customer_name   as latest_customer_name
       ,latest_territory_code  as latest_territory_code
       ,UPPER(latest_territory_name) as latest_territory_name
       ,latest_salesman_code   as latest_salesman_code
       ,latest_salesman_name   as latest_salesman_name
       ,latest_uniquesalescode as latest_uniquesalescode
FROM wks_mi_msl_reco_vs_sold
),
transformed as(
    select * from union1
    union all
    select * from union2
    union all
    select * from union3
),
final as(
    select
		mth_mm::number(18,0) as mth_mm,
		customer_code::varchar(100) as customer_code,
		customer_name::varchar(200) as customer_name,
		retailer_code::varchar(100) as retailer_code,
		retailer_name::varchar(200) as retailer_name,
		rtruniquecode::varchar(100) as rtruniquecode,
		region_name::varchar(100) as region_name,
		zone_name::varchar(100) as zone_name,
		territory_name::varchar(100) as territory_name,
		channel_name::varchar(100) as channel_name,
		retailer_category_name::varchar(100) as retailer_category_name,
		unique_sales_code::varchar(50) as unique_sales_code,
		salesman_code::varchar(50) as salesman_code,
		salesman_name::varchar(255) as salesman_name,
		mothersku_code_recom::varchar(50) as mothersku_code_recom,
		mothersku_name_recom::varchar(200) as mothersku_name_recom,
		mothersku_code_sold::varchar(50) as mothersku_code_sold,
		mothersku_name_sold::varchar(200) as mothersku_name_sold,
		quantity::number(38,2) as quantity,
		achievement_nr::number(38,2) as achievement_nr,
		total_subd::number(18,0) as total_subd,
		qtr::number(18,0) as qtr,
		period::varchar(6) as period,
		msl_count::number(18,0) as msl_count,
		msl_sold_count::number(18,0) as msl_sold_count,
		ms_flag::varchar(1) as ms_flag,
		hit_ms_flag::varchar(1) as hit_ms_flag,
		dataset::varchar(100) as dataset,
		to_date(crtdtm) as crtdtm,
		to_date(upddtm) as upddtm,
		latest_customer_code::varchar(50) as latest_customer_code,
		latest_customer_name::varchar(150) as latest_customer_name,
		latest_territory_code::varchar(50) as latest_territory_code,
		latest_territory_name::varchar(150) as latest_territory_name,
		latest_salesman_code::varchar(50) as latest_salesman_code,
		latest_salesman_name::varchar(150) as latest_salesman_name,
		latest_uniquesalescode::varchar(50) as latest_uniquesalescode
    from transformed

)
select * from final

