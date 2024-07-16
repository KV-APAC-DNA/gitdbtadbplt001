with edw_hcp360_kpi_rpt as
(
    select * from DEV_DNA_CORE.SNAPINDEDW_INTEGRATION.EDW_HCP360_KPI_RPT
),
trans1 as
(
    SELECT 
       country,
       source_system,
       activity_type,
       year_month,
       brand,
       customer_code,
       customer_name,
       retailer_code,
       retailer_name,
       retailer_category_cd,
       retailer_category_name,
       region,
       zone,
       sales_area,
       num_buying_retailer,
       sales_value
    FROM edw_hcp360_kpi_rpt
    WHERE source_system = 'SALES_CUBE'
       AND activity_type = 'SALES_ANALYSIS_NOCB'
       AND country = 'IN'
),
final as
(
    select
    country::varchar(20) as country,
	source_system::varchar(20) as source_system,
	activity_type::varchar(50) as activity_type,
	year_month::varchar(25) as year_month,
	brand::varchar(20) as brand,
	customer_code::varchar(50) as customer_code,
	customer_name::varchar(150) as customer_name,
	retailer_code::varchar(100) as retailer_code,
	retailer_name::varchar(150) as retailer_name,
	retailer_category_cd::varchar(25) as retailer_category_cd,
	retailer_category_name::varchar(50) as retailer_category_name,
	region::varchar(100) as region,
	zone::varchar(50) as zone,
	sales_area::varchar(100) as sales_area,
	num_buying_retailer::varchar(500) as num_buying_retailer,
	sales_value::number(18,5) as sales_value
    from trans1
)

select * from final