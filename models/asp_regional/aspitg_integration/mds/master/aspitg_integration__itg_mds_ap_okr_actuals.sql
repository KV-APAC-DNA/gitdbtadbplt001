--Import CTE
with itg_mds_ap_sales_ops_map as (
    select * from {{ ref('aspitg_integration__itg_mds_ap_sales_ops_map') }}
), 

sdl_mds_ap_okr_actuals as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_ap_okr_actuals') }}
),

edw_okr_brand_map as (
    select * from {{ source('aspedw_integration', 'edw_okr_brand_map') }}
),

transformed as
(
    select distinct kpi_code , 
            pyramid_code , 
            case when market_name is null and upper(cluster_code) = 'ANZ' then 'Pacific'
                when market_name is null and upper(cluster_code) = 'MA'  then 'Metropolitan Asia'
                when (upper(cluster_code) like '%CHINA%' or upper(market_name) like '%CHINA%')then 'China'
                when (upper(cluster_code) like '%JP%'  or upper(market_name) in ('JJKK' , 'JP DCL'))then 'Japan'
                when market_name is not null then map.destination_cluster
                            else cluster_code end  as cluster,
            market_name,
            fisc_yr,
            qtr,
            year_month,
            case when base.brand_code is not null then brand_map.segment else base.segment_code end as franchise,
            brand_code,
            nts_grwng_share_size,
            actual_value,
            ytd
        from   
    (select kpi_code , pyramid_code , cluster_code , market_name ,  cast(fisc_yr_code as integer) as fisc_yr ,1 as qtr ,fisc_yr_code || '01' as year_month , segment_code , brand_code , nts_grwng_share_size, jan_actual as actual_value, ytd  from sdl_mds_ap_okr_actuals  where (cluster_code is  not null or market_name is  not null or segment_code is  not null or brand_code is  not null )
    and fisc_yr_code is not null
    UNION ALL
    select kpi_code , pyramid_code , cluster_code , market_name ,  cast(fisc_yr_code as integer) as fisc_yr,1 as qtr ,fisc_yr_code || '02' as year_month , segment_code , brand_code , nts_grwng_share_size, feb_actual 
    as actual_value , ytd from sdl_mds_ap_okr_actuals  where (cluster_code is  not null or market_name is  not null or segment_code is  not null or brand_code is  not null )
    and fisc_yr_code is not null
    UNION ALL
    select kpi_code , pyramid_code , cluster_code , market_name ,  cast(fisc_yr_code as integer) as fisc_yr,1 as qtr ,fisc_yr_code || '03' as year_month , segment_code , brand_code , nts_grwng_share_size, mar_actual 
    as actual_value, ytd from sdl_mds_ap_okr_actuals  where (cluster_code is  not null or market_name is  not null or segment_code is  not null or brand_code is  not null )
    and fisc_yr_code is not null
    UNION ALL
    select kpi_code , pyramid_code , cluster_code , market_name ,  cast(fisc_yr_code as integer) as fisc_yr,2 as qtr ,fisc_yr_code || '04' as year_month , segment_code , brand_code , nts_grwng_share_size, apr_actual 
    as actual_value, ytd from sdl_mds_ap_okr_actuals  where (cluster_code is  not null or market_name is  not null or segment_code is  not null or brand_code is  not null )
    and fisc_yr_code is not null
    UNION ALL
    select kpi_code , pyramid_code , cluster_code , market_name ,  cast(fisc_yr_code as integer) as fisc_yr,2 as qtr ,fisc_yr_code || '05' as year_month , segment_code , brand_code , nts_grwng_share_size, may_actual 
    as actual_value, ytd from sdl_mds_ap_okr_actuals  where (cluster_code is  not null or market_name is  not null or segment_code is  not null or brand_code is  not null )
    and fisc_yr_code is not null
    UNION ALL
    select kpi_code , pyramid_code , cluster_code , market_name ,  cast(fisc_yr_code as integer) as fisc_yr,2 as qtr ,fisc_yr_code || '06' as year_month , segment_code , brand_code , nts_grwng_share_size, june_actual 
    as actual_value, ytd from sdl_mds_ap_okr_actuals  where (cluster_code is  not null or market_name is  not null or segment_code is  not null or brand_code is  not null )
    and fisc_yr_code is not null
    UNION ALL
    select kpi_code , pyramid_code , cluster_code , market_name ,  cast(fisc_yr_code as integer) as fisc_yr,3 as qtr ,fisc_yr_code || '07' as year_month , segment_code , brand_code , nts_grwng_share_size, july_actual 
    as actual_value, ytd from sdl_mds_ap_okr_actuals  where (cluster_code is  not null or market_name is  not null or segment_code is  not null or brand_code is  not null )
    and fisc_yr_code is not null
    UNION ALL
    select kpi_code , pyramid_code , cluster_code , market_name ,  cast(fisc_yr_code as integer) as fisc_yr,3 as qtr ,fisc_yr_code || '08' as year_month , segment_code , brand_code , nts_grwng_share_size, august_actual 
    as actual_value, ytd from sdl_mds_ap_okr_actuals  where (cluster_code is  not null or market_name is  not null or segment_code is  not null or brand_code is  not null )
    and fisc_yr_code is not null
    UNION ALL
    select kpi_code , pyramid_code , cluster_code , market_name ,  cast(fisc_yr_code as integer) as fisc_yr,3 as qtr ,fisc_yr_code || '09' as year_month , segment_code , brand_code , nts_grwng_share_size, september_actual 
    as actual_value, ytd from sdl_mds_ap_okr_actuals  where (cluster_code is  not null or market_name is  not null or segment_code is  not null or brand_code is  not null )
    and fisc_yr_code is not null
    UNION ALL
    select kpi_code , pyramid_code , cluster_code , market_name ,  cast(fisc_yr_code as integer) as fisc_yr,4 as qtr ,fisc_yr_code || '10' as year_month , segment_code , brand_code , nts_grwng_share_size, october_actual 
    as actual_value, ytd from sdl_mds_ap_okr_actuals  where (cluster_code is  not null or market_name is  not null or segment_code is  not null or brand_code is  not null )
    and fisc_yr_code is not null
    UNION ALL
    select kpi_code , pyramid_code , cluster_code , market_name ,  cast(fisc_yr_code as integer) as fisc_yr,4 as qtr ,fisc_yr_code || '11' as year_month , segment_code , brand_code , nts_grwng_share_size, november_actual 
    as actual_value, ytd from sdl_mds_ap_okr_actuals  where (cluster_code is  not null or market_name is  not null or segment_code is  not null or brand_code is  not null )
    and fisc_yr_code is not null
    UNION ALL
    select kpi_code , pyramid_code , cluster_code , market_name ,  cast(fisc_yr_code as integer) as fisc_yr,4 as qtr ,fisc_yr_code || '12' as year_month , segment_code , brand_code , nts_grwng_share_size, december_actual 
    as actual_value, ytd from sdl_mds_ap_okr_actuals  where (cluster_code is  not null or market_name is  not null or segment_code is  not null or brand_code is  not null )
    and fisc_yr_code is not null
    UNION ALL 
    select kpi_code , pyramid_code , cluster_code , market_name ,  cast(fisc_yr_code as integer) as fisc_yr , null as qtr , null as year_month , segment_code , brand_code , nts_grwng_share_size, year_actual as actual_value, ytd  from sdl_mds_ap_okr_actuals  where (cluster_code is  not null or market_name is  not null or segment_code is  not null or brand_code is  not null )
    and fisc_yr_code is not null
    UNION ALL
    select kpi_code , pyramid_code , cluster_code , market_name ,  cast(fisc_yr_code as integer) as fisc_yr , 1 as qtr , null as year_month , segment_code , brand_code , nts_grwng_share_size, q1_actual as actual_value, ytd  from sdl_mds_ap_okr_actuals  where (cluster_code is  not null or market_name is  not null or segment_code is  not null or brand_code is  not null )
    and fisc_yr_code is not null
    UNION ALL
    select kpi_code , pyramid_code , cluster_code , market_name ,  cast(fisc_yr_code as integer) as fisc_yr , 2 as qtr , null as year_month , segment_code , brand_code , nts_grwng_share_size, q2_actual as actual_value, ytd  from sdl_mds_ap_okr_actuals  where (cluster_code is  not null or market_name is  not null or segment_code is  not null or brand_code is  not null )
    and fisc_yr_code is not null
    UNION ALL
    select kpi_code , pyramid_code , cluster_code , market_name ,  cast(fisc_yr_code as integer) as fisc_yr , 3 as qtr , null as year_month , segment_code , brand_code , nts_grwng_share_size, q3_actual as actual_value, ytd  from sdl_mds_ap_okr_actuals  where (cluster_code is  not null or market_name is  not null or segment_code is  not null or brand_code is  not null )
    and fisc_yr_code is not null
    UNION ALL
    select kpi_code , pyramid_code , cluster_code , market_name ,  cast(fisc_yr_code as integer) as fisc_yr , 4 as qtr , null as year_month , segment_code , brand_code , nts_grwng_share_size, q4_actual as actual_value, ytd  from sdl_mds_ap_okr_actuals  where (cluster_code is  not null or market_name is  not null or segment_code is  not null or brand_code is  not null )
    and fisc_yr_code is not null
    )base 
    left join  (select distinct destination_cluster ,  source_market 
    from itg_mds_ap_sales_ops_map where dataset = 'Market Share QSD') map on map.source_market = base.market_name
    left join edw_okr_brand_map brand_map on trim(upper(brand_map.brand)) = case when trim(upper(base.brand_code)) = 'DR.CI.LABO' then 'DR. CI: LABO'
                                                                                        when trim(upper(base.brand_code))  = 'CLEAN&CLEAR' then 'CLEAN & CLEAR'
                                                                                        when trim(upper(base.brand_code)) = 'O.B' then 'O.B.' else trim(upper(base.brand_code)) end
    and base.brand_code is not null
),

--Final CTE
final as (
    select
        kpi_code::varchar(500) as kpi,
        pyramid_code::varchar(30) as pyramid_code, 
        cluster::varchar(30) as cluster, 
        market_name::varchar(50) as market, 
        fisc_yr::number(18,0) as year, 
        qtr::number(18,0) as quarter, 
        year_month::varchar(10) as year_month, 
        franchise::varchar(50) as franchise, 
        brand_code::varchar(50) as brand, 
        nts_grwng_share_size::number(18,0) as nts_grwng_share_size, 
        actual_value::number(28,4) as actual_value,
        ytd::number(38,4) as ytd 
    from transformed
)

--Final select
select * from final

    
	

	
	
	
	
	

