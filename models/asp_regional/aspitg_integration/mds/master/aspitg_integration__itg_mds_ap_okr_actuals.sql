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

--Logical CTE
map as (
    select distinct
        source_market,
        case
            when source_market IN ('India', 'Indonesia', 'Philippines')
                then 'Southern Asia'
            else destination_cluster
        end as destination_cluster
    from itg_mds_ap_sales_ops_map
    where
        dataset = 'Market Share QSD'
),

base as (
    select
        kpi_code,
        pyramid_code,
        cluster_code,
        market_name,
        cast(fisc_yr_code as int) as fisc_yr,
        1 as qtr,
        fisc_yr_code || '01' as year_month,
        segment_code,
        brand_code,
        nts_grwng_share_size,
        jan_actual as actual_value,
        ytd
    from sdl_mds_ap_okr_actuals
    where
        (
            not cluster_code is null
            or not market_name is null
            or not segment_code is null
            or not brand_code is null
        )
        AND not fisc_yr_code is null
    union all
    select
        kpi_code,
        pyramid_code,
        cluster_code,
        market_name,
        cast(fisc_yr_code as int) as fisc_yr,
        1 as qtr,
        fisc_yr_code || '02' as year_month,
        segment_code,
        brand_code,
        nts_grwng_share_size,
        feb_actual as actual_value,
        ytd
    from sdl_mds_ap_okr_actuals
    where
        (
            not cluster_code is null
            or not market_name is null
            or not segment_code is null
            or not brand_code is null
        )
        AND not fisc_yr_code is null
    union all
    select
        kpi_code,
        pyramid_code,
        cluster_code,
        market_name,
        cast(fisc_yr_code as int) as fisc_yr,
        1 as qtr,
        fisc_yr_code || '03' as year_month,
        segment_code,
        brand_code,
        nts_grwng_share_size,
        mar_actual as actual_value,
        ytd
    from sdl_mds_ap_okr_actuals
    where
        (
            not cluster_code is null
            or not market_name is null
            or not segment_code is null
            or not brand_code is null
        )
        AND not fisc_yr_code is null
    union all
    select
        kpi_code,
        pyramid_code,
        cluster_code,
        market_name,
        cast(fisc_yr_code as int) as fisc_yr,
        2 as qtr,
        fisc_yr_code || '04' as year_month,
        segment_code,
        brand_code,
        nts_grwng_share_size,
        apr_actual as actual_value,
        ytd
    from sdl_mds_ap_okr_actuals
    where
        (
            not cluster_code is null
            or not market_name is null
            or not segment_code is null
            or not brand_code is null
        )
        AND not fisc_yr_code is null
    union all
    select
        kpi_code,
        pyramid_code,
        cluster_code,
        market_name,
        cast(fisc_yr_code as int) as fisc_yr,
        2 as qtr,
        fisc_yr_code || '05' as year_month,
        segment_code,
        brand_code,
        nts_grwng_share_size,
        may_actual as actual_value,
        ytd
    from sdl_mds_ap_okr_actuals
    where
        (
            not cluster_code is null
            or not market_name is null
            or not segment_code is null
            or not brand_code is null
        )
        AND not fisc_yr_code is null
    union all
    select
        kpi_code,
        pyramid_code,
        cluster_code,
        market_name,
        cast(fisc_yr_code as int) as fisc_yr,
        2 as qtr,
        fisc_yr_code || '06' as year_month,
        segment_code,
        brand_code,
        nts_grwng_share_size,
        june_actual as actual_value,
        ytd
    from sdl_mds_ap_okr_actuals
    where
        (
            not cluster_code is null
            or not market_name is null
            or not segment_code is null
            or not brand_code is null
        )
        AND not fisc_yr_code is null
    union all
    select
        kpi_code,
        pyramid_code,
        cluster_code,
        market_name,
        cast(fisc_yr_code as int) as fisc_yr,
        3 as qtr,
        fisc_yr_code || '07' as year_month,
        segment_code,
        brand_code,
        nts_grwng_share_size,
        july_actual as actual_value,
        ytd
    from sdl_mds_ap_okr_actuals
    where
        (
            not cluster_code is null
            or not market_name is null
            or not segment_code is null
            or not brand_code is null
        )
        AND not fisc_yr_code is null
    union all
    select
        kpi_code,
        pyramid_code,
        cluster_code,
        market_name,
        cast(fisc_yr_code as int) as fisc_yr,
        3 as qtr,
        fisc_yr_code || '08' as year_month,
        segment_code,
        brand_code,
        nts_grwng_share_size,
        august_actual as actual_value,
        ytd
    from sdl_mds_ap_okr_actuals
    where
        (
            not cluster_code is null
            or not market_name is null
            or not segment_code is null
            or not brand_code is null
        )
        AND not fisc_yr_code is null
    union all
    select
        kpi_code,
        pyramid_code,
        cluster_code,
        market_name,
        cast(fisc_yr_code as int) as fisc_yr,
        3 as qtr,
        fisc_yr_code || '09' as year_month,
        segment_code,
        brand_code,
        nts_grwng_share_size,
        september_actual as actual_value,
        ytd
    from sdl_mds_ap_okr_actuals
    where
        (
            not cluster_code is null
            or not market_name is null
            or not segment_code is null
            or not brand_code is null
        )
        AND not fisc_yr_code is null
    union all
    select
        kpi_code,
        pyramid_code,
        cluster_code,
        market_name,
        cast(fisc_yr_code as int) as fisc_yr,
        4 as qtr,
        fisc_yr_code || '10' as year_month,
        segment_code,
        brand_code,
        nts_grwng_share_size,
        october_actual as actual_value,
        ytd
    from sdl_mds_ap_okr_actuals
    where
        (
            not cluster_code is null
            or not market_name is null
            or not segment_code is null
            or not brand_code is null
        )
        AND not fisc_yr_code is null
    union all
    select
        kpi_code,
        pyramid_code,
        cluster_code,
        market_name,
        cast(fisc_yr_code as int) as fisc_yr,
        4 as qtr,
        fisc_yr_code || '11' as year_month,
        segment_code,
        brand_code,
        nts_grwng_share_size,
        november_actual as actual_value,
        ytd
    from sdl_mds_ap_okr_actuals
    where
        (
            not cluster_code is null
            or not market_name is null
            or not segment_code is null
            or not brand_code is null
        )
        AND not fisc_yr_code is null
    union all
    select
        kpi_code,
        pyramid_code,
        cluster_code,
        market_name,
        cast(fisc_yr_code as int) as fisc_yr,
        4 as qtr,
        fisc_yr_code || '12' as year_month,
        segment_code,
        brand_code,
        nts_grwng_share_size,
        december_actual as actual_value,
        ytd
    from sdl_mds_ap_okr_actuals
    where
        (
            not cluster_code is null
            or not market_name is null
            or not segment_code is null
            or not brand_code is null
        )
        AND not fisc_yr_code is null
    union all
    select
        kpi_code,
        pyramid_code,
        cluster_code,
        market_name,
        cast(fisc_yr_code as int) as fisc_yr,
        NULL as qtr,
        NULL as year_month,
        segment_code,
        brand_code,
        nts_grwng_share_size,
        year_actual as actual_value,
        ytd
    from sdl_mds_ap_okr_actuals
    where
        (
            not cluster_code is null
            or not market_name is null
            or not segment_code is null
            or not brand_code is null
        )
        AND not fisc_yr_code is null
    union all
    select
        kpi_code,
        pyramid_code,
        cluster_code,
        market_name,
        cast(fisc_yr_code as int) as fisc_yr,
        1 as qtr,
        NULL as year_month,
        segment_code,
        brand_code,
        nts_grwng_share_size,
        q1_actual as actual_value,
        ytd
    from sdl_mds_ap_okr_actuals
    where
        (
            not cluster_code is null
            or not market_name is null
            or not segment_code is null
            or not brand_code is null
        )
        AND not fisc_yr_code is null
    union all
    select
        kpi_code,
        pyramid_code,
        cluster_code,
        market_name,
        cast(fisc_yr_code as int) as fisc_yr,
        2 as qtr,
        NULL as year_month,
        segment_code,
        brand_code,
        nts_grwng_share_size,
        q2_actual as actual_value,
        ytd
    from sdl_mds_ap_okr_actuals
    where
        (
            not cluster_code is null
            or not market_name is null
            or not segment_code is null
            or not brand_code is null
        )
        AND not fisc_yr_code is null
    union all
    select
        kpi_code,
        pyramid_code,
        cluster_code,
        market_name,
        cast(fisc_yr_code as int) as fisc_yr,
        3 as qtr,
        NULL as year_month,
        segment_code,
        brand_code,
        nts_grwng_share_size,
        q3_actual as actual_value,
        ytd
    from sdl_mds_ap_okr_actuals
    where
        (
            not cluster_code is null
            or not market_name is null
            or not segment_code is null
            or not brand_code is null
        )
        AND not fisc_yr_code is null
    union all
    select
        kpi_code,
        pyramid_code,
        cluster_code,
        market_name,
        cast(fisc_yr_code as int) as fisc_yr,
        4 as qtr,
        NULL as year_month,
        segment_code,
        brand_code,
        nts_grwng_share_size,
        q4_actual as actual_value,
        ytd
    from sdl_mds_ap_okr_actuals
    where
        (
            not cluster_code is null
            or not market_name is null
            or not segment_code is null
            or not brand_code is null
        )
        AND not fisc_yr_code is null
),

transformed as (
    select distinct
        kpi_code,
        pyramid_code,
        coalesce(
            case
                when UPPER(cluster_code) = 'ANZ'
                    then 'Pacific'
                when UPPER(cluster_code) = 'MA'
                    then 'Metropolitan Asia'
                when UPPER(cluster_code) = 'SOUTH ASIA'
                    then 'Southern Asia'
                when (
                    UPPER(cluster_code) LIKE '%CHINA%'
                    OR UPPER(market_name) LIKE '%CHINA%'
                )
                    then 'China'
                when (
                    UPPER(cluster_code) LIKE '%JP%'
                    OR UPPER(market_name) IN ('JJKK', 'JP DCL')
                )
                    then 'Japan'
                else cluster_code
            end,
            map.destination_cluster
        ) as cluster,
        market_name,
        fisc_yr,
        qtr,
        year_month,
        case
            when not base.brand_code is null
                then brand_map.segment
            else base.segment_code
        end as franchise,
        brand_code,
        nts_grwng_share_size,
        actual_value,
        ytd
    FROM base
    LEFT JOIN map
        ON map.source_market = base.market_name
    LEFT JOIN edw_okr_brand_map as brand_map
    ON TRIM(UPPER(brand_map.brand)) = case
        when TRIM(UPPER(base.brand_code)) = 'DR.CI.LABO'
            then 'DR. CI: LABO'
        when TRIM(UPPER(base.brand_code)) = 'CLEAN&CLEAR'
            then 'CLEAN & CLEAR'
        when TRIM(UPPER(base.brand_code)) = 'O.B'
            then 'O.B.'
        else TRIM(UPPER(base.brand_code))
    end
    AND not base.brand_code is null
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

    
	

	
	
	
	
	

