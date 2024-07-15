with sss_scorecard_msl_extract as(
    select * from {{ ref('indwks_integration__sss_scorecard_msl_extract') }}
),
itg_sss_scorecard_data as(
    select * from {{ ref('inditg_integration__itg_sss_scorecard_data') }}
),
sss as(
    select right(quarter, 1) as "QUARTER",
            year,
            program_type,
            jnj_id,
            upper(max(outlet_name)) as "LATEST_OUTLET_NAME"
        from itg_sss_scorecard_data
        group by quarter,
            year,
            program_type,
            jnj_id
),
transformed as(
    select msl.country,
        msl.region_name,
        msl.zone_name,
        msl.territory_name,
        msl.channel_name,
        msl.retail_environment,
        msl.salesman_code,
        msl.salesman_name,
        msl.distributor_code,
        msl.distributor_name,
        msl.qtr,
        msl.cal_yr,
        msl.store_code,
        msl.store_name,
        msl.rtruniquecode,
        msl.prod_hier_l3,
        msl.prod_hier_l4,
        msl.prod_hier_l5,
        msl.prod_hier_l6,
        msl.prod_hier_l7,
        msl.prod_hier_l8,
        msl.prod_hier_l9
        --,  case when msl.msl_flag = 'y' then cast(1 as numeric(10,4)) else cast(0 as numeric(10,4)) end as "target"
        --,  case when msl.msl_hit = 'y' then cast(1 as numeric(10,4)) else cast(0 as numeric(10,4)) end as "actual"
        ,
        msl.msl_flag as target,
        msl.msl_hit as actual,
        sss.program_type,
        sss.latest_outlet_name
    from sss_scorecard_msl_extract msl
    left join sss on msl.store_code = sss.jnj_id
        and msl.cal_yr = sss.year
        and msl.qtr = sss.quarter
)
select * from transformed
