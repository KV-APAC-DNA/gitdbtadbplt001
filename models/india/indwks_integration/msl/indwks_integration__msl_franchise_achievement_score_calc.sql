with msl_extract_combine_program_type as(
    select * from {{ ref('indwks_integration__msl_extract_combine_program_type') }}
),
itg_mds_in_sss_score as(
    select * from {{ ref('inditg_integration__itg_mds_in_sss_score') }}
),
temp as (
    select
        program_type,
        store_code,
        latest_outlet_name,
        prod_hier_l3,
        cal_yr,
        qtr,
        count(case
            when actual = 'Y'
                then 1
        end) as actual_l3,
        count(target) as target_l3
    from msl_extract_combine_program_type
    group by
        program_type,
        store_code,
        latest_outlet_name,
        prod_hier_l3,
        cal_yr,
        qtr
),
score_l3 as (
    select
        program_type,
        store_code,
        latest_outlet_name,
        prod_hier_l3,
        cal_yr,
        qtr,
        actual_l3,
        target_l3,
        --nvl(actual_l3,0) / nvl(target_l3,0) as franchise_msl_achievement,
        sco.min_value,
        sco.max_value,
        sco.value,
        coalesce(cast(actual_l3 as decimal(15, 2)), 0)
        / coalesce(cast(target_l3 as decimal(15, 2)), 0)
            as franchise_msl_achievement
    from temp,
        itg_mds_in_sss_score as sco
    where
        upper(sco.store_class) = upper(temp.program_type)
        and upper(sco.brand) = upper(temp.prod_hier_l3)
        and upper(sco.kpi) = 'MSL COMPLIANCE'
),
transformed as (
    select
        msl.cal_yr,
        msl.rtruniquecode,
        msl.target,
        msl.actual,
        franchise_msl_achievement,
        upper(msl.country) as country,
        upper(msl.region_name) as region_name,
        upper(msl.zone_name) as zone_name,
        upper(msl.territory_name) as territory_name,
        upper(msl.channel_name) as channel_name,
        upper(msl.retail_environment) as retail_environment,
        upper(msl.salesman_code) as salesman_code,
        upper(msl.salesman_name) as salesman_name,
        upper(msl.distributor_code) as distributor_code,
        upper(msl.distributor_name) as distributor_name,
        upper(msl.qtr) as qtr,
        upper(msl.store_code) as store_code,
        upper(msl.store_name) as store_name,
        upper(msl.prod_hier_l3) as prod_hier_l3,
        upper(msl.prod_hier_l4) as prod_hier_l4,
        upper(msl.prod_hier_l5) as prod_hier_l5,
        upper(msl.prod_hier_l6) as prod_hier_l6,
        upper(msl.prod_hier_l7) as prod_hier_l7,
        upper(msl.prod_hier_l8) as prod_hier_l8,
        upper(msl.prod_hier_l9) as prod_hier_l9,
        upper(msl.program_type) as program_type,
        upper(msl.latest_outlet_name) as latest_outlet_name,
        coalesce(score_l3.actual_l3, 0) as actual_l3,
        coalesce(score_l3.target_l3, 0) as target_l3,
        case
            when
                cast(franchise_msl_achievement as decimal(15, 2)) >= min_value
                and max_value is null
                then value
            when
                cast(franchise_msl_achievement as decimal(15, 2)) >= min_value
                and cast(franchise_msl_achievement as decimal(15, 2))
                < max_value
                then value
            else 0
        end as franchise_score
    from msl_extract_combine_program_type as msl, score_l3
    where
        upper(msl.program_type) = upper(score_l3.program_type)
        and upper(msl.store_code) = upper(score_l3.store_code)
        and upper(msl.prod_hier_l3) = upper(score_l3.prod_hier_l3)
        and msl.cal_yr = score_l3.cal_yr
        and msl.qtr = score_l3.qtr
),
final as(
    select
        country::varchar(7) as country,
        region_name::varchar(75) as region_name,
        zone_name::varchar(75) as zone_name,
        territory_name::varchar(75) as territory_name,
        channel_name::varchar(225) as channel_name,
        retail_environment::varchar(75) as retail_environment,
        salesman_code::varchar(150) as salesman_code,
        salesman_name::varchar(300) as salesman_name,
        distributor_code::varchar(75) as distributor_code,
        distributor_name::varchar(225) as distributor_name,
        qtr::varchar(16) as qtr,
        cal_yr::number(18,0) as cal_yr,
        store_code::varchar(150) as store_code,
        store_name::varchar(376) as store_name,
        rtruniquecode::varchar(100) as rtruniquecode,
        prod_hier_l3::varchar(18) as prod_hier_l3,
        prod_hier_l4::varchar(75) as prod_hier_l4,
        prod_hier_l5::varchar(225) as prod_hier_l5,
        prod_hier_l6::varchar(225) as prod_hier_l6,
        prod_hier_l7::varchar(1) as prod_hier_l7,
        prod_hier_l8::varchar(1) as prod_hier_l8,
        prod_hier_l9::varchar(225) as prod_hier_l9,
        target::varchar(1) as target,
        actual::varchar(1) as actual,
        program_type::varchar(75) as program_type,
        latest_outlet_name::varchar(225) as latest_outlet_name,
        actual_l3::number(38,0) as actual_l3,
        target_l3::number(38,0) as target_l3,
        franchise_msl_achievement::number(31,16) as franchise_msl_achievement,
        franchise_score::number(31,3) as franchise_score
    from transformed
)
select * from final