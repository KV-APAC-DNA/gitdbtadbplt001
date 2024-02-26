--Import CTE
with sdl_mds_cn_sku_benchmarks as (
    select * from {{ source('chnsdl_raw', 'sdl_mds_cn_sku_benchmarks') }}
),
sdl_mds_in_sku_benchmarks as (
    select * from {{ source('indsdl_raw', 'sdl_mds_in_sku_benchmarks') }}
),

sdl_mds_jp_sku_benchmarks as (
    select * from {{ source('jpnsdl_raw', 'sdl_mds_jp_sku_benchmarks') }}
),

sdl_mds_jp_dcl_sku_benchmarks as (
    select * from {{ source('jpnsdl_raw', 'sdl_mds_jp_dcl_sku_benchmarks') }}
),

sdl_mds_kr_sku_benchmarks as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_sku_benchmarks') }}
),

sdl_mds_hk_sku_benchmarks as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_hk_sku_benchmarks') }}
),

sdl_mds_tw_sku_benchmarks as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_tw_sku_benchmarks') }}
),

sdl_mds_pacific_sku_benchmarks as (
    select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_sku_benchmarks') }}
),

sdl_mds_id_sku_benchmarks as (
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_sku_benchmarks') }}
),
--create thailand schema and add this in tha schemas
sdl_mds_th_sku_benchmarks as (
    select * from {{ source('osesdl_raw', 'sdl_mds_th_sku_benchmarks') }}
),
--create phillipines schema and add this in phl schemas
sdl_mds_ph_sku_benchmarks as (
    select * from {{ source('osesdl_raw', 'sdl_mds_ph_sku_benchmarks') }}
),

sdl_mds_sg_sku_benchmarks as (
    select * from {{ source('sgpsdl_raw', 'sdl_mds_sg_sku_benchmarks') }}
),

sdl_mds_my_sku_benchmarks as (
    select * from {{ source('myssdl_raw', 'sdl_mds_my_sku_benchmarks') }}
),
--create vietnam schema and add this in vnm schemas
sdl_mds_vn_sku_benchmarks as (
    select * from {{ source('osesdl_raw', 'sdl_mds_vn_sku_benchmarks') }}
),

--Logical CTE
transformed as (
    select 
        'China Personal Care' as "market", 
        jj_upc, jj_sku_description, 
        jj_packsize, 
        jj_target, 
        variance, 
        comp_upc, 
        comp_sku_description, 
        comp_packsize, 
        valid_from, 
        valid_to
    from sdl_mds_cn_sku_benchmarks group by 1,2,3,4,4,5,6,7,8,9,10,11
    UNION ALL
    select 
        'India' as "market",
        jj_upc,
        jj_sku_description,
        jj_packsize,
        jj_target,
        variance,
        comp_upc,
        comp_sku_description,
        comp_packsize,
        valid_from,
        valid_to
    from sdl_mds_in_sku_benchmarks 
    group by 1,2,3,4,4,5,6,7,8,9,10,11
    UNION ALL
    select 
        'Japan' as "market",
        jj_upc,
        jj_sku_description,
        jj_packsize,
        jj_target,
        variance,
        comp_upc,
        comp_sku_description,
        comp_packsize,
        valid_from,
        valid_to
    from sdl_mds_jp_sku_benchmarks 
    group by 1,2,3,4,4,5,6,7,8,9,10,11
    UNION ALL
    select 
        'Japan DCL' as "market",
        jj_upc,
        jj_sku_description,
        jj_packsize,
        jj_target,
        variance,
        comp_upc,
        comp_sku_description,
        comp_packsize,
        valid_from,
        valid_to
    from sdl_mds_jp_dcl_sku_benchmarks group by 1,2,3,4,4,5,6,7,8,9,10,11
    UNION ALL
    select 
        'Korea' as "market",
        jj_upc,
        jj_sku_description,
        jj_packsize,
        jj_target,
        variance,
        comp_upc,
        comp_sku_description,
        comp_packsize,
        valid_from,
        valid_to
    from sdl_mds_kr_sku_benchmarks 
    group by 1,2,3,4,4,5,6,7,8,9,10,11
    UNION ALL
    select 
        'Hong Kong' as "market",
        jj_upc,
        jj_sku_description,
        jj_packsize,
        jj_target,
        variance,
        comp_upc,
        comp_sku_description,
        comp_packsize,
        valid_from,
        valid_to
    from sdl_mds_hk_sku_benchmarks group by 1,2,3,4,4,5,6,7,8,9,10,11
    UNION ALL
    select 
        'Taiwan' as "market",
        jj_upc,
        jj_sku_description,
        jj_packsize,
        jj_target,
        variance,
        comp_upc,
        comp_sku_description,
        comp_packsize,
        valid_from,
        valid_to
    from sdl_mds_tw_sku_benchmarks 
    group by 1,2,3,4,4,5,6,7,8,9,10,11
    UNION ALL
    select 
        'Australia' as "market",
        jj_upc,
        jj_sku_description,
        jj_packsize,
        jj_target,
        variance,
        comp_upc,
        comp_sku_description,
        comp_packsize,
        valid_from,
        valid_to
    from sdl_mds_pacific_sku_benchmarks 
    group by 1,2,3,4,4,5,6,7,8,9,10,11
    UNION ALL
    select 
        'Indonesia' as "market",
        jj_upc,
        jj_sku_description,
        jj_packsize,
        jj_target,
        variance,
        comp_upc,
        comp_sku_description,
        comp_packsize,
        valid_from,
        valid_to
    from sdl_mds_id_sku_benchmarks 
    group by 1,2,3,4,4,5,6,7,8,9,10,11
    UNION ALL
    select 
        'Thailand' as "market",
        jj_upc,
        jj_sku_description,
        jj_packsize,
        jj_target,
        variance,
        comp_upc,
        comp_sku_description,
        comp_packsize,
        valid_from,
        valid_to
    from sdl_mds_th_sku_benchmarks 
    group by 1,2,3,4,4,5,6,7,8,9,10,11
    UNION ALL
    select 
        'Philippines' as "market",
        jj_upc,
        jj_sku_description,
        jj_packsize,
        jj_target,
        variance,
        comp_upc,
        comp_sku_description,
        comp_packsize,
        valid_from,
        valid_to
    from sdl_mds_ph_sku_benchmarks 
    group by 1,2,3,4,4,5,6,7,8,9,10,11
    UNION ALL
    select 
        'Singapore' as "market",
        jj_upc,
        jj_sku_description,
        jj_packsize,
        jj_target,
        variance,
        comp_upc,
        comp_sku_description,
        comp_packsize,
        valid_from,
        valid_to
    from sdl_mds_sg_sku_benchmarks 
    group by 1,2,3,4,4,5,6,7,8,9,10,11
    UNION ALL
    select 
        'Malaysia' as "market",
        jj_upc,
        jj_sku_description,
        jj_packsize,
        jj_target,
        variance,
        comp_upc,
        comp_sku_description,
        comp_packsize,
        valid_from,
        valid_to
    from sdl_mds_my_sku_benchmarks 
    group by 1,2,3,4,4,5,6,7,8,9,10,11
    UNION ALL
    select 
        'Vietnam' as "market",
        jj_upc,
        jj_sku_description,
        jj_packsize,
        jj_target,
        variance,
        comp_upc,
        comp_sku_description,
        comp_packsize,
        valid_from,
        valid_to
    from sdl_mds_vn_sku_benchmarks 
    group by 1,2,3,4,4,5,6,7,8,9,10,11
),

--Final CTE
final as (
    select 
        "market"::varchar(510) as market,
        jj_upc::varchar(200) as jj_upc,
        jj_sku_description::varchar(510) as jj_sku_description,
        jj_packsize::number(31,3) as jj_packsize,
        jj_target::number(31,3) as jj_target,
        variance::number(31,3) as variance,
        comp_upc::varchar(200) as comp_upc,
        comp_sku_description::varchar(510) as comp_sku_description,
        comp_packsize::number(31,3) as comp_packsize,
        valid_from::timestamp_ntz(9) as valid_from,
        valid_to::timestamp_ntz(9) as valid_to,
        current_timestamp()::timestamp_ntz(9) as ctrd_dttm
    from transformed
)

--Final select
select * from final
