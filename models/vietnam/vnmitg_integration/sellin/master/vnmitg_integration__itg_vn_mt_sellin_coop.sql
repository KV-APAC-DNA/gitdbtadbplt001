{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "{%if is_incremental()%}
        delete from {{this}} where filename in ( select distinct filename from {{ source('vnmsdl_raw', 'sdl_vn_mt_sellin_coop') }} where filename not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_mt_sellin_coop__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_mt_sellin_coop__lookup_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_mt_sellin_coop__lookup_test2')}}
        ));{%endif%}"
    )
}}

with source as(
    select *, dense_rank() over (partition by null order by filename desc) rnk
     from {{ source('vnmsdl_raw', 'sdl_vn_mt_sellin_coop') }}
    where filename not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_mt_sellin_coop__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_mt_sellin_coop__lookup_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_mt_sellin_coop__lookup_test2')}}
        )
        qualify rnk = 1
),
final as(
    select
        SUBSTRING(RIGHT(filename, 11), 4, 4)::varchar(20) AS year,
        CASE
            WHEN SUBSTRING(RIGHT(filename, 11), 1, 3) LIKE '%Jan%'
            THEN '01'
            WHEN SUBSTRING(RIGHT(filename, 11), 1, 3) LIKE '%Feb%'
            THEN '02'
            WHEN SUBSTRING(RIGHT(filename, 11), 1, 3) LIKE '%Mar%'
            THEN '03'
            WHEN SUBSTRING(RIGHT(filename, 11), 1, 3) LIKE '%Apr%'
            THEN '04'
            WHEN SUBSTRING(RIGHT(filename, 11), 1, 3) LIKE '%May%'
            THEN '05'
            WHEN SUBSTRING(RIGHT(filename, 11), 1, 3) LIKE '%Jun%'
            THEN '06'
            WHEN SUBSTRING(RIGHT(filename, 11), 1, 3) LIKE '%Jul%'
            THEN '07'
            WHEN SUBSTRING(RIGHT(filename, 11), 1, 3) LIKE '%Aug%'
            THEN '08'
            WHEN SUBSTRING(RIGHT(filename, 11), 1, 3) LIKE '%Sep%'
            THEN '09'
            WHEN SUBSTRING(RIGHT(filename, 11), 1, 3) LIKE '%Oct%'
            THEN '10'
            WHEN SUBSTRING(RIGHT(filename, 11), 1, 3) LIKE '%Nov%'
            THEN '11'
            WHEN SUBSTRING(RIGHT(filename, 11), 1, 3) LIKE '%Dec%'
            THEN '12'
        END::varchar(20) AS month,
        sku::varchar(20) as sku,
        idescr::varchar(200) as idescr,
        vendor::varchar(50) as vendor,
        asname::varchar(200) as asname,
        imfgr::varchar(50) as imfgr,
        store::varchar(50) as store,
        COALESCE(sales_quantity, 0)::number(18,0) AS sales_quantity,
        filename::varchar(100) as filename,
        run_id::number(14,0) as run_id,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final