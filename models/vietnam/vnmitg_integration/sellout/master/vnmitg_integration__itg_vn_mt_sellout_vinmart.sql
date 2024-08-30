{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['filename'],
        pre_hook= "{%if is_incremental()%}
        delete from {{this}} where filename in (select distinct filename from {{ source('vnmsdl_raw', 'sdl_vn_mt_sellout_vinmart') }} where filename not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_mt_sellout_vinmart__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_mt_sellout_vinmart__duplicate_test')}}
        )
        );
        {%endif%}"
    )
}}

with source as(
    select *, dense_rank() over (partition by null order by filename desc) rnk
     from {{ source('vnmsdl_raw', 'sdl_vn_mt_sellout_vinmart') }}
    where filename not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_mt_sellout_vinmart__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_mt_sellout_vinmart__duplicate_test')}}
        ) qualify rnk = 1
),
final as(
    select 
        SUBSTRING(RIGHT (filename,11),4,4)::varchar(20) AS year,
        CASE
            WHEN SUBSTRING(RIGHT (filename,11),1,3) LIKE '%Jan%' THEN '01'
            WHEN SUBSTRING(RIGHT (filename,11),1,3) LIKE '%Feb%' THEN '02'
            WHEN SUBSTRING(RIGHT (filename,11),1,3) LIKE '%Mar%' THEN '03'
            WHEN SUBSTRING(RIGHT (filename,11),1,3) LIKE '%Apr%' THEN '04'
            WHEN SUBSTRING(RIGHT (filename,11),1,3) LIKE '%May%' THEN '05'
            WHEN SUBSTRING(RIGHT (filename,11),1,3) LIKE '%Jun%' THEN '06'
            WHEN SUBSTRING(RIGHT (filename,11),1,3) LIKE '%Jul%' THEN '07'
            WHEN SUBSTRING(RIGHT (filename,11),1,3) LIKE '%Aug%' THEN '08'
            WHEN SUBSTRING(RIGHT (filename,11),1,3) LIKE '%Sep%' THEN '09'
            WHEN SUBSTRING(RIGHT (filename,11),1,3) LIKE '%Oct%' THEN '10'
            WHEN SUBSTRING(RIGHT (filename,11),1,3) LIKE '%Nov%' THEN '11'
            WHEN SUBSTRING(RIGHT (filename,11),1,3) LIKE '%Dec%' THEN '12'
        END::varchar(20) AS month,
        store::varchar(20) as store,
        store_name::varchar(255) as store_name,
        mch5_mc::varchar(20) as mch5_mc,
        mc::varchar(100) as mc,
        article::varchar(20) as article,
        article_name::varchar(255) as article_name,
        manufacturer::varchar(100) as manufacturer,
        region::varchar(100) as region,
        pos_quantity::number(18,0) as pos_quantity,
        pos_revenue::number(20,5) as pos_revenue,
        filename::varchar(100) as filename,
        run_id::number(14,0) as run_id,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final