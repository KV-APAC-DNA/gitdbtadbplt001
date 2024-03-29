{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['filename'],
        pre_hook= "delete from {{this}} where filename in (select distinct filename from {{ source('vnmsdl_raw', 'sdl_vn_mt_sellout_mega') }})"
    )
}}

with source as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_mt_sellout_mega') }}
),
final as(
    select 
        SUBSTRING(RIGHT (filename, 11), 4, 4)::varchar(20) AS year, 
        case
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
        site_no::varchar(20) as site_no,
        site_name::varchar(255) as site_name,
        period::varchar(20) as period,
        art_no::varchar(20) as art_no,
        art_sv_name::varchar(255) as art_sv_name,
        suppl_no::varchar(20) as suppl_no,
        suppl_name::varchar(255) as suppl_name,
        sale_qty::number(18,0) as sale_qty,
        cogs_amt::number(20,5) as cogs_amt,
        filename::varchar(100) as filename,
        run_id::number(14,0) as run_id,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm, 
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final