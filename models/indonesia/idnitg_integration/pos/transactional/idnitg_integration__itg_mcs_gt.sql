{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["year"],
        pre_hook = "delete from {{this}} where cast(year as integer) in (
        select cast(year as integer) from {{ source('idnsdl_raw', 'sdl_mds_id_lav_mcs_list') }});"
    )
}}
with source as 
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_lav_mcs_list') }}
),
edw_time_dim as 
(
    select * from {{ source('snapidnedw_integration', 'edw_time_dim') }}
) , ---change schema
trans as 
(
        select
            t1.year,
            t1.tiering,
            t1.sku,
            t2.jj_mnth_long,
            decode(
                jj_mnth_long,
                'January',
                january,
                'February',
                february,
                'March',
                march,
                'April',
                april,
                'May',
                may,
                'June',
                june,
                'July',
                july,
                'August',
                august,
                'September',
                september,
                'October',
                october,
                'November',
                november,
                'December',
                december
            )::varchar(255) as val
        from source as t1,
            (
                SELECT DISTINCT jj_mnth_long
                FROM edw_time_dim
            ) AS t2
),
final as
(
    select
    tiering::VARCHAR(255) as tiering,
    sku::VARCHAR(2000) AS sku_name,
    YEAR::VARCHAR(20) as YEAR,
    jj_mnth_long::VARCHAR(20) as jj_mnth_long,
    val::VARCHAR(255) AS sku_code,
    current_timestamp()::timestamp_ntz(9) AS crt_dttm
    from trans
)
select * from final