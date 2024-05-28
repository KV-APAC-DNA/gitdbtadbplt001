{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["year"],
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where cast(year as integer) in (
        select cast(year as integer) from {{ source('idnsdl_raw', 'sdl_mds_id_lav_mcs_list') }});
        {% endif %}"
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
    tiering::varchar(255) as tiering,
    sku::varchar(2000) as sku_name,
    year::varchar(20) as year,
    jj_mnth_long::varchar(20) as month,
    val::varchar(255) as sku_code,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as crt_dttm
    from trans
)
select * from final