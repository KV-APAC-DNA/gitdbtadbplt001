{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=["file_name"],
        pre_hook= "delete from {{this}} 
            where file_name in (select distinct file_name 
            from {{ source('thasdl_raw', 'sdl_jnj_mer_share_of_shelf') }}
            where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_mer_share_of_shelf__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_mer_share_of_shelf__duplicate_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_mer_share_of_shelf__test_date_format_odd_eve') }}
            )
            )"
    )
}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_jnj_mer_share_of_shelf') }}
            where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_mer_share_of_shelf__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_mer_share_of_shelf__duplicate_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_mer_share_of_shelf__test_date_format_odd_eve') }}
            )
),
final as(
    select
        sos_date::varchar(255) as sos_date,
        merchandiser_name::varchar(255) as merchandiser_name,
        supervisor_name::varchar(255) as supervisor_name,
        area::varchar(255) as area,
        channel::varchar(255) as channel,
        account::varchar(255) as account,
        store_id::varchar(255) as store_id,
        store_name::varchar(255) as store_name,
        category::varchar(255) as category,
        agency::varchar(255) as agency,
        brand::varchar(255) as brand,
        size::varchar(255) as size,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name,
        yearmo::varchar(255) as yearmo,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
  from source
)
select * from final