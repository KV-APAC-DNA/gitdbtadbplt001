{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["matl_num"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}
with source as 
(
    select * from {{ source('ntasdl_raw', 'sdl_kr_fert_material_fact') }}
),
final as
(
    select
        matl_num::varchar(18) as matl_num,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final