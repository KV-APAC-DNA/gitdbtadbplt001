{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["filename"],
        pre_hook="{% if is_incremental() %}
                delete from {{this}} where filename  in (select distinct filename from {{ source('idnsdl_raw', 'sdl_id_pos_igr_sellout') }});
                {% endif %}"
    )
}}

with source as (
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_igr_sellout') }}
),

final as (
    select
        no::number(20, 0) as no,
        description::varchar(200) as description,
        branch::varchar(100) as branch,
        type::varchar(10) as type,
        "values"::number(18, 2) as "values",
        upper(pos_cust)::varchar(50) as pos_cust,
        yearmonth::varchar(10) as yearmonth,
        run_id::number(14, 0) as run_id,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crtd_dttm,
        filename::varchar(100) as filename
    from source
)

select * from final
