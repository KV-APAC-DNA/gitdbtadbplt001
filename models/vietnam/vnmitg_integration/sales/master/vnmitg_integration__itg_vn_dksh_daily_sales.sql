{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['group_ds', 'material', 'file_name', 'syslot'],
        pre_hook= "delete from {{this}} where trim(coalesce(upper(group_ds), '')) || trim(upper(material)) || trim(substring(file_name, 12, 8)) || trim(coalesce(upper(syslot), '')) in ( select distinct trim(coalesce(upper(group_ds), '')) || trim(upper(material)) || trim(file_date) || trim(coalesce(upper(syslot), '')) from {{ source('vnmsdl_raw', 'sdl_vn_dksh_daily_sales') }} );"
    )
}}

with source as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dksh_daily_sales') }}
),
final as(
    select
        group_ds::varchar(255) as group_ds,
        category::varchar(255) as category,
        material::varchar(255) as material,
        materialdescription::varchar(255) as materialdescription,
        syslot::varchar(255) as syslot,
        batchno::varchar(255) as batchno,
        exp_date::varchar(255) as exp_date,
        total::number(18,0) as total,
        CAST(NULLIF(REPLACE(hcm, '-', 0), 0) AS INT) as hcm,
        CAST(NULLIF(REPLACE(vsip, '-', 0), 0) AS INT) as vsip,
        CAST(NULLIF(REPLACE(langha, '-', 0), 0) AS INT) as langha,
        CAST(NULLIF(REPLACE(thanhtri, '-', 0), 0) AS INT) as thanhtri,
        CAST(NULLIF(REPLACE(danang, '-', 0), 0) AS INT) as danang,
        values_lc::number(24,10) as values_lc,
        reason::varchar(255) as reason,
        (SUBSTRING(file_date, 0, 4) || '-' || SUBSTRING(file_date, 5, 2) || '-' || SUBSTRING(file_date, 7, 2))::varchar(255) AS file_date,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
)
select * from final