{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as(
    select * from {{ source('idnsdl_raw','sdl_id_pos_guardian_stock') }}
    where filename not in (
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_pos_guardian_stock__null_test') }}
            union all
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_pos_guardian_stock__date_check') }}
    ) 
),
final as(
    select 
        article as article,
        article_desc as article_desc,
        category as category,
        soh_stores as soh_stores,
        soh_dc as soh_dc,
        pos_cust as pos_cust,
        yearmonth as yearmonth,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        filename as filename
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)

select * from final