{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as(
    select * from {{ source('idnsdl_raw','sdl_id_pos_daily_sat_sellout') }}
),
final as(
    select 
        account as account,
        kode_branch as kode_branch,
        branch_name as branch_name,
        tgl as tgl,
        plu as plu,
        descp as descp,
        type as type,
        value as value,
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