{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= '{{build_da_iv_edi_accum_temp()}}'
    )
}}

with source as(
    select * from {{ source('jpnsdl_raw', 'edi_invt_dt') }}
),
da_iv_edi_accum as(
    select * from {{ source('jpnedw_integration', 'da_iv_edi_accum_temp') }} 
),
final as(
    select 
        create_dt as create_dt,
        create_user as create_user,
        update_dt as update_dt,
        update_user as update_user,
        cstm_cd as cstm_cd,
        itm_cd as item_cd,
        invt_dt as invt_dt,
        cs_qty as cs_qty,
        qty as qty,
        proc_dt as proc_dt,
        van_type as van_type,
        jan_cd as jan_cd
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.update_dt > (SELECT SUBSTRING(MAX(UPDATE_DT),1,19) FROM da_iv_edi_accum) 
    {% endif %}
)
select * from final