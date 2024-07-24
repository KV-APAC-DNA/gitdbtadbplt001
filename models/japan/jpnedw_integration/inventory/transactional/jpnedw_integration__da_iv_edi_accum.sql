{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= " update {{this}} edw set valid_flg = '0' from {{ ref('jpnwks_integration__wk_iv_edi') }} wk where edw.cstm_cd = wk.cstm_cd and edw.item_cd = wk.item_cd and edw.invt_dt = wk.invt_dt;"
    )
}}

with wk_iv_edi as(
    select * from {{ ref('jpnwks_integration__wk_iv_edi') }}
),
final as(
    select 
        create_dt::timestamp_ntz(9) as create_dt,
        create_user::varchar(20) as create_user,
        update_dt::timestamp_ntz(9) as update_dt,
        update_user::varchar(20) as update_user,
        cstm_cd::varchar(10) as cstm_cd,
        item_cd::varchar(18) as item_cd,
        invt_dt::timestamp_ntz(9) as invt_dt,
        cs_qty::number(6,0) as cs_qty,
        qty::number(6,0) as qty,
        proc_dt::timestamp_ntz(9) as proc_dt,
        van_type::varchar(10) as van_type,
        jan_cd::varchar(18) as jan_cd,
        current_timestamp()::timestamp_ntz(9) as exec_dt,
        '1'::varchar(1) as valid_flg
    from wk_iv_edi
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where wk_iv_edi.update_dt > (select max(exec_dt) from {{ this }})
    {% endif %}
)
select * from final