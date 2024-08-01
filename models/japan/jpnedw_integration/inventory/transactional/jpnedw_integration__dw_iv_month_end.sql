{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "  update {{this}} edw set item_cd = wk.item_cd, cs_qty = wk.cs_qty, qty = wk.qty, proc_dt = wk.proc_dt, update_dt = current_timestamp() from {{ ref('jpnwks_integration__wk_iv_month_end_stg') }} wk where edw.jan_cd = wk.jan_cd and edw.cstm_cd = wk.cstm_cd and edw.invt_dt = wk.invt_dt;
                    delete from {{ ref('jpnwks_integration__wk_iv_month_end_stg') }} wk using {{this}} edw where edw.jan_cd = wk.jan_cd and edw.cstm_cd = wk.cstm_cd and edw.invt_dt = wk.invt_dt;"
    )
}}

with wk_iv_month_end_stg as(
    select * from {{ ref('jpnwks_integration__wk_iv_month_end_stg') }}
),
final as(
    select
        cstm_cd::varchar(10) as cstm_cd,
        jan_cd::varchar(18) as jan_cd,
        invt_dt::timestamp_ntz(9) as invt_dt,
        item_cd::varchar(18) as item_cd,
        pc::number(5,0) as pc,
        cs_qty::number(20,3) as cs_qty,
        qty::number(20,3) as qty,
        proc_dt::timestamp_ntz(9) as proc_dt,
        current_timestamp()::timestamp_ntz(9) as create_dt,
        current_timestamp()::timestamp_ntz(9) as update_dt
    from wk_iv_month_end_stg
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where wk_iv_month_end_stg.proc_dt > (select max(update_dt) from {{ this }})
    {% endif %}
)
select * from final