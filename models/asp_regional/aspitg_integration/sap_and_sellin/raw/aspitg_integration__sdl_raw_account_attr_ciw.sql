{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as (
     select * from {{ ref('aspitg_integration__vw_stg_sdl_account_attr_ciw') }} ),
final as (
    select  chrt_accts,
        account,
        objvers,
        changed,
        bal_flag,
        cstel_flag,
        glacc_flag,
        logsys,
        sem_posit,
        zbravol1,
        zbravol2,
        zbravol3,
        zbravol4,
        zbravol5,
        zbravol6,
        zciwhl1,
        zciwhl2,
        zciwhl3,
        zciwhl4,
        zciwhl5,
        zciwhl6,
        zpb_gl_ty,
        filename,
        run_id,
        crt_dttm as crtd_dttm 
    from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crtd_dttm) from {{ this }}) 
 {% endif %}
    
    -- Need to add delta logic
)

select * from final