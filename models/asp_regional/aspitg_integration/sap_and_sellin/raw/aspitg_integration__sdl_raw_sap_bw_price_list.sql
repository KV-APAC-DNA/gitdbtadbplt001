{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_price_list') }}
),
final as (
    select        
        sls_org,
        material,
        cond_rec_no,
        matl_grp,
        valid_to,
        knart,
        dt_from,
        amount,
        currency,
        unit,
        record_mode,
        comp_cd,
        price_unit,
        zcurrfpa,
        cdl_dttm,
        crt_dttm as curr_dt,
        file_name 
    from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(curr_dt) from {{ this }}) 
 {% endif %}
)

select * from final