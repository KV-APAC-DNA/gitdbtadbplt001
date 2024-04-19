{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
    )
}}

with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_dstr_woolworth_inv') }}
),
final as(
    select
        inv_date,
        rank,
        article_code,
        articledesc,
        mm_code,
        mm_name,
        cm_code,
        cm_name,
        rm_code,
        rm_name,
        rep_code,
        replenisher,
        goods_supplier_code,
        goods_supplier_name,
        lt,
        dc_code,
        acd,
        rp_type,
        alc_status,
        om,
        vp,
        ti,
        hi,
        ww_stores,
        sl_perc,
        sl_missed_value,
        soh_oms,
        soo_oms,
        soh_price,
        demand_oms,
        issues_oms,
        not_supplied_oms,
        fairshare_oms,
        overlay_oms,
        awd_oms,
        avg_issues,
        dos_oms,
        due_date,
        reason,
        oos_comments,
        oos_28_days,
        cons_days_oos,
        total_wholesale_demand_om,
        total_wholesale_issue_om,
        wholesale_flag,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
     {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
 {% endif %}
)
select * from final