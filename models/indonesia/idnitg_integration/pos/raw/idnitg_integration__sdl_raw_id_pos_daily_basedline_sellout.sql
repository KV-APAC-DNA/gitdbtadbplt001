{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as(
    select * from {{ source('idnsdl_raw','sdl_id_pos_daily_basedline_sellout') }}
),
final as(
    select 
        key_account as key_account,
        plu as plu,
        month as month,
        year as year,
        roi_month as roi_month,
        qty_trx as qty_trx,
        cum_ytm_qty as cum_ytm_qty,
        promo_qty as promo_qty,
        cum_promo_qty as cum_promo_qty,
        basedline_total_qty as basedline_total_qty,
        cum_basedline_total_qty as cum_basedline_total_qty,
        total_days as total_days,
        cum_total_days as cum_total_days,
        promo_days as promo_days,
        cum_promo_days as cum_promo_days,
        baselined_total_days as baselined_total_days,
        cum_baselined_total_days as cum_baselined_total_days,
        total_qty_baselined as total_qty_baselined,
        indirect_qty_trx as indirect_qty_trx,
        indirect_cum_qty as indirect_cum_qty,
        indirect_promo_qty as indirect_promo_qty,
        indirect_cum_promo_qty as indirect_cum_promo_qty,
        indirect_basedline_total_qty as indirect_basedline_total_qty,
        indirect_cum_basedline_total_qty as indirect_cum_basedline_total_qty,
        indirect_qty_basedlined as indirect_qty_basedlined,
        pos_cust as pos_cust,
        yearmonth as yearmonth,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        file_name as file_name
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)

select * from final