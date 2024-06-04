with source as (
    select *
    from {{ source('idnsdl_raw', 'sdl_id_pos_daily_basedline_sellout') }}
),

final as (
    select
        key_account::varchar(20) as key_account,
        plu::number(18, 0) as plu,
        month::number(18, 0) as month,
        year::number(18, 0) as year,
        roi_month::varchar(200) as roi_month,
        qty_trx::number(18, 0) as qty_trx,
        cum_ytm_qty::number(18, 0) as cum_ytm_qty,
        promo_qty::number(18, 0) as promo_qty,
        cum_promo_qty::number(18, 0) as cum_promo_qty,
        basedline_total_qty::number(18, 0) as basedline_total_qty,
        cum_basedline_total_qty::number(18, 0) as cum_basedline_total_qty,
        total_days::number(18, 0) as total_days,
        cum_total_days::number(18, 0) as cum_total_days,
        promo_days::number(18, 0) as promo_days,
        cum_promo_days::number(18, 0) as cum_promo_days,
        baselined_total_days::number(18, 0) as baselined_total_days,
        cum_baselined_total_days::number(18, 0) as cum_baselined_total_days,
        total_qty_baselined::number(18, 0) as total_qty_baselined,
        indirect_qty_trx::number(18, 0) as indirect_qty_trx,
        indirect_cum_qty::number(18, 0) as indirect_cum_qty,
        indirect_promo_qty::number(18, 0) as indirect_promo_qty,
        indirect_cum_promo_qty::number(18, 0) as indirect_cum_promo_qty,
        indirect_basedline_total_qty::number(18, 0)
            as indirect_basedline_total_qty,
        indirect_cum_basedline_total_qty::number(18, 0)
            as indirect_cum_basedline_total_qty,
        indirect_qty_basedlined::number(18, 0) as indirect_qty_basedlined,
        pos_cust::varchar(50) as pos_cust,
        yearmonth::varchar(18) as yearmonth,
        run_id::number(18, 0) as run_id,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crtd_dttm,
        file_name::varchar(100) as file_name
    from source
)

select * from final
