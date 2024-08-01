with wk_iv_month_end as(
    select * from {{ ref('jpnwks_integration__wk_iv_month_end') }}
),
transformed as(
    SELECT cstm_cd,
        jan_cd,
        invt_dt,
        item_cd,
        pc,
        cs_qty,
        qty,
        proc_dt
    FROM wk_iv_month_end
)
select * from transformed