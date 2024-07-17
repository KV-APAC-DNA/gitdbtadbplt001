with wk_iv_month_end as(
    select * from DEV_DNA_CORE.SNAPJPNWKS_INTEGRATION.WK_IV_MONTH_END
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
    FROM WK_IV_MONTH_END
)
select * from transformed