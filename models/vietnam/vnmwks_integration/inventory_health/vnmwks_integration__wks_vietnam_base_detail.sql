with wks_vietnam_lastnmonths as (
    select * from dev_dna_core.VNMWKS_INTEGRATION.wks_vietnam_lastnmonths
),
wks_vietnam_base as (
    select * from dev_dna_core.VNMWKS_INTEGRATION.wks_vietnam_base
),
edw_vw_os_time_dim as (
    select * from dev_dna_core.SGPITG_INTEGRATION.edw_vw_os_time_dim
),
transformed as (
    SELECT agg.sap_parent_customer_key,
            agg.sap_parent_customer_desc,
            agg.matl_num,
            agg.month,
            base.matl_num AS base_matl_num,
            base.so_qty,
            base.so_value,
            base.inv_qty,
            base.inv_value,
            base.sell_in_qty,
            base.sell_in_value,
            agg.last_3months_so,
            agg.last_6months_so,
            agg.last_12months_so,
            agg.last_3months_so_value,
            agg.last_6months_so_value,
            agg.last_12months_so_value,
            agg.last_36months_so_value,
            CASE
                WHEN (base.matl_num IS NULL) THEN 'Y'
                ELSE 'N'
            END AS replicated_flag
        FROM wks_vietnam_lastnmonths agg,
            wks_vietnam_base base
        WHERE LEFT (agg.month, 4) >= (DATE_PART(YEAR, SYSDATE) -2)
            AND agg.sap_parent_customer_key = base.sap_parent_customer_key(+)
            AND agg.sap_parent_customer_desc = base.sap_parent_customer_desc(+)
            AND agg.matl_num = base.matl_num(+)
            AND agg.month = base.month(+)
            AND agg.month <= (
                SELECT DISTINCT mnth_id
                FROM edw_vw_os_time_dim
                WHERE cal_date = TRUNC(sysdate)
            )
),
final as (
    SELECT sap_parent_customer_key,
    sap_parent_customer_desc,
    matl_num,
    month,
    matl_num AS base_matl_num,
    replicated_flag,
    SUM(so_qty) so_qty,
    SUM(so_value) so_value,
    SUM(inv_qty) inv_qty,
    SUM(inv_value) inv_value,
    SUM(sell_in_qty) sell_in_qty,
    SUM(sell_in_value) sell_in_value,
    SUM(last_3months_so) last_3months_so,
    SUM(last_6months_so) last_6months_so,
    SUM(last_12months_so) last_12months_so,
    SUM(last_3months_so_value) last_3months_so_value,
    SUM(last_6months_so_value) last_6months_so_value,
    SUM(last_12months_so_value) last_12months_so_value,
    SUM(last_36months_so_value) last_36months_so_value
    from transformed
    GROUP BY sap_parent_customer_key,
    sap_parent_customer_desc,
    matl_num,
    month,
    replicated_flag
)
select * from final