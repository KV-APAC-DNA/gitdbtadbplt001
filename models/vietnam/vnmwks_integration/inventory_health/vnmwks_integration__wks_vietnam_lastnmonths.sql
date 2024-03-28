with wks_vietnam_allmonths_base as (
    select * from dev_dna_core.SNAPOSEWKS_INTEGRATION.wks_vietnam_allmonths_base
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
last_3_months as (
    SELECT base3.sap_parent_customer_key,
                    base3.sap_parent_customer_desc,
                    base3.matl_num,
                    mnth_id,
                    SUM(so_qty) AS last_3months_so_matl,
                    SUM(so_value) AS last_3months_so_value_matl
                FROM (
                        SELECT *
                        FROM wks_vietnam_allmonths_base
                        WHERE LEFT (MONTH, 4) >= (DATE_PART(YEAR, current_timestamp()::date) -6)
                    ) base3,
                    (
                        SELECT mnth_id,
                            third_month
                        FROM (
                                SELECT YEAR,
                                    mnth_id,
                                    LAG(mnth_id, 2) OVER (
                                        ORDER BY mnth_id
                                    ) third_month
                                FROM (
                                        SELECT DISTINCT "year" as year,
                                            mnth_id
                                        FROM edw_vw_os_time_dim
                                        WHERE "year" >= (DATE_PART(YEAR, current_timestamp()::date) -6)
                                    )
                            ) month_base
                    ) to_month
                WHERE MONTH <= mnth_id
                    AND MONTH >= third_month
                GROUP BY base3.sap_parent_customer_key,
                    base3.sap_parent_customer_desc,
                    base3.matl_num,
                    mnth_id
),
last_6_months as (
    SELECT base6.sap_parent_customer_key,
                    base6.sap_parent_customer_desc,
                    base6.matl_num,
                    mnth_id,
                    SUM(so_qty) AS last_6months_so_matl,
                    SUM(so_value) AS last_6months_so_value_matl
                FROM (
                        SELECT *
                        FROM wks_vietnam_allmonths_base
                        WHERE LEFT (MONTH, 4) >= (DATE_PART(YEAR, current_timestamp()::date) -6)
                    ) base6,
                    (
                        SELECT mnth_id,
                            sixth_month
                        FROM (
                                SELECT YEAR,
                                    mnth_id,
                                    LAG(mnth_id, 5) OVER (
                                        ORDER BY mnth_id
                                    ) sixth_month
                                FROM (
                                        SELECT DISTINCT "year" as year,
                                            mnth_id
                                        FROM edw_vw_os_time_dim
                                        WHERE "year" >= (DATE_PART(YEAR, current_timestamp()::date) -6)
                                    )
                            ) month_base
                    ) to_month
                WHERE MONTH <= mnth_id
                    AND MONTH >= sixth_month
                GROUP BY base6.sap_parent_customer_key,
                    base6.sap_parent_customer_desc,
                    base6.matl_num,
                    mnth_id
),
last_12_months as (
    SELECT base12.sap_parent_customer_key,
                    base12.sap_parent_customer_desc,
                    base12.matl_num,
                    mnth_id,
                    SUM(so_qty) AS last_12months_so_matl,
                    SUM(so_value) AS last_12months_so_value_matl
                FROM (
                        SELECT *
                        FROM wks_vietnam_allmonths_base
                        WHERE LEFT (MONTH, 4) >= (DATE_PART(YEAR, current_timestamp()::date) -6)
                    ) base12,
                    (
                        SELECT mnth_id,
                            twelfth_month
                        FROM (
                                SELECT YEAR,
                                    mnth_id,
                                    LAG(mnth_id, 11) OVER (
                                        ORDER BY mnth_id
                                    ) twelfth_month
                                FROM (
                                        SELECT DISTINCT "year" as year,
                                            mnth_id
                                        FROM edw_vw_os_time_dim
                                        WHERE "year" >= (DATE_PART(YEAR, current_timestamp()::date) -6)
                                    )
                            ) month_base
                    ) to_month
                WHERE MONTH <= mnth_id
                    AND MONTH >= twelfth_month
                GROUP BY base12.sap_parent_customer_key,
                    base12.sap_parent_customer_desc,
                    base12.matl_num,
                    mnth_id
),
last_36_months as (
    SELECT base36.sap_parent_customer_key,
                    base36.sap_parent_customer_desc,
                    base36.matl_num,
                    mnth_id,
                    SUM(so_qty) AS last_36months_so_matl,
                    SUM(so_value) AS last_36months_so_value_matl
                FROM (
                        SELECT *
                        FROM wks_vietnam_allmonths_base
                        WHERE LEFT (MONTH, 4) >= (DATE_PART(YEAR, current_timestamp()::date) -6)
                    ) base36,
                    (
                        SELECT mnth_id,
                            thirtysixth_month
                        FROM (
                                SELECT YEAR,
                                    mnth_id,
                                    LAG(mnth_id, 35) OVER (
                                        ORDER BY mnth_id
                                    ) thirtysixth_month
                                FROM (
                                        SELECT DISTINCT "year" as year,
                                            mnth_id
                                        FROM edw_vw_os_time_dim
                                        WHERE "year" >= (DATE_PART(YEAR, current_timestamp()::date) -6)
                                    )
                            ) month_base
                    ) to_month
                WHERE MONTH <= mnth_id
                    AND MONTH >= thirtysixth_month
                GROUP BY base36.sap_parent_customer_key,
                    base36.sap_parent_customer_desc,
                    base36.matl_num,
                    mnth_id
),
transformed as (
    SELECT base.sap_parent_customer_key,
            base.sap_parent_customer_desc,
            base.matl_num,
            base.month,
            so_qty,
            so_value,
            inv_qty,
            inv_value,
            sell_in_qty,
            sell_in_value,
            last_3_months.last_3months_so_matl AS last_3months_so,
            last_3_months.last_3months_so_value_matl AS last_3months_so_value,
            last_6_months.last_6months_so_matl AS last_6months_so,
            last_6_months.last_6months_so_value_matl AS last_6months_so_value,
            last_12_months.last_12months_so_matl AS last_12months_so,
            last_12_months.last_12months_so_value_matl AS last_12months_so_value,
            last_36_months.last_36months_so_matl AS last_36months_so,
            last_36_months.last_36months_so_value_matl AS last_36months_so_value
    FROM wks_vietnam_allmonths_base base,
    last_3_months,
    last_6_months,
    last_12_months,
    last_36_months
    WHERE base.sap_parent_customer_key = last_3_months.sap_parent_customer_key(+)
            AND base.sap_parent_customer_desc = last_3_months.sap_parent_customer_desc(+)
            AND base.matl_num = last_3_months.matl_num(+)
            AND base.month = last_3_months.mnth_id(+)
            AND base.sap_parent_customer_key = last_6_months.sap_parent_customer_key(+)
            AND base.sap_parent_customer_desc = last_6_months.sap_parent_customer_desc(+)
            AND base.matl_num = last_6_months.matl_num(+)
            AND base.month = last_6_months.mnth_id(+)
            AND base.sap_parent_customer_key = last_12_months.sap_parent_customer_key(+)
            AND base.sap_parent_customer_desc = last_12_months.sap_parent_customer_desc(+)
            AND base.matl_num = last_12_months.matl_num(+)
            AND base.month = last_12_months.mnth_id(+)
            AND base.sap_parent_customer_key = last_36_months.sap_parent_customer_key(+)
            AND base.sap_parent_customer_desc = last_36_months.sap_parent_customer_desc(+)
            AND base.matl_num = last_36_months.matl_num(+)
            AND base.month = last_36_months.mnth_id(+)
),
final as (
    SELECT sap_parent_customer_key,
    sap_parent_customer_desc,
    COALESCE(NULLIF(matl_num, ''), 'NA') AS matl_num,
    month,
    SUM(so_qty) so_qty,
    SUM(so_value) so_value,
    SUM(inv_qty) inv_qty,
    SUM(inv_value) inv_value,
    SUM(sell_in_qty) sell_in_qty,
    SUM(sell_in_value) sell_in_value,
    SUM(last_3months_so) AS last_3months_so,
    SUM(last_3months_so_value) AS last_3months_so_value,
    SUM(last_6months_so) AS last_6months_so,
    SUM(last_6months_so_value) AS last_6months_so_value,
    SUM(last_12months_so) AS last_12months_so,
    SUM(last_12months_so_value) AS last_12months_so_value,
    SUM(last_36months_so) AS last_36months_so,
    SUM(last_36months_so_value) AS last_36months_so_value
    from transformed
    GROUP BY sap_parent_customer_key,
    sap_parent_customer_desc,
    matl_num,
    month
)
select * from final