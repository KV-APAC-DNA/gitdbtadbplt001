with EDW_VW_OS_TIME_DIM as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
wks_vietnam_base_detail as (
    select * from dev_dna_core.SNAPOSEWKS_INTEGRATION.wks_vietnam_base_detail
),
filtered_wvbd as (
    SELECT sap_parent_customer_key,
            sap_parent_customer_desc,
            MONTH,
            SUM(so_qty) so_qty,
            SUM(so_value) so_value,
            SUM(inv_qty) inv_qty,
            SUM(inv_value) inv_value
        FROM wks_vietnam_base_detail
        WHERE MONTH <= (
                SELECT DISTINCT MNTH_ID
                FROM EDW_VW_OS_TIME_DIM
                WHERE cal_date = to_date(current_timestamp())
            )
        GROUP BY sap_parent_customer_key,
            sap_parent_customer_desc,
            MONTH
),
final as (
    SELECT sap_parent_customer_key,
    sap_parent_customer_desc,
    month,
    so_qty,
    so_value,
    inv_qty,
    inv_value,
    CASE
        WHEN month >(
            SELECT third_month
            FROM (
                    SELECT mnth_id,
                        LAG(mnth_id, 2) OVER (
                            ORDER BY mnth_id
                        ) third_month
                    FROM (
                            SELECT DISTINCT "year",
                                mnth_id
                            FROM EDW_VW_OS_TIME_DIM
                            WHERE mnth_id <= (
                                    SELECT DISTINCT MNTH_ID
                                    FROM EDW_VW_OS_TIME_DIM
                                    WHERE cal_date = to_date(current_timestamp())
                                )
                        )
                )
            WHERE mnth_id = (
                    SELECT DISTINCT MNTH_ID
                    FROM EDW_VW_OS_TIME_DIM
                    WHERE cal_date = to_date(current_timestamp())
                )
        )
        AND (
            nvl (so_value, 0) = 0
            OR nvl (inv_value, 0) = 0
        ) THEN 'Y'
        ELSE 'N'
    END AS propagate_flag,
    MAX(month) OVER (PARTITION BY sap_parent_customer_key) latest_month
    FROM filtered_wvbd
)
select * from final