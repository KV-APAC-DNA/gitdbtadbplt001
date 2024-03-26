with EDW_VW_OS_TIME_DIM as (

),
wks_vietnam_base_detail as (

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
                WHERE cal_date = TRUNC(convert_timezone('SGT', sysdate))
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
            -- mnth_id,
            SELECT third_month
            FROM (
                    SELECT mnth_id,
                        LAG(mnth_id, 2) OVER (
                            ORDER BY mnth_id
                        ) third_month
                    FROM (
                            SELECT DISTINCT YEAR,
                                mnth_id
                            FROM EDW_VW_OS_TIME_DIM
                            WHERE mnth_id <= (
                                    SELECT DISTINCT MNTH_ID
                                    FROM EDW_VW_OS_TIME_DIM
                                    WHERE cal_date = TRUNC(convert_timezone('SGT', sysdate))
                                )
                        )
                )
            WHERE mnth_id = (
                    SELECT DISTINCT MNTH_ID
                    FROM EDW_VW_OS_TIME_DIM
                    WHERE cal_date = TRUNC(convert_timezone('SGT', sysdate))
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