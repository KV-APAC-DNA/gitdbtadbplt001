with wks_th_watsons_base_detail as(
    select * from {{ ref('thawks_integration__wks_th_watsons_base_detail') }}
),
edw_vw_os_time_dim as(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
trans as(
    SELECT
        sap_parent_customer_key,
        sap_parent_customer_desc,
        month,
        SUM(so_qty) AS so_qty,
        SUM(so_value) AS so_value,
        SUM(inv_qty) AS inv_qty,
        SUM(inv_value) AS inv_value
    FROM wks_th_watsons_base_detail
    WHERE
        month <= (
        SELECT DISTINCT
            cal_mnth_id
        FROM EDW_VW_OS_TIME_DIM
        WHERE
            cal_date = CURRENT_DATE
        )
    GROUP BY
        sap_parent_customer_key,
        sap_parent_customer_desc,
        month
),
trans2 as(
    SELECT 
        DISTINCT cal_YEAR AS year,
        cal_mnth_id AS mnth_id
    FROM EDW_VW_OS_TIME_DIM
    WHERE
    cal_mnth_id <= (
        SELECT DISTINCT
            cal_mnth_id
        FROM EDW_VW_OS_TIME_DIM
        WHERE
        cal_date = CURRENT_DATE
    )
),
transformed as(
    SELECT
        sap_parent_customer_key,
        sap_parent_customer_desc,
        month,
        so_qty,
        so_value,
        inv_qty,
        inv_value,
        CASE
            WHEN month > (
            SELECT
                third_month
            FROM (
                SELECT
                mnth_id,
                LAG(mnth_id, 2) OVER (ORDER BY mnth_id) AS third_month
                FROM trans2
            )
            WHERE
                mnth_id = (
                SELECT DISTINCT
                    cal_mnth_id
                FROM EDW_VW_OS_TIME_DIM
                WHERE
                    cal_date = CURRENT_DATE
                )
            )
            AND (
            COALESCE(so_value, 0) = 0 OR COALESCE(inv_value, 0) = 0
            )
            THEN 'Y'
            ELSE 'N'
        END AS propagate_flag,
        MAX(month) OVER (PARTITION BY sap_parent_customer_key, sap_parent_customer_desc) AS latest_month
    FROM trans
),
final as(
    select 
    	sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
        sap_parent_customer_desc::varchar(50) as sap_parent_customer_desc,
        month::number(18,0) as month,
        so_qty::number(38,4) as so_qty,
        so_value::number(38,8) as so_value,
        inv_qty::number(38,4) as inv_qty,
        inv_value::number(38,8) as inv_value,
        propagate_flag::varchar(1) as propagate_flag,
        latest_month::number(18,0) as latest_month
    from transformed
)
select * from final