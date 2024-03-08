with edw_vw_os_time_dim as(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
wks_th_watsons_allmonths_base as(
    select * from {{ ref('thawks_integration__wks_th_watsons_allmonths_base') }}
),
last_3_months as(
    SELECT
      base3.sap_parent_customer_key,
      base3.sap_parent_customer_desc,
      base3.matl_num,
      mnth_id,
      third_month as third_month,
      SUM(so_qty) AS last_3months_so_matl,
      SUM(inv_qty) AS last_3months_inv_matl,
      SUM(inv_value) AS last_3months_inv_value_matl,
      SUM(sell_in_qty) AS last_3months_si_matl,
      SUM(sell_in_value) AS last_3months_si_value_matl,
      SUM(so_value) AS last_3months_so_value_matl
    FROM (
      SELECT
        *
      FROM wks_th_watsons_allmonths_base
      WHERE
        LEFT(month, 4) >= (
          DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
        )
    ) AS base3, (
      SELECT
        mnth_id,
        third_month
      FROM (
        SELECT
          year,
          mnth_id,
          LAG(mnth_id, 3) OVER (ORDER BY mnth_id) AS third_month
        FROM (
          SELECT DISTINCT
            cal_year AS year,
            cal_mnth_id AS mnth_id
          FROM edw_vw_os_time_dim
          WHERE
            cal_year >= (
              DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
            )
        )
      ) AS month_base
    ) AS to_month
    WHERE
      month = third_month
    GROUP BY
      base3.sap_parent_customer_key,
      base3.sap_parent_customer_desc,
      base3.matl_num,
      mnth_id,
      third_month
),
last_6_months as(
    SELECT
      base6.sap_parent_customer_key,
      base6.sap_parent_customer_desc,
      base6.matl_num,
      mnth_id,
      sixth_month,
      SUM(so_qty) AS last_6months_so_matl,
      SUM(inv_qty) AS last_6months_inv_matl,
      SUM(inv_value) AS last_6months_inv_value_matl,
      SUM(sell_in_qty) AS last_6months_si_matl,
      SUM(sell_in_value) AS last_6months_si_value_matl,
      SUM(so_value) AS last_6months_so_value_matl
    FROM (
      SELECT
        *
      FROM wks_th_watsons_allmonths_base
      WHERE
        LEFT(month, 4) >= (
          DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
        )
    ) AS base6, (
      SELECT
        mnth_id,
        sixth_month
      FROM (
        SELECT
          year,
          mnth_id,
          LAG(mnth_id, 6) OVER (ORDER BY mnth_id) AS sixth_month
        FROM (
          SELECT DISTINCT
            cal_year AS year,
            cal_mnth_id AS mnth_id
          FROM edw_vw_os_time_dim
          WHERE
            cal_year >= (
              DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
            )
        )
      ) AS month_base
    ) AS to_month
    WHERE
      month = sixth_month
    GROUP BY
      base6.sap_parent_customer_key,
      base6.sap_parent_customer_desc,
      base6.matl_num,
      mnth_id,
      sixth_month
),
last_12_months as(
    SELECT
      base12.sap_parent_customer_key,
      base12.sap_parent_customer_desc,
      base12.matl_num,
      mnth_id,
      twelfth_month,
      SUM(so_qty) AS last_12months_so_matl,
      SUM(inv_qty) AS last_12months_inv_matl,
      SUM(inv_value) AS last_12months_inv_value_matl,
      SUM(sell_in_qty) AS last_12months_si_matl,
      SUM(sell_in_value) AS last_12months_si_value_matl,
      SUM(so_value) AS last_12months_so_value_matl
    FROM (
      SELECT
        *
      FROM wks_th_watsons_allmonths_base
      WHERE
        LEFT(month, 4) >= (
          DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
        )
    ) AS base12, (
      SELECT
        mnth_id,
        twelfth_month
      FROM (
        SELECT
          year,
          mnth_id,
          LAG(mnth_id, 9) OVER (ORDER BY mnth_id) AS twelfth_month
        FROM (
          SELECT DISTINCT
            cal_year AS year,
            cal_mnth_id AS mnth_id
          FROM edw_vw_os_time_dim /* limit 100 */
          WHERE
            cal_year >= (
              DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
            )
        )
      ) AS month_base
    ) AS to_month
    WHERE
      month = twelfth_month
    GROUP BY
      base12.sap_parent_customer_key,
      base12.sap_parent_customer_desc,
      base12.matl_num,
      mnth_id,
      twelfth_month
),
last_15_months as(
    SELECT
      base15.sap_parent_customer_key,
      base15.sap_parent_customer_desc,
      base15.matl_num,
      mnth_id,
      fifteenth_month,
      SUM(so_qty) AS last_15months_so_matl,
      SUM(inv_qty) AS last_15months_inv_matl,
      SUM(inv_value) AS last_15months_inv_value_matl,
      SUM(sell_in_qty) AS last_15months_si_matl,
      SUM(sell_in_value) AS last_15months_si_value_matl,
      SUM(so_value) AS last_15months_so_value_matl
    FROM (
      SELECT
        *
      FROM wks_th_watsons_allmonths_base
      WHERE
        LEFT(month, 4) >= (
          DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
        )
    ) AS base15, (
      SELECT
        mnth_id,
        fifteenth_month
      FROM (
        SELECT
          year,
          mnth_id,
          LAG(mnth_id, 12) OVER (ORDER BY mnth_id) AS fifteenth_month
        FROM (
          SELECT DISTINCT
            cal_year AS year,
            cal_mnth_id AS mnth_id
          FROM edw_vw_os_time_dim /* limit 100 */
          WHERE
            cal_year >= (
              DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
            )
        )
      ) AS month_base
    ) AS to_month
    WHERE
      month = fifteenth_month
    GROUP BY
      base15.sap_parent_customer_key,
      base15.sap_parent_customer_desc,
      base15.matl_num,
      mnth_id,
      fifteenth_month
),
last_18_months as(
    SELECT
      base18.sap_parent_customer_key,
      base18.sap_parent_customer_desc,
      base18.matl_num,
      mnth_id,
      eightteenth_month,
      SUM(so_qty) AS last_18months_so_matl,
      SUM(inv_qty) AS last_18months_inv_matl,
      SUM(inv_value) AS last_18months_inv_value_matl,
      SUM(sell_in_qty) AS last_18months_si_matl,
      SUM(sell_in_value) AS last_18months_si_value_matl,
      SUM(so_value) AS last_18months_so_value_matl
    FROM (
      SELECT
        *
      FROM wks_th_watsons_allmonths_base
      WHERE
        LEFT(month, 4) >= (
          DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
        )
    ) AS base18, (
      SELECT
        mnth_id,
        eightteenth_month
      FROM (
        SELECT
          year,
          mnth_id,
          LAG(mnth_id, 15) OVER (ORDER BY mnth_id) AS eightteenth_month
        FROM (
          SELECT DISTINCT
            cal_year AS year,
            cal_mnth_id AS mnth_id
          FROM edw_vw_os_time_dim /* limit 100 */
          WHERE
            cal_year >= (
              DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
            )
        )
      ) AS month_base
    ) AS to_month
    WHERE
      month = eightteenth_month
    GROUP BY
      base18.sap_parent_customer_key,
      base18.sap_parent_customer_desc,
      base18.matl_num,
      mnth_id,
      eightteenth_month
),
last_21_months as(
    SELECT
      base21.sap_parent_customer_key,
      base21.sap_parent_customer_desc,
      base21.matl_num,
      mnth_id,
      twentyfirst_month,
      SUM(so_qty) AS last_21months_so_matl,
      SUM(inv_qty) AS last_21months_inv_matl,
      SUM(inv_value) AS last_21months_inv_value_matl,
      SUM(sell_in_qty) AS last_21months_si_matl,
      SUM(sell_in_value) AS last_21months_si_value_matl,
      SUM(so_value) AS last_21months_so_value_matl
    FROM (
      SELECT
        *
      FROM wks_th_watsons_allmonths_base
      WHERE
        LEFT(month, 4) >= (
          DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
        )
    ) AS base21, (
      SELECT
        mnth_id,
        twentyfirst_month
      FROM (
        SELECT
          year,
          mnth_id,
          LAG(mnth_id, 18) OVER (ORDER BY mnth_id) AS twentyfirst_month
        FROM (
          SELECT DISTINCT
            cal_year AS year,
            cal_mnth_id AS mnth_id
          FROM edw_vw_os_time_dim /* limit 100 */
          WHERE
            cal_year >= (
              DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
            )
        )
      ) AS month_base
    ) AS to_month
    WHERE
      month = twentyfirst_month
    GROUP BY
      base21.sap_parent_customer_key,
      base21.sap_parent_customer_desc,
      base21.matl_num,
      mnth_id,
      twentyfirst_month
),
last_24_months as(
    SELECT
      base24.sap_parent_customer_key,
      base24.sap_parent_customer_desc,
      base24.matl_num,
      mnth_id,
      twentyfourth_month,
      SUM(so_qty) AS last_24months_so_matl,
      SUM(inv_qty) AS last_24months_inv_matl,
      SUM(inv_value) AS last_24months_inv_value_matl,
      SUM(sell_in_qty) AS last_24months_si_matl,
      SUM(sell_in_value) AS last_24months_si_value_matl,
      SUM(so_value) AS last_24months_so_value_matl
    FROM (
      SELECT
        *
      FROM wks_th_watsons_allmonths_base
      WHERE
        LEFT(month, 4) >= (
          DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
        )
    ) AS base24, (
      SELECT
        mnth_id,
        twentyfourth_month
      FROM (
        SELECT
          year,
          mnth_id,
          LAG(mnth_id, 21) OVER (ORDER BY mnth_id) AS twentyfourth_month
        FROM (
          SELECT DISTINCT
            cal_year AS year,
            cal_mnth_id AS mnth_id
          FROM edw_vw_os_time_dim /* limit 100 */
          WHERE
            cal_year >= (
              DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
            )
        )
      ) AS month_base
    ) AS to_month
    WHERE
      month = twentyfourth_month
    GROUP BY
      base24.sap_parent_customer_key,
      base24.sap_parent_customer_desc,
      base24.matl_num,
      mnth_id,
      twentyfourth_month
),
last_27_months as(
    SELECT
      base27.sap_parent_customer_key,
      base27.sap_parent_customer_desc,
      base27.matl_num,
      mnth_id,
      twentyseventh_month,
      SUM(so_qty) AS last_27months_so_matl,
      SUM(inv_qty) AS last_27months_inv_matl,
      SUM(inv_value) AS last_27months_inv_value_matl,
      SUM(sell_in_qty) AS last_27months_si_matl,
      SUM(sell_in_value) AS last_27months_si_value_matl,
      SUM(so_value) AS last_27months_so_value_matl
    FROM (
      SELECT
        *
      FROM wks_th_watsons_allmonths_base
      WHERE
        LEFT(month, 4) >= (
          DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
        )
    ) AS base27, (
      SELECT
        mnth_id,
        twentyseventh_month
      FROM (
        SELECT
          year,
          mnth_id,
          LAG(mnth_id, 24) OVER (ORDER BY mnth_id) AS twentyseventh_month
        FROM (
          SELECT DISTINCT
            cal_year AS year,
            cal_mnth_id AS mnth_id
          FROM edw_vw_os_time_dim /* limit 100 */
          WHERE
            cal_year >= (
              DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
            )
        )
      ) AS month_base
    ) AS to_month
    WHERE
      month = twentyseventh_month
    GROUP BY
      base27.sap_parent_customer_key,
      base27.sap_parent_customer_desc,
      base27.matl_num,
      mnth_id,
      twentyseventh_month
),
last_30_months as(
    SELECT
      base30.sap_parent_customer_key,
      base30.sap_parent_customer_desc,
      base30.matl_num,
      mnth_id,
      thirtieth_month,
      SUM(so_qty) AS last_30months_so_matl,
      SUM(inv_qty) AS last_30months_inv_matl,
      SUM(inv_value) AS last_30months_inv_value_matl,
      SUM(sell_in_qty) AS last_30months_si_matl,
      SUM(sell_in_value) AS last_30months_si_value_matl,
      SUM(so_value) AS last_30months_so_value_matl
    FROM (
      SELECT
        *
      FROM wks_th_watsons_allmonths_base
      WHERE
        LEFT(month, 4) >= (
          DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
        )
    ) AS base30, (
      SELECT
        mnth_id,
        thirtieth_month
      FROM (
        SELECT
          year,
          mnth_id,
          LAG(mnth_id, 27) OVER (ORDER BY mnth_id) AS thirtieth_month
        FROM (
          SELECT DISTINCT
            cal_year AS year,
            cal_mnth_id AS mnth_id
          FROM edw_vw_os_time_dim /* limit 100 */
          WHERE
            cal_year >= (
              DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
            )
        )
      ) AS month_base
    ) AS to_month
    WHERE
      month = thirtieth_month
    GROUP BY
      base30.sap_parent_customer_key,
      base30.sap_parent_customer_desc,
      base30.matl_num,
      mnth_id,
      thirtieth_month
),
last_33_months as(
    SELECT
      base33.sap_parent_customer_key,
      base33.sap_parent_customer_desc,
      base33.matl_num,
      mnth_id,
      thirtythird_month,
      SUM(so_qty) AS last_33months_so_matl,
      SUM(inv_qty) AS last_33months_inv_matl,
      SUM(inv_value) AS last_33months_inv_value_matl,
      SUM(sell_in_qty) AS last_33months_si_matl,
      SUM(sell_in_value) AS last_33months_si_value_matl,
      SUM(so_value) AS last_33months_so_value_matl
    FROM (
      SELECT
        *
      FROM wks_th_watsons_allmonths_base
      WHERE
        LEFT(month, 4) >= (
          DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
        )
    ) AS base33, (
      SELECT
        mnth_id,
        thirtythird_month
      FROM (
        SELECT
          year,
          mnth_id,
          LAG(mnth_id, 30) OVER (ORDER BY mnth_id) AS thirtythird_month
        FROM (
          SELECT DISTINCT
            cal_year AS year,
            cal_mnth_id AS mnth_id
          FROM edw_vw_os_time_dim /* limit 100 */
          WHERE
            cal_year >= (
              DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
            )
        )
      ) AS month_base
    ) AS to_month
    WHERE
      month = thirtythird_month
    GROUP BY
      base33.sap_parent_customer_key,
      base33.sap_parent_customer_desc,
      base33.matl_num,
      mnth_id,
      thirtythird_month
),
last_36_months as(
    SELECT
      base36.sap_parent_customer_key,
      base36.sap_parent_customer_desc,
      base36.matl_num,
      mnth_id,
      thirtysixth_month,
      SUM(so_qty) AS last_36months_so_matl,
      SUM(inv_qty) AS last_36months_inv_matl,
      SUM(inv_value) AS last_36months_inv_value_matl,
      SUM(sell_in_qty) AS last_36months_si_matl,
      SUM(sell_in_value) AS last_36months_si_value_matl,
      SUM(so_value) AS last_36months_so_value_matl
    FROM (
      SELECT
        *
      FROM wks_th_watsons_allmonths_base
      WHERE
        LEFT(month, 4) >= (
          DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
        )
    ) AS base36, (
      SELECT
        mnth_id,
        thirtysixth_month
      FROM (
        SELECT
          year,
          mnth_id,
          LAG(mnth_id, 33) OVER (ORDER BY mnth_id) AS thirtysixth_month
        FROM (
          SELECT DISTINCT
            cal_year AS year,
            cal_mnth_id AS mnth_id
          FROM edw_vw_os_time_dim /* limit 100 */
          WHERE
            cal_year >= (
              DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6
            )
        )
      ) AS month_base
    ) AS to_month
    WHERE
      month = thirtysixth_month
    GROUP BY
      base36.sap_parent_customer_key,
      base36.sap_parent_customer_desc,
      base36.matl_num,
      mnth_id,
      thirtysixth_month
),
transformed as(
    SELECT
        sap_parent_customer_key,
        sap_parent_customer_desc,
        COALESCE(NULLIF(matl_num, ''), 'NA') AS matl_num,
        month, /* third_month,sixth_month,twelfth_month, */
        SUM(so_qty) AS so_qty,
        SUM(so_value) AS so_value,
        SUM(inv_qty) AS inv_qty,
        SUM(inv_value) AS inv_value,
        SUM(sell_in_qty) AS sell_in_qty,
        SUM(sell_in_value) AS sell_in_value,
        SUM(last_3months_so) AS last_3months_so,
        SUM(last_3months_so_value) AS last_3months_so_value,
        SUM(last_6months_so) AS last_6months_so,
        SUM(last_6months_so_value) AS last_6months_so_value,
        SUM(last_12months_so) AS last_12months_so,
        SUM(last_12months_so_value) AS last_12months_so_value,
        SUM(last_15months_so_value) AS last_15months_so_value,
        SUM(last_18months_so_value) AS last_18months_so_value,
        SUM(last_21months_so_value) AS last_21months_so_value,
        SUM(last_24months_so_value) AS last_24months_so_value,
        SUM(last_27months_so_value) AS last_27months_so_value,
        SUM(last_30months_so_value) AS last_30months_so_value,
        SUM(last_33months_so_value) AS last_33months_so_value,
        SUM(last_36months_so_value) AS last_36months_so_value
    FROM(
        SELECT
            base.sap_parent_customer_key,
            base.sap_parent_customer_desc,
            base.matl_num,
            base.month,
            last_3_months.third_month,
            last_6_months.sixth_month,
            last_12_months.twelfth_month,
            last_15_months.fifteenth_month,
            last_18_months.eightteenth_month,
            last_21_months.twentyfirst_month,
            last_24_months.twentyfourth_month,
            last_27_months.twentyseventh_month,
            last_30_months.thirtieth_month,
            last_33_months.thirtythird_month,
            last_36_months.thirtysixth_month,
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
            last_15_months.last_15months_so_value_matl AS last_15months_so_value,
            last_18_months.last_18months_so_value_matl AS last_18months_so_value,
            last_21_months.last_21months_so_value_matl AS last_21months_so_value,
            last_24_months.last_24months_so_value_matl AS last_24months_so_value,
            last_27_months.last_27months_so_value_matl AS last_27months_so_value,
            last_30_months.last_30months_so_value_matl AS last_30months_so_value,
            last_33_months.last_33months_so_value_matl AS last_33months_so_value,
            last_36_months.last_36months_so_value_matl AS last_36months_so_value
        FROM wks_th_watsons_allmonths_base as base,
        last_3_months, 
        last_6_months, 
        last_12_months, 
        last_15_months, 
        last_18_months, 
        last_21_months, 
        last_24_months, 
        last_27_months, 
        last_30_months, 
        last_33_months, 
        last_36_months
        WHERE
            base.sap_parent_customer_key = last_3_months.SAP_PARENT_CUSTOMER_KEY(+)
            AND base.sap_parent_customer_desc = last_3_months.SAP_PARENT_CUSTOMER_DESC(+)
            AND base.matl_num = last_3_months.MATL_NUM(+)
            AND base.month = last_3_months.MNTH_ID(+)
            AND base.sap_parent_customer_key = last_6_months.SAP_PARENT_CUSTOMER_KEY(+)
            AND base.sap_parent_customer_desc = last_6_months.SAP_PARENT_CUSTOMER_DESC(+)
            AND base.matl_num = last_6_months.MATL_NUM(+)
            AND base.month = last_6_months.MNTH_ID(+)
            AND base.sap_parent_customer_key = last_12_months.SAP_PARENT_CUSTOMER_KEY(+)
            AND base.sap_parent_customer_desc = last_12_months.SAP_PARENT_CUSTOMER_DESC(+)
            AND base.matl_num = last_12_months.MATL_NUM(+)
            AND base.month = last_12_months.MNTH_ID(+)
            AND base.sap_parent_customer_key = last_15_months.SAP_PARENT_CUSTOMER_KEY(+)
            AND base.sap_parent_customer_desc = last_15_months.SAP_PARENT_CUSTOMER_DESC(+)
            AND base.matl_num = last_15_months.MATL_NUM(+)
            AND base.month = last_15_months.MNTH_ID(+)
            AND base.sap_parent_customer_key = last_18_months.SAP_PARENT_CUSTOMER_KEY(+)
            AND base.sap_parent_customer_desc = last_18_months.SAP_PARENT_CUSTOMER_DESC(+)
            AND base.matl_num = last_18_months.MATL_NUM(+)
            AND base.month = last_18_months.MNTH_ID(+)
            AND base.sap_parent_customer_key = last_21_months.SAP_PARENT_CUSTOMER_KEY(+)
            AND base.sap_parent_customer_desc = last_21_months.SAP_PARENT_CUSTOMER_DESC(+)
            AND base.matl_num = last_21_months.MATL_NUM(+)
            AND base.month = last_21_months.MNTH_ID(+)
            AND base.sap_parent_customer_key = last_24_months.SAP_PARENT_CUSTOMER_KEY(+)
            AND base.sap_parent_customer_desc = last_24_months.SAP_PARENT_CUSTOMER_DESC(+)
            AND base.matl_num = last_24_months.MATL_NUM(+)
            AND base.month = last_24_months.MNTH_ID(+)
            AND base.sap_parent_customer_key = last_27_months.SAP_PARENT_CUSTOMER_KEY(+)
            AND base.sap_parent_customer_desc = last_27_months.SAP_PARENT_CUSTOMER_DESC(+)
            AND base.matl_num = last_27_months.MATL_NUM(+)
            AND base.month = last_27_months.MNTH_ID(+)
            AND base.sap_parent_customer_key = last_30_months.SAP_PARENT_CUSTOMER_KEY(+)
            AND base.sap_parent_customer_desc = last_30_months.SAP_PARENT_CUSTOMER_DESC(+)
            AND base.matl_num = last_30_months.MATL_NUM(+)
            AND base.month = last_30_months.MNTH_ID(+)
            AND base.sap_parent_customer_key = last_33_months.SAP_PARENT_CUSTOMER_KEY(+)
            AND base.sap_parent_customer_desc = last_33_months.SAP_PARENT_CUSTOMER_DESC(+)
            AND base.matl_num = last_33_months.MATL_NUM(+)
            AND base.month = last_33_months.MNTH_ID(+)
            AND base.sap_parent_customer_key = last_36_months.SAP_PARENT_CUSTOMER_KEY(+)
            AND base.sap_parent_customer_desc = last_36_months.SAP_PARENT_CUSTOMER_DESC(+)
            AND base.matl_num = last_36_months.MATL_NUM(+)
            AND base.month = last_36_months.MNTH_ID(+)
    )
    GROUP BY
    sap_parent_customer_key,
    sap_parent_customer_desc,
    matl_num,
    month
),
final as(
    select 
    	sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
	sap_parent_customer_desc::varchar(50) as sap_parent_customer_desc,
	matl_num::varchar(1500) as matl_num,
	month::number(18,0) as month,
	so_qty::number(38,4) as so_qty,
	so_value::number(38,8) as so_value,
	inv_qty::number(38,4) as inv_qty,
	inv_value::number(38,8) as inv_value,
	sell_in_qty::number(38,4) as sell_in_qty,
	sell_in_value::number(38,4) as sell_in_value,
	last_3months_so::number(38,4) as last_3months_so,
	last_3months_so_value::number(38,8) as last_3months_so_value,
	last_6months_so::number(38,4) as last_6months_so,
	last_6months_so_value::number(38,8) as last_6months_so_value,
	last_12months_so::number(38,4) as last_12months_so,
	last_12months_so_value::number(38,8) as last_12months_so_value,
	last_15months_so_value::number(38,8) as last_15months_so_value,
	last_18months_so_value::number(38,8) as last_18months_so_value,
	last_21months_so_value::number(38,8) as last_21months_so_value,
	last_24months_so_value::number(38,8) as last_24months_so_value,
	last_27months_so_value::number(38,8) as last_27months_so_value,
	last_30months_so_value::number(38,8) as last_30months_so_value,
	last_33months_so_value::number(38,8) as last_33months_so_value,
	last_36months_so_value::number(38,8) as last_36months_so_value
    from transformed
)
select * from final