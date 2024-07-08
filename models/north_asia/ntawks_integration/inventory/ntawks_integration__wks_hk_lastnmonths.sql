with wks_hk_allmonths_base
as (
  select *
  from {{ ref('ntawks_integration__wks_hk_allmonths_base') }}
  ),
edw_vw_os_time_dim
as (
  select *
  from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
  ),
transformed
AS (
  SELECT sap_parent_customer_key,
    coalesce(nullif(matl_num, ''), 'NA') AS matl_num,
    month,
    sum(so_qty) so_qty,
    sum(so_value) so_value,
    sum(inv_qty) inv_qty,
    sum(inv_value) inv_value,
    sum(sell_in_qty) sell_in_qty,
    sum(sell_in_value) sell_in_value,
    sum(last_3months_so) AS last_3months_so,
    sum(last_3months_so_value) AS last_3months_so_value,
    sum(last_6months_so) AS last_6months_so,
    sum(last_6months_so_value) AS last_6months_so_value,
    sum(last_12months_so) AS last_12months_so,
    sum(last_12months_so_value) AS last_12months_so_value,
    sum(last_36months_so) AS last_36months_so,
    sum(last_36months_so_value) AS last_36months_so_value
  FROM (
    SELECT base.sap_parent_customer_key,
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
    FROM WKS_HK_allmonths_base base,
      (
        SELECT base3.sap_parent_customer_key,
          base3.matl_num,
          mnth_id,
          sum(so_qty) AS last_3months_so_matl,
          sum(so_value) AS last_3months_so_value_matl
        FROM (
          SELECT *
          FROM WKS_HK_allmonths_base
          WHERE left(month, 4) >= (DATE_PART(YEAR, convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)) - 6)
          ) base3,
          (
            SELECT mnth_id,
              third_month
            FROM (
              SELECT "year",
                mnth_id,
                lag(mnth_id, 2) OVER (
                  ORDER BY mnth_id
                  ) third_month
              FROM (
                SELECT DISTINCT "year",
                  mnth_id
                FROM edw_vw_os_time_dim -- limit 100
                WHERE "year" >= (DATE_PART("year", convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)) - 6)
                )
              ) month_base
            ) to_month
        WHERE month <= mnth_id
          AND month >= third_month
        GROUP BY base3.sap_parent_customer_key,
          base3.matl_num,
          mnth_id
        ) last_3_months,
      (
        SELECT base6.sap_parent_customer_key,
          base6.matl_num,
          mnth_id,
          sum(so_qty) AS last_6months_so_matl,
          sum(so_value) AS last_6months_so_value_matl
        FROM (
          SELECT *
          FROM WKS_HK_allmonths_base
          WHERE left(month, 4) >= (DATE_PART("year", convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)) - 6)
          ) base6,
          (
            SELECT mnth_id,
              sixth_month
            FROM (
              SELECT "year",
                mnth_id,
                lag(mnth_id, 5) OVER (
                  ORDER BY mnth_id
                  ) sixth_month
              FROM (
                SELECT DISTINCT "year" AS "year",
                  mnth_id AS mnth_id
                FROM edw_vw_os_time_dim -- limit 100
                WHERE "year" >= (DATE_PART("year", convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)) - 6)
                )
              ) month_base
            ) to_month
        WHERE month <= mnth_id
          AND month >= sixth_month
        GROUP BY base6.sap_parent_customer_key,
          base6.matl_num,
          mnth_id
        ) last_6_months,
      (
        SELECT base12.sap_parent_customer_key,
          base12.matl_num,
          mnth_id,
          sum(so_qty) AS last_12months_so_matl,
          sum(so_value) AS last_12months_so_value_matl
        FROM (
          SELECT *
          FROM WKS_HK_allmonths_base
          WHERE left(month, 4) >= (DATE_PART(year, convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)) - 6)
          ) base12,
          (
            SELECT mnth_id,
              twelfth_month
            FROM (
              SELECT year,
                mnth_id,
                lag(mnth_id, 11) OVER (
                  ORDER BY mnth_id
                  ) twelfth_month
              FROM (
                SELECT DISTINCT "year" AS year,
                  mnth_id AS mnth_id
                FROM edw_vw_os_time_dim -- limit 100
                WHERE year >= (DATE_PART(YEAR, convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)) - 6)
                )
              ) month_base
            ) to_month
        WHERE month <= mnth_id
          AND month >= twelfth_month
        GROUP BY base12.sap_parent_customer_key,
          base12.matl_num,
          mnth_id
        ) last_12_months,
      (
        SELECT base36.sap_parent_customer_key,
          base36.matl_num,
          mnth_id,
          sum(so_qty) AS last_36months_so_matl,
          sum(so_value) AS last_36months_so_value_matl
        FROM (
          SELECT *
          FROM WKS_HK_allmonths_base
          WHERE left(month, 4) >= (DATE_PART(YEAR, convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)) - 6)
          ) base36,
          (
            SELECT mnth_id,
              thirtysixth_month
            FROM (
              SELECT year,
                mnth_id,
                lag(mnth_id, 35) OVER (
                  ORDER BY mnth_id
                  ) thirtysixth_month
              FROM (
                SELECT DISTINCT "year" AS year,
                  mnth_id AS mnth_id
                FROM edw_vw_os_time_dim -- limit 100
                WHERE year >= (DATE_PART(YEAR, convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)) - 6)
                )
              ) month_base
            ) to_month
        WHERE month <= mnth_id
          AND month >= thirtysixth_month
        GROUP BY base36.sap_parent_customer_key,
          base36.matl_num,
          mnth_id
        ) last_36_months
    WHERE base.sap_parent_customer_key = last_3_months.sap_parent_customer_key(+)
      AND base.matl_num = last_3_months.matl_num(+)
      AND base.month = last_3_months.mnth_id(+)
      AND base.sap_parent_customer_key = last_6_months.sap_parent_customer_key(+)
      AND base.matl_num = last_6_months.matl_num(+)
      AND base.month = last_6_months.mnth_id(+)
      AND base.sap_parent_customer_key = last_12_months.sap_parent_customer_key(+)
      AND base.matl_num = last_12_months.matl_num(+)
      AND base.month = last_12_months.mnth_id(+)
      AND base.sap_parent_customer_key = last_36_months.sap_parent_customer_key(+)
      AND base.matl_num = last_36_months.matl_num(+)
      AND base.month = last_36_months.mnth_id(+)
    )
  GROUP BY sap_parent_customer_key,
    matl_num,
    month
  ),
final as (
select
sap_parent_customer_key::varchar(50) as sap_parent_customer_key,
matl_num::varchar(255) as matl_num,
month::varchar(23) as month,
so_qty::number(38,0) as so_qty,
so_value::number(38,4) as so_value,
inv_qty::number(38,5) as inv_qty,
inv_value::number(38,9) as inv_value,
sell_in_qty::number(38,4) as sell_in_qty,
sell_in_value::number(38,4) as sell_in_value,
last_3months_so::number(38,0) as last_3months_so,
last_3months_so_value::number(38,4) as last_3months_so_value,
last_6months_so::number(38,0) as last_6months_so,
last_6months_so_value::number(38,4) as last_6months_so_value,
last_12months_so::number(38,0) as last_12months_so,
last_12months_so_value::number(38,4) as last_12months_so_value,
last_36months_so::number(38,0) as last_36months_so,
last_36months_so_value::number(38,4) as last_36months_so_value
from transformed
)
SELECT *
FROM final
