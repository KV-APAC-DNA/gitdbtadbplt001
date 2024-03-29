with wrk_vn_target_p1 as(
    select * from {{ ref('vnmwks_integration__wrk_vn_target_p1') }} 
),
wrk_vn_target_p2 as(
    select * from {{ ref('vnmwks_integration__wrk_vn_target_p2') }}
),
inn as(
	SELECT
        p1.dstrbtr_id,
        p1.saleman_code,
        p1.target_cyc,
        p1.target_wk,
        p1.target_value AS tgt_by_month,
        p1.sales_mnth AS prev_sales_mnth1,
        p1.sales_wk AS prev_sales_mnth_wk1,
        p1.amt_by_wk AS prev_sales_amt_wk1,
        p2.sales_mnth AS prev_sales_mnth2,
        p2.sales_wk AS prev_sales_mnth_wk2,
        p2.amt_by_wk AS prev_sales_amt_wk2,
        COALESCE(p1.amt_by_wk, 0) + COALESCE(p2.amt_by_wk, 0) AS sum_by_wk,
        SUM(COALESCE(p1.amt_by_wk, 0) + COALESCE(p2.amt_by_wk, 0)) OVER (PARTITION BY p1.dstrbtr_id, p1.saleman_code, p1.target_cyc) AS sum_for_both_months
  FROM wrk_vn_target_p1 AS p1, wrk_vn_target_p2 AS p2
  WHERE
    p1.dstrbtr_id = p2.dstrbtr_id
    AND p1.saleman_code = p2.saleman_code
    AND p1.target_cyc = p2.target_cyc
    AND p1.target_wk = p2.target_wk
  ORDER BY
    p1.dstrbtr_id,
    p1.saleman_code,
    p1.target_cyc,
    p1.target_wk
),
transformed as(
SELECT
  inn.*,
  CASE
    WHEN (
      COALESCE(sum_for_both_months, 0) > 0 AND COALESCE(sum_by_wk, 0) > 0
    )
    THEN (
      (
        ROUND((
          ROUND(sum_by_wk, 7) / ROUND(sum_for_both_months, 7)
        ), 7)
      ) * tgt_by_month
    )
    ELSE CASE
      WHEN (
        COALESCE(tgt_by_month, 0) > 0
        AND COALESCE(
          SUM(sum_for_both_months) OVER (PARTITION BY dstrbtr_id, saleman_code, target_cyc),
          0
        ) = 0
      )
      THEN ROUND(
        tgt_by_month / MAX(target_wk) OVER (PARTITION BY dstrbtr_id, saleman_code, target_cyc),
        2
      )
    END
  END AS tgt_by_week
FROM  inn
),
final as(
    select 
        dstrbtr_id::varchar(30) as dstrbtr_id,
        saleman_code::varchar(30) as saleman_code,
        target_cyc::number(18,0) as target_cyc,
        target_wk::number(18,0) as target_wk,
        tgt_by_month::number(15,4) as tgt_by_month,
        prev_sales_mnth1::varchar(23) as prev_sales_mnth1,
        prev_sales_mnth_wk1::number(18,0) as prev_sales_mnth_wk1,
        prev_sales_amt_wk1::number(20,4) as prev_sales_amt_wk1,
        prev_sales_mnth2::varchar(23) as prev_sales_mnth2,
        prev_sales_mnth_wk2::number(18,0) as prev_sales_mnth_wk2,
        prev_sales_amt_wk2::number(20,4) as prev_sales_amt_wk2,
        sum_by_wk::number(20,4) as sum_by_wk,
        sum_for_both_months::number(20,4) as sum_for_both_months,
        tgt_by_week::number(20,4) as tgt_by_week
    from transformed
)
select * from final