WITH affiliate_cancel_wk3
AS (
  SELECT *
  FROM dev_dna_core.snapjpdcledw_integration.affiliate_cancel_wk3
  ),
affiliate_cancel_sameday_order
AS (
  SELECT *
  FROM dev_dna_core.snapjpdcledw_integration.affiliate_cancel_sameday_order
  ),
affiliate_cancel_receive
AS (
  SELECT *
  FROM dev_dna_core.snapjpdclitg_integration.affiliate_cancel_receive
  ),
transformed
AS (
  SELECT ind.achievement,
    ind.click_dt,
    ind.accrual_dt,
    ind.asp,
    ind.unique_id,
    ind.media_name,
    ind.asp_control_no,
    ind.sale_num,
    ind.amount_including_tax,
    ind.amount_excluded_tax,
    ind.orderdate,
    ind.webid AS ind_webid,
    da.rcv_orderid,
    da.rcv_orderdt,
    da.rcv_price,
    da.cnxt_price_tax_excl,
    da.cnxt_ditotalprc,
    da.cnxt_diordertax,
    da.cnxt_c_didiscountprc,
    da.cnxt_c_didiscountall,
    da.cnxt_diusepoint,
    da.cnxt_dihaisoprc,
    da.cnxt_c_dicollectprc,
    da.cnxt_c_ditoujitsuhaisoprc,
    da.cnxt_diseikyuprc,
    da.cnxt_diorderid,
    da.diusrid,
    da.webid AS da_webid,
    da.judge_price,
    da.price_sabun,
    da.cmpr_sts,
    CASE 
      WHEN da.cmpr_sts = 'データなし'
        AND so.webid IS NOT NULL
        THEN '*'
      ELSE NULL
      END AS samday_odr_flg
  FROM affiliate_cancel_receive ind
  LEFT JOIN affiliate_cancel_wk3 da ON ind.unique_id = da.rcv_orderid
  LEFT JOIN (
    SELECT DISTINCT webid
    FROM affiliate_cancel_sameday_order
    ) so ON ind.webid = so.webid
  WHERE ind.STATUS IS NULL
  ORDER BY ind.unique_id,
    ind.accrual_dt
  ),
final
AS (
  SELECT achievement::VARCHAR(10) AS achievement,
    click_dt::VARCHAR(19) AS click_dt,
    accrual_dt::VARCHAR(19) AS accrual_dt,
    asp::VARCHAR(10) AS asp,
    unique_id::VARCHAR(12) AS unique_id,
    media_name::VARCHAR(100) AS media_name,
    asp_control_no::VARCHAR(100) AS asp_control_no,
    sale_num::number(18, 0) AS sale_num,
    amount_including_tax::number(18, 0) AS amount_including_tax,
    amount_excluded_tax::number(18, 0) AS amount_excluded_tax,
    orderdate::VARCHAR(19) AS orderdate,
    ind_webid::VARCHAR(8) AS webid,
    rcv_orderid::VARCHAR(12) AS rcv_orderid,
    rcv_orderdt::VARCHAR(19) AS rcv_orderdt,
    rcv_price::number(18, 0) AS rcv_price,
    cnxt_price_tax_excl::number(18, 0) AS cnxt_price_tax_excl,
    cnxt_ditotalprc::number(10, 0) AS cnxt_ditotalprc,
    cnxt_diordertax::number(10, 0) AS cnxt_diordertax,
    cnxt_c_didiscountprc::number(10, 0) AS cnxt_c_didiscountprc,
    cnxt_c_didiscountall::number(10, 0) AS cnxt_c_didiscountall,
    cnxt_diusepoint::number(10, 0) AS cnxt_diusepoint,
    cnxt_dihaisoprc::number(10, 0) AS cnxt_dihaisoprc,
    cnxt_c_dicollectprc::number(10, 0) AS cnxt_c_dicollectprc,
    cnxt_c_ditoujitsuhaisoprc::number(10, 0) AS cnxt_c_ditoujitsuhaisoprc,
    cnxt_diseikyuprc::number(10, 0) AS cnxt_diseikyuprc,
    cnxt_diorderid::number(10, 0) AS cnxt_diorderid,
    diusrid::VARCHAR(40) AS diusrid,
    da_webid::VARCHAR(32) AS webid_1,
    judge_price::number(18, 0) AS judge_price,
    price_sabun::number(18, 0) AS price_sabun,
    cmpr_sts::VARCHAR(15) AS cmpr_sts,
    samday_odr_flg::VARCHAR(1) AS samday_odr_flg,
    current_timestamp()::timestamp_ntz(9) AS inserted_date,
    current_timestamp()::timestamp_ntz(9) AS updated_date
  FROM transformed
  )
SELECT *
FROM final