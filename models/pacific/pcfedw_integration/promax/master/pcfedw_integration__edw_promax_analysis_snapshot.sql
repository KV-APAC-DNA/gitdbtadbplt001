{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key=['snapshot_date'],
        pre_hook="delete from {{this}} where  snapshot_date::date <  SUBSTRING(add_months(current_timestamp(),cast((-1)*(select parameter_value from {{ source('pcfitg_integration', 'itg_query_parameters') }} where parameter_name='Pacific_Promax_master_snapshotdate_months')as integer)),1,19)::date"
    )
}}


with itg_query_parameters as(
    select * from {{ source('pcfitg_integration', 'itg_query_parameters') }}
),
px_combined_ciw_fact as(
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.PX_COMBINED_CIW_FACT
),
edw_px_gl_trans_lkp as(
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PX_GL_TRANS_LKP
),
vw_customer_dim as(
    select * from DEV_DNA_CORE.PCFEDW_INTEGRATION.VW_CUSTOMER_DIM
),
vw_material_dim as(
    select * from DEV_DNA_CORE.PCFEDW_INTEGRATION.VW_MATERIAL_DIM
),
edw_time_dim as(
    select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
edw_px_master_fact as(
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PX_MASTER_FACT
),
cte1 as(
SELECT
    etd.cal_date,
    etd.time_id,
    etd.jj_wk,
    etd.jj_mnth,
    etd.jj_mnth_shrt,
    etd.jj_mnth_long,
    etd.jj_qrtr,
    etd.jj_year,
    etd.cal_mnth_id,
    etd.jj_mnth_id,
    etd.cal_mnth,
    etd.cal_qrtr,
    etd.cal_year,
    etd.jj_mnth_tot,
    etd.jj_mnth_day,
    etd.cal_mnth_nm,
    vcd.cust_no,
    vcd.cmp_id,
    vcd.channel_cd,
    vcd.channel_desc,
    vcd.ctry_key,
    vcd.country,
    vcd.state_cd,
    vcd.post_cd,
    vcd.cust_suburb,
    vcd.cust_nm,
    vcd.sls_org,
    vcd.cust_del_flag,
    vcd.sales_office_cd,
    vcd.sales_office_desc,
    vcd.sales_grp_cd,
    vcd.sales_grp_desc,
    vcd.mercia_ref,
    vcd.curr_cd,
    vmd.matl_id,
    vmd.matl_desc,
    vmd.mega_brnd_cd,
    vmd.mega_brnd_desc,
    vmd.brnd_cd,
    vmd.brnd_desc,
    vmd.base_prod_cd,
    vmd.base_prod_desc,
    vmd.variant_cd,
    vmd.variant_desc,
    vmd.fran_cd,
    vmd.fran_desc,
    vmd.grp_fran_cd,
    vmd.grp_fran_desc,
    vmd.matl_type_cd,
    vmd.matl_type_desc,
    vmd.prod_fran_cd,
    vmd.prod_fran_desc,
    vmd.prod_hier_cd,
    vmd.prod_hier_desc,
    vmd.prod_mjr_cd,
    vmd.prod_mjr_desc,
    vmd.prod_mnr_cd,
    vmd.prod_mnr_desc,
    vmd.mercia_plan,
    vmd.putup_cd,
    vmd.putup_desc,
    vmd.bar_cd,
    vmd.updt_dt,
    epmf.ac_code,
    epmf.ac_longname,
    epmf.p_promonumber,
    epmf.p_startdate,
    epmf.p_stopdate,
    epmf.promo_length,
    epmf.promotionforecastweek,
    epmf.p_buystartdatedef,
    epmf.p_buystopdatedef,
    epmf.buyperiod_length,
    epmf.hierarchy_rowid,
    epmf.hierarchy_longname,
    epmf.activity_longname,
    CASE
      WHEN epmf.confirmed_switch = 1
      THEN CAST('Confirmed' AS VARCHAR)
      WHEN epmf.confirmed_switch = 0
      THEN CAST('Unconfirmed' AS VARCHAR)
      ELSE CAST('Pending' AS VARCHAR)
    END AS confirmed_switch,
    CASE
      WHEN epmf.closed_switch = 1
      THEN CAST('Closed' AS VARCHAR)
      WHEN epmf.closed_switch = 0
      THEN CAST('Open' AS VARCHAR)
      ELSE CAST(NULL AS VARCHAR)
    END AS closed_switch,
    epmf.sku_longname,
    epmf.sku_profitcentre,
    epmf.sku_attribute,
    epmf.gltt_rowid,
    epmf.transaction_longname,
    epmf.case_deal,
    epmf.case_quantity,
    epmf.planspend_total,
    epmf.paid_total,
    CASE
      WHEN epmf.closed_switch = 1
      THEN epmf.paid_total
      ELSE CASE
        WHEN epmf.planspend_total > epmf.paid_total
        THEN epmf.planspend_total
        ELSE epmf.paid_total
      END
    END - epmf.paid_total AS open_total,
    CASE
      WHEN epmf.closed_switch = 1
      THEN epmf.paid_total
      ELSE CASE
        WHEN epmf.planspend_total > epmf.paid_total
        THEN epmf.planspend_total
        ELSE epmf.paid_total
      END
    END AS committed_spend,
    CASE
      WHEN epmf.p_deleted = 1
      THEN CAST('Yes' AS VARCHAR)
      WHEN epmf.p_deleted = 0
      THEN CAST('No' AS VARCHAR)
      ELSE CAST(NULL AS VARCHAR)
    END AS p_deleted,
    epmf.local_ccy,
    epmf.aud_rate,
    epmf.sgd_rate,
    epgm.sap_account,
    cpf.sap_accnt_nm,
    epgm.promax_measure,
    epgm.promax_bucket,
    epmf.promotionrowid
  FROM (
    SELECT DISTINCT
      px_combined_ciw_fact.sap_accnt,
      px_combined_ciw_fact.sap_accnt_nm
    FROM px_combined_ciw_fact
  ) AS cpf, edw_px_gl_trans_lkp AS epgm, edw_px_master_fact AS epmf
  LEFT JOIN vw_customer_dim AS vcd
    ON CAST(epmf.cust_id AS TEXT) = LTRIM(CAST(vcd.cust_no AS TEXT), CAST(CAST('0' AS VARCHAR) AS TEXT))
  LEFT JOIN vw_material_dim AS vmd
    ON CAST(epmf.matl_id AS TEXT) = LTRIM(CAST(vmd.matl_id AS TEXT), CAST(CAST('0' AS VARCHAR) AS TEXT))
  LEFT JOIN edw_time_dim AS etd
    ON ltrim(epmf.promotionforecastweek)::date = ltrim(etd.cal_date)::date
  WHERE
    epmf.gltt_rowid = epgm.row_id
    AND CAST(epgm.sap_account AS TEXT) = CAST(cpf.sap_accnt AS TEXT)
),
transformed as(
SELECT
  CONVERT_TIMEZONE('AEDT', CURRENT_TIMESTAMP()) AS SNAPSHOT_DATE,
  TO_CHAR(CAST(CURRENT_TIMESTAMP() AS TIMESTAMPNTZ), 'MON') AS SNAPSHOT_MONTH,
  TO_CHAR(CAST(CURRENT_TIMESTAMP() AS TIMESTAMPNTZ), 'yyyy') AS SNAPSHOT_YEAR,
  ac_code,
  ac_longname,
  activity_longname,
  channel_desc,
  closed_switch,
  confirmed_switch,
  country,
  curr_cd,
  cust_no,
  cust_del_flag,
  cust_nm,
  fran_desc,
  gltt_rowid,
  grp_fran_desc,
  jj_mnth_day,
  jj_mnth_shrt,
  jj_mnth_long,
  jj_mnth,
  jj_mnth_id,
  jj_qrtr,
  jj_wk,
  jj_year,
  local_ccy,
  matl_desc,
  matl_id,
  mega_brnd_desc,
  prod_fran_desc,
  prod_mjr_desc,
  prod_mnr_desc,
  promax_bucket,
  p_buystartdatedef,
  p_buystopdatedef,
  p_deleted,
  promotionforecastweek,
  p_promonumber,
  promotionrowid,
  p_startdate,
  p_stopdate,
  sales_grp_desc,
  sales_office_desc,
  sap_accnt_nm,
  sap_account,
  transaction_longname,
  updt_dt,
  buyperiod_length,
  case_deal,
  case_quantity,
  committed_spend,
  aud_rate,
  open_total,
  paid_total,
  planspend_total,
  promo_length
FROM cte1
where (jj_mnth_id)> cast((SUBSTRING(add_months(current_timestamp()::timestamp_ntz(9),cast((-1)*(select parameter_value from itg_query_parameters where parameter_name='Pacific_Promax_master_snapshot_data_past_months')as integer)),1,4))

||(SUBSTRING(add_months(current_timestamp()::timestamp_ntz(9),cast((-1)*(select parameter_value from itg_query_parameters where parameter_name='Pacific_Promax_master_snapshot_data_past_months')as integer)),6,2))as numeric)

and
(jj_mnth_id)< cast((SUBSTRING(add_months(current_timestamp()::timestamp_ntz(9),cast((select parameter_value from itg_query_parameters where parameter_name='Pacific_Promax_master_snapshot_data_forecast_months')as integer)),1,4))

||(SUBSTRING(add_months(current_timestamp()::timestamp_ntz(9),cast((select parameter_value from itg_query_parameters where parameter_name='Pacific_Promax_master_snapshot_data_forecast_months')as integer)),6,2)) as numeric)
)
select * from transformed