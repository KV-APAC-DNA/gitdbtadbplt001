{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
    )
}}
with source as(
    select * from {{ ref('aspitg_integration__itg_copa17_trans') }}
),
edw_acct_hier as(
select * from DEV_DNA_CORE.SM05_WORKSPACE.EDW_ACCT_HIER
)
SELECT
    fisc_yr_per,
    fisc_yr_vrnt,
    fisc_yr,
    cal_day,
    pstng_per,
    cal_yr_mo,
    cal_yr,
    vers,
    val_type,
    co_cd,
    cntl_area,
    prft_ctr,
    sls_emp_hist,
    sls_org,
    sls_grp,
    sls_off,
    cust_grp,
    dstn_chnl,
    sls_dstrc,
    cust,
    matl,
    cust_sls_view,
    DIV,
    plnt,
    mercia_ref,
    b3_base_prod,
    b4_vrnt,
    b5_put_up,
    b1_mega_brnd,
    b2_brnd,
    rgn,
    ctry,
    prod_minor,
    prod_maj,
    prod_fran,
    fran,
    fran_grp,
    oper_grp,
    fisc_qtr,
    matl2,
    bill_type,
    fisc_wk,
    SUM(amt_grp_crcy * multiplication_factor) AS amt_grp_crcy,
    SUM(amt_obj_crcy * multiplication_factor) AS amt_obj_crcy,
    crncy,
    obj_crncy,
    itg_copa17_trans.acct_num,
    chrt_of_acct,
    mgmt_entity,
    sls_prsn_respons,
    busn_area,
    SUM(ga * multiplication_factor) AS ga,
    tc,
    matl_plnt_view,
    SUM(qty * multiplication_factor) AS qty,
    uom,
    SUM(sls_vol_ieu * multiplication_factor) AS sls_vol_ieu,
    un_sls_vol__ieu,
    bpt_dstn_chnl,
    measure_name,
    measure_code,
    CASE
      WHEN vers = '000'
      THEN 'BP'
      WHEN CAST(vers as int) IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
      THEN 'RF'
      WHEN CAST(vers as int) IN (101, 102, 104, 105, 107, 109, 111, 112)
      THEN 'LE'
      WHEN CAST(vers as int) IN (103)
      THEN 'MU'
      WHEN CAST(vers as int) IN (106)
      THEN 'JU'
      WHEN CAST(vers as int) IN (108)
      THEN 'SU'
      WHEN CAST(vers as int) IN (110)
      THEN 'NU'
      WHEN CAST(vers as int) IN (521)
      THEN 'PBP1'
      WHEN CAST(vers as int) IN (522)
      THEN 'PBP2'
      WHEN CAST(vers as int) IN (523)
      THEN 'PBP3'
      WHEN CAST(vers as int) IN (550)
      THEN 'SMT'
      WHEN CAST(vers as int) IN (600)
      THEN 'PPI'
      WHEN CAST(vers as int) IN (800)
      THEN 'PACV'
      WHEN CAST(vers as int) IN (801)
      THEN 'FR'
      WHEN CAST(vers as int) IN (900)
      THEN 'WIAS00'
      WHEN CAST(vers as int) IN (901)
      THEN 'WIS1'
      WHEN CAST(vers as int) IN (902)
      THEN 'WIS2'
      WHEN CAST(vers as int) IN (903)
      THEN 'WIS3'
      WHEN CAST(vers as int) IN (999)
      THEN 'MIV'
      WHEN vers IN ('PMS')
      THEN 'M&SM'
      ELSE 'NOT DEFINED'
    END AS catagory,
    CASE
      WHEN CAST(vers as int) IN (1, 101)
      THEN 'Jan'
      WHEN CAST(vers as int) IN (2, 102)
      THEN 'Feb'
      WHEN CAST(vers as int) IN (3)
      THEN 'Mar'
      WHEN CAST(vers as int) IN (4, 104)
      THEN 'Apr'
      WHEN CAST(vers as int) IN (5, 105)
      THEN 'May'
      WHEN CAST(vers as int) IN (6)
      THEN 'Jun'
      WHEN CAST(vers as int) IN (7, 107)
      THEN 'Jul'
      WHEN CAST(vers as int) IN (8)
      THEN 'Aug'
      WHEN CAST(vers as int) IN (9, 109)
      THEN 'Sep'
      WHEN CAST(vers as int) IN (10)
      THEN 'Oct'
      WHEN CAST(vers as int) IN (11, 111)
      THEN 'Nov'
      WHEN CAST(vers as int) IN (12, 112)
      THEN 'Dec'
      WHEN CAST(vers as int) IN (103)
      THEN 'QTR1'
      WHEN CAST(vers as int) IN (106)
      THEN 'QTR2'
      WHEN CAST(vers as int) IN (108)
      THEN 'QTR3'
      WHEN CAST(vers as int) IN (110)
      THEN 'QTR4'
      ELSE 'YTD'
    END AS Freq
  FROM source as itg_copa17_trans
  INNER JOIN edw_acct_hier AS B
    ON LTRIM(RTRIM(itg_copa17_trans.acct_num)) = LTRIM(RTRIM(B.acct_num))
  GROUP BY
    fisc_yr_per,
    fisc_yr_vrnt,
    fisc_yr,
    cal_day,
    pstng_per,
    cal_yr_mo,
    cal_yr,
    vers,
    val_type,
    co_cd,
    cntl_area,
    prft_ctr,
    sls_emp_hist,
    sls_org,
    sls_grp,
    sls_off,
    cust_grp,
    dstn_chnl,
    sls_dstrc,
    cust,
    matl,
    cust_sls_view,
    DIV,
    plnt,
    mercia_ref,
    b3_base_prod,
    b4_vrnt,
    b5_put_up,
    b1_mega_brnd,
    b2_brnd,
    rgn,
    ctry,
    prod_minor,
    prod_maj,
    prod_fran,
    fran,
    fran_grp,
    oper_grp,
    fisc_qtr,
    matl2,
    bill_type,
    fisc_wk,
    crncy,
    obj_crncy,
    itg_copa17_trans.acct_num,
    chrt_of_acct,
    mgmt_entity,
    sls_prsn_respons,
    busn_area,
    tc,
    matl_plnt_view,
    uom,
    un_sls_vol__ieu,
    bpt_dstn_chnl,
    measure_name,
    measure_code