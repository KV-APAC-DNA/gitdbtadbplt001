WITH edi_store_m
AS (
  SELECT *
  FROM {{ ref('jpnedw_integration__edi_store_m') }}
  ),
mt_prf
AS (
  SELECT *
  FROM {{ ref('jpnedw_integration__mt_prf') }}
  ),
edi_chn_m
AS (
  SELECT *
  FROM {{ ref('jpnedw_integration__edi_chn_m') }}
  ),
mt_sgmt
AS (
  SELECT *
  FROM {{ ref('jpnedw_integration__mt_sgmt') }}
  ),
m_area_master
AS (
  SELECT *
  FROM {{ ref('jpnedw_integration__m_area_master') }}
  ),
m_ldw_store_local
AS (
  SELECT *
  FROM {{ ref('jpnedw_integration__m_ldw_store_local') }}
  ),
s
AS (
  SELECT al1.reg_dt AS reg_dt,
    nvl(nullif(al1.str_cd, ''), CONCAT (
        'D',
        al2.chn_cd
        )) AS str_cd,
    al1.lgl_nm_knj1 AS lgl_nm_knj,
    al1.lgl_nm_knj2 AS lgl_nm_knj2,
    al1.lgl_nm_kn AS lgl_nm_kn,
    al1.cmmn_nm_knj AS cmmn_nm_lnj,
    al1.cmmn_nm_kn AS cmmn_nm_kn,
    al1.adrs_knj1 AS adrs_knj1,
    al1.adrs_knj2 AS adrs_knj2,
    al1.adrs_kn AS adrs_kn,
    al1.pst_co AS pst_no,
    al1.tel_no AS tel_no,
    al1.jis_prfct_c AS jis_prefct_cd,
    al3.prf_nm_knj AS prefct_nm,
    al1.jis_city_cd AS jis_city_cd,
    al1.trd_cd AS trd_cd,
    al1.trd_offc_cd AS trd_offc_cd,
    al2.chn_cd AS chn_cd,
    al2.lgl_nm AS chn_lgl_nm,
    al2.cmmn_nm AS chn_cmmn_nm,
    al2.rank AS chn_rank,
    al2.sgmt AS chn_seg_cd,
    al4.sgmt_nm AS chn_seg_nm,
    al2.acnt_prsn_cd AS chn_ant_psn_cd,
    al2.sales_group AS sales_group,
    '' AS sales_group_nm,
    al2.scnd_acnt_prsn AS chn_scnd_ant_psn_cd,
    '' AS chn_scnd_sal_grp,
    '' AS chn_scnd_sal_grp_nm,
    al2.chn_offc_cd AS chn_offc_cd,
    al7.frnc AS frnc,
    al6.lgl_nm AS frnc_nm,
    al7.lgl_nm AS chn_offc_lgl_nm,
    al7.cmmn_nm AS chn_offc_cmmn_nm,
    al7.rank AS chn_offc_rank,
    al7.sgmt AS chn_offc_seg_cd,
    al8.sgmt_nm AS chn_offc_seg_nm,
    al7.acnt_prsn_cd AS chn_offc_ant_psn_cd,
    al7.sales_group AS chn_offc_sal_grp,
    '' AS chn_offc_sal_grp_nm,
    al7.scnd_acnt_prsn AS chn_offc_scnd_ant_psn_cd,
    '' AS chn_offc_scnd_sal_grp,
    '' AS chn_offc_scnd_sal_grp_nm,
    al1.chn_cd_oth AS chn_cd_oth,
    al1.emp_cd_kk AS emp_cd_kk,
    al1.all_str_ass AS all_str_ass,
    al1.agrm_str AS agrm_str,
    al1.pj_ass AS pjass,
    al1.emp_cd_roc AS emp_cd_roc
  FROM edi_store_m al1,
    edi_chn_m al2,
    mt_prf al3,
    mt_sgmt al4,
    edi_chn_m al6,
    edi_chn_m al7,
    mt_sgmt al8
  WHERE (
      al1.jis_prfct_c = al3.prf_cd(+)
      AND al1.chn_cd(+) = al2.chn_cd
      AND al2.sgmt = al4.sgmt(+)
      AND al2.chn_offc_cd = al7.chn_cd(+)
      AND al7.frnc = al6.chn_cd(+)
      AND al7.sgmt = al8.sgmt(+)
      )
  ),
ct1
AS (
  SELECT s.reg_dt,
    s.str_cd,
    s.lgl_nm_knj,
    s.lgl_nm_knj2,
    s.lgl_nm_kn,
    s.cmmn_nm_lnj,
    s.cmmn_nm_kn,
    s.adrs_knj1,
    s.adrs_knj2,
    s.adrs_kn,
    s.pst_no,
    s.tel_no,
    s.jis_prefct_cd,
    s.prefct_nm,
    s.jis_city_cd,
    s.trd_cd,
    s.trd_offc_cd,
    s.chn_cd,
    s.chn_lgl_nm,
    s.chn_cmmn_nm,
    s.chn_rank,
    s.chn_seg_cd,
    s.chn_seg_nm,
    s.chn_ant_psn_cd,
    s.sales_group,
    s.sales_group_nm,
    s.chn_scnd_ant_psn_cd,
    s.chn_scnd_sal_grp,
    s.chn_scnd_sal_grp_nm,
    s.chn_offc_cd,
    s.frnc,
    s.frnc_nm,
    s.chn_offc_lgl_nm,
    s.chn_offc_cmmn_nm,
    s.chn_offc_rank,
    s.chn_offc_seg_cd,
    s.chn_offc_seg_nm,
    s.chn_offc_ant_psn_cd,
    s.chn_offc_sal_grp,
    s.chn_offc_sal_grp_nm,
    s.chn_offc_scnd_ant_psn_cd,
    s.chn_offc_scnd_sal_grp,
    s.chn_offc_scnd_sal_grp_nm,
    s.chn_cd_oth,
    s.emp_cd_kk,
    s.all_str_ass,
    s.agrm_str,
    s.pjass,
    s.emp_cd_roc,
    a.area_nm
  FROM s,
    m_area_master a
  WHERE s.jis_prefct_cd = a.jis_prefct_c
  ),
ct2
AS (
  SELECT reg_dt,
    str_cd,
    lgl_nm_knj,
    lgl_nm_knj2,
    lgl_nm_kn,
    cmmn_nm_lnj,
    cmmn_nm_kn,
    adrs_knj1,
    adrs_knj2,
    adrs_kn,
    pst_no,
    tel_no,
    jis_prefct_cd,
    prefct_nm,
    jis_city_cd,
    trd_cd,
    trd_offc_cd,
    chn_cd,
    chn_lgl_nm,
    chn_cmmn_nm,
    chn_rank,
    chn_seg_cd,
    chn_seg_nm,
    chn_ant_psn_cd,
    sales_group,
    sales_group_nm,
    chn_scnd_ant_psn_cd,
    chn_scnd_sal_grp,
    chn_scnd_sal_grp_nm,
    chn_offc_cd,
    frnc,
    frnc_nm,
    chn_offc_lgl_nm,
    chn_offc_cmmn_nm,
    chn_offc_rank,
    chn_offc_seg_cd,
    chn_offc_seg_nm,
    chn_offc_ant_psn_cd,
    chn_offc_sal_grp,
    chn_offc_sal_grp_nm,
    chn_offc_scnd_ant_psn_cd,
    chn_offc_scnd_sal_grp,
    chn_offc_scnd_sal_grp_nm,
    chn_cd_oth,
    emp_cd_kk,
    all_str_ass,
    agrm_str,
    pjass,
    emp_cd_roc,
    area
  FROM m_ldw_store_local
  ),
trns
AS (
  SELECT *
  FROM ct1
  
  UNION ALL
  
  SELECT *
  FROM ct2
  ),
final
AS (
  SELECT reg_dt::timestamp_ntz(9) AS reg_dt,
    str_cd::VARCHAR(256) AS str_cd,
    lgl_nm_knj::VARCHAR(256) AS lgl_nm_knj,
    lgl_nm_knj2::VARCHAR(256) AS lgl_nm_knj2,
    lgl_nm_kn::VARCHAR(256) AS lgl_nm_kn,
    cmmn_nm_lnj::VARCHAR(256) AS cmmn_nm_lnj,
    cmmn_nm_kn::VARCHAR(256) AS cmmn_nm_kn,
    adrs_knj1::VARCHAR(256) AS adrs_knj1,
    adrs_knj2::VARCHAR(256) AS adrs_knj2,
    adrs_kn::VARCHAR(256) AS adrs_kn,
    pst_no::VARCHAR(256) AS pst_no,
    tel_no::VARCHAR(256) AS tel_no,
    jis_prefct_cd::VARCHAR(256) AS jis_prefct_cd,
    prefct_nm::VARCHAR(256) AS prefct_nm,
    jis_city_cd::VARCHAR(256) AS jis_city_cd,
    trd_cd::VARCHAR(256) AS trd_cd,
    trd_offc_cd::VARCHAR(256) AS trd_offc_cd,
    chn_cd::VARCHAR(256) AS chn_cd,
    chn_lgl_nm::VARCHAR(256) AS chn_lgl_nm,
    chn_cmmn_nm::VARCHAR(256) AS chn_cmmn_nm,
    chn_rank::VARCHAR(256) AS chn_rank,
    chn_seg_cd::VARCHAR(256) AS chn_seg_cd,
    chn_seg_nm::VARCHAR(256) AS chn_seg_nm,
    chn_ant_psn_cd::VARCHAR(256) AS chn_ant_psn_cd,
    sales_group::VARCHAR(256) AS sales_group,
    sales_group_nm::VARCHAR(256) AS sales_group_nm,
    chn_scnd_ant_psn_cd::VARCHAR(256) AS chn_scnd_ant_psn_cd,
    chn_scnd_sal_grp::VARCHAR(256) AS chn_scnd_sal_grp,
    chn_scnd_sal_grp_nm::VARCHAR(256) AS chn_scnd_sal_grp_nm,
    chn_offc_cd::VARCHAR(256) AS chn_offc_cd,
    frnc::VARCHAR(256) AS frnc,
    frnc_nm::VARCHAR(256) AS frnc_nm,
    chn_offc_lgl_nm::VARCHAR(256) AS chn_offc_lgl_nm,
    chn_offc_cmmn_nm::VARCHAR(256) AS chn_offc_cmmn_nm,
    chn_offc_rank::VARCHAR(256) AS chn_offc_rank,
    chn_offc_seg_cd::VARCHAR(256) AS chn_offc_seg_cd,
    chn_offc_seg_nm::VARCHAR(256) AS chn_offc_seg_nm,
    chn_offc_ant_psn_cd::VARCHAR(256) AS chn_offc_ant_psn_cd,
    chn_offc_sal_grp::VARCHAR(256) AS chn_offc_sal_grp,
    chn_offc_sal_grp_nm::VARCHAR(256) AS chn_offc_sal_grp_nm,
    chn_offc_scnd_ant_psn_cd::VARCHAR(256) AS chn_offc_scnd_ant_psn_cd,
    chn_offc_scnd_sal_grp::VARCHAR(256) AS chn_offc_scnd_sal_grp,
    chn_offc_scnd_sal_grp_nm::VARCHAR(256) AS chn_offc_scnd_sal_grp_nm,
    chn_cd_oth::VARCHAR(256) AS chn_cd_oth,
    emp_cd_kk::VARCHAR(256) AS emp_cd_kk,
    all_str_ass::VARCHAR(256) AS all_str_ass,
    agrm_str::VARCHAR(256) AS agrm_str,
    pjass::VARCHAR(256) AS pjass,
    emp_cd_roc::VARCHAR(256) AS emp_cd_roc,
    area_nm::VARCHAR(256) AS area
  FROM trns
  )
SELECT *
FROM final