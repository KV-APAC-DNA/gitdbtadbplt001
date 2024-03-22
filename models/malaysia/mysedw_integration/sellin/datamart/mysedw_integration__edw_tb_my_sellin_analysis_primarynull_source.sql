{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )
}}
--Import CTEs
with edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_tb_my_curr_prev_sellin_fact as (
    select * from {{ ref('mysedw_integration__edw_tb_my_curr_prev_sellin_fact') }}
),

ds_primary_null_src as  (
                                                    SELECT veotd.jj_year,
                                                        veotd.jj_qtr,
                                                        veotd.jj_mnth_id,
                                                        veotd.jj_mnth_no,
                                                        veossf1.cntry_nm,
                                                        ltrim(
                                                            (veossf1.item_cd)::text,
                                                            ('0'::character varying)::text
                                                        ) AS item_cd,
                                                        ltrim(
                                                            (veossf1.cust_id)::text,
                                                            ('0'::character varying)::text
                                                        ) AS cust_id,
                                                        veossf1.acct_no,
                                                        sum(veossf1.base_val) AS base_val,
                                                        sum(veossf1.sls_qty) AS sls_qty,
                                                        sum(veossf1.ret_qty) AS ret_qty,
                                                        sum(veossf1.sls_less_rtn_qty) AS sls_less_rtn_qty,
                                                        sum(veossf1.gts_val) AS gts_val,
                                                        sum(veossf1.ret_val) AS ret_val,
                                                        sum(veossf1.gts_less_rtn_val) AS gts_less_rtn_val,
                                                        sum(veossf1.trdng_term_val) AS trdng_term_val,
                                                        sum(veossf1.tp_val) AS tp_val,
                                                        sum(veossf1.trde_prmtn_val) AS trde_prmtn_val,
                                                        sum(veossf1.nts_val) AS nts_val,
                                                        sum(veossf1.nts_qty) AS nts_qty,
                                                        veossf1.is_curr
                                                    FROM edw_tb_my_curr_prev_sellin_fact veossf1,
                                                        (
                                                            SELECT a."year" AS jj_year,
                                                                a.qrtr_no,
                                                                a.qrtr AS jj_qtr,
                                                                a.mnth_id AS jj_mnth_id,
                                                                a.mnth_desc,
                                                                a.mnth_no AS jj_mnth_no,
                                                                a.mnth_shrt,
                                                                a.mnth_long
                                                            FROM edw_vw_os_time_dim a
                                                            GROUP BY a."year",
                                                                a.qrtr_no,
                                                                a.qrtr,
                                                                a.mnth_id,
                                                                a.mnth_desc,
                                                                a.mnth_no,
                                                                a.mnth_shrt,
                                                                a.mnth_long
                                                        ) veotd
                                                    WHERE (
                                                            (
                                                                (veossf1.cntry_nm)::text = ('MY'::character varying)::text
                                                            )
                                                            AND (
                                                                trim((veossf1.jj_mnth_id)::text) = (
                                                                    (
                                                                        (veotd.jj_mnth_id)::number(18, 0)
                                                                    )::character varying
                                                                )::text
                                                            )
                                                        )
                                                    GROUP BY veotd.jj_year,
                                                        veotd.jj_qtr,
                                                        veotd.jj_mnth_id,
                                                        veotd.jj_mnth_no,
                                                        veossf1.cntry_nm,
                                                        veossf1.item_cd,
                                                        veossf1.cust_id,
                                                        veossf1.acct_no,
                                                        veossf1.is_curr
) 

select * from ds_primary_null_src