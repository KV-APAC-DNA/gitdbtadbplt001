with edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
itg_kr_sales_target_am_sls_grp as (
    select * from {{ ref('ntaitg_integration__itg_kr_sales_target_am_sls_grp') }} 
),
itg_kr_sales_target_am_cust_link as (
    select * from {{ ref('ntaitg_integration__itg_kr_sales_target_am_cust_link') }} 
),
final as
(
    select 
        catg_sls_tgt_matrix.sls_grp,
        catg_sls_tgt_matrix.brnd,
        catg_sls_tgt_matrix.acct_mgr,
        catg_sls_tgt_matrix.ctry_cd,
        c.cust_num,
        catg_sls_tgt_matrix.yr,
        catg_sls_tgt_matrix.mo,
        CASE
            WHEN catg_sls_tgt_matrix.mo = 1 THEN catg_sls_tgt_matrix.jan_trgt_amt
            WHEN catg_sls_tgt_matrix.mo = 2 THEN catg_sls_tgt_matrix.feb_trgt_amt
            WHEN catg_sls_tgt_matrix.mo = 3 THEN catg_sls_tgt_matrix.mar_trgt_amt
            WHEN catg_sls_tgt_matrix.mo = 4 THEN catg_sls_tgt_matrix.apr_trgt_amt
            WHEN catg_sls_tgt_matrix.mo = 5 THEN catg_sls_tgt_matrix.may_trgt_amt
            WHEN catg_sls_tgt_matrix.mo = 6 THEN catg_sls_tgt_matrix.jun_trgt_amt
            WHEN catg_sls_tgt_matrix.mo = 7 THEN catg_sls_tgt_matrix.jul_trgt_amt
            WHEN catg_sls_tgt_matrix.mo = 8 THEN catg_sls_tgt_matrix.aug_trgt_amt
            WHEN catg_sls_tgt_matrix.mo = 9 THEN catg_sls_tgt_matrix.sep_trgt_amt
            WHEN catg_sls_tgt_matrix.mo = 10 THEN catg_sls_tgt_matrix.oct_trgt_amt
            WHEN catg_sls_tgt_matrix.mo = 11 THEN catg_sls_tgt_matrix.nov_trgt_amt
            WHEN catg_sls_tgt_matrix.mo = 12 THEN catg_sls_tgt_matrix.dec_trgt_amt
            ELSE 0::numeric::numeric(18, 0)
        end as sls_grp_cat_trgt
    from
        (
            SELECT 
                a.sls_grp,
                a.brnd,
                a.acct_mgr,
                a.yr,
                b.cal_mo_2 as mo,
                a.jan_trgt_amt,
                a.feb_trgt_amt,
                a.mar_trgt_amt,
                a.apr_trgt_amt,
                a.may_trgt_amt,
                a.jun_trgt_amt,
                a.jul_trgt_amt,
                a.aug_trgt_amt,
                a.sep_trgt_amt,
                a.oct_trgt_amt,
                a.nov_trgt_amt,
                a.dec_trgt_amt,
                a.ctry_cd
            FROM itg_kr_sales_target_am_sls_grp a
                cross join 
                (
                    select distinct cal_mo_2 from edw_calendar_dim
                ) b
        ) catg_sls_tgt_matrix
        left outer join itg_kr_sales_target_am_cust_link c on trim(catg_sls_tgt_matrix.acct_mgr) = trim(c.acct_mgr)
)
select * from final