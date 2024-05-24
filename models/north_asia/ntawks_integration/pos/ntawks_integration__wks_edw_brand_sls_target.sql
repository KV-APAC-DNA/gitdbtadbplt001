with source as 
(
    select * from snapntaitg_integration.itg_kr_sales_target_am_brand
),
edw_calendar_dim as 
(
    select * from snapntaedw_integration.edw_calendar_dim
),
itg_kr_sales_target_am_cust_link as 
(
    select * from snapntaitg_integration.itg_kr_sales_target_am_cust_link
),
final as
(
    select x.brnd,
        x.ctry_cd,
        x.acct_mgr,
        c.cust_num,
        x.yr,
        x.mo,
        CASE
            WHEN x.mo = 1 THEN x.jan_trgt_amt
            WHEN x.mo = 2 THEN x.feb_trgt_amt
            WHEN x.mo = 3 THEN x.mar_trgt_amt
            WHEN x.mo = 4 THEN x.apr_trgt_amt
            WHEN x.mo = 5 THEN x.may_trgt_amt
            WHEN x.mo = 6 THEN x.jun_trgt_amt
            WHEN x.mo = 7 THEN x.jul_trgt_amt
            WHEN x.mo = 8 THEN x.aug_trgt_amt
            WHEN x.mo = 9 THEN x.sep_trgt_amt
            WHEN x.mo = 10 THEN x.oct_trgt_amt
            WHEN x.mo = 11 THEN x.nov_trgt_amt
            WHEN x.mo = 12 THEN x.dec_trgt_amt
            ELSE 0::numeric::numeric(18, 0)
        end as brnd_trgt
    from
        (
            SELECT 
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
            FROM source a
                cross join 
                (
                    select distinct cal_mo_2 from edw_calendar_dim
                ) b
        ) x
        left outer join itg_kr_sales_target_am_cust_link c on trim(x.acct_mgr) = trim(c.acct_mgr)
)
select * from final