with v_intrm_copa_trd_prc_tp_spend as (
    select * from {{ ref('ntaedw_integration__v_intrm_copa_trd_prc_tp_spend') }}
),
v_intrm_cust_tp_plan_actl as (
    select * from {{ ref('ntaedw_integration__v_intrm_cust_tp_plan_actl') }}
),
edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
b as (
    SELECT b.prft_ctr,
        b.cust_hier_l2,
        b.cust_hdqtr_cd,
        b.cust_hdqtr_nm,
        b.ctry_cd,
        b.yr,
        a.mo,
        b.crncy_cd,
        CASE
            WHEN (a.mo = 1) THEN b.jan_trd_promo_pln
            WHEN (a.mo = 2) THEN b.feb_trd_promo_pln
            WHEN (a.mo = 3) THEN b.mar_trd_promo_pln
            WHEN (a.mo = 4) THEN b.apr_trd_promo_pln
            WHEN (a.mo = 5) THEN b.may_trd_promo_pln
            WHEN (a.mo = 6) THEN b.jun_trd_promo_pln
            WHEN (a.mo = 7) THEN b.jul_trd_promo_pln
            WHEN (a.mo = 8) THEN b.aug_trd_promo_pln
            WHEN (a.mo = 9) THEN b.sep_trd_promo_pln
            WHEN (a.mo = 10) THEN b.oct_trd_promo_pln
            WHEN (a.mo = 11) THEN b.nov_trd_promo_pln
            WHEN (a.mo = 12) THEN b.dec_trd_promo_pln
            ELSE ((0)::numeric)::numeric(18, 0)
        END AS ttl_tp_budget,
        CASE
            WHEN (a.mo = 1) THEN b.jan_actl_trd_promo
            WHEN (a.mo = 2) THEN b.feb_actl_trd_promo
            WHEN (a.mo = 3) THEN b.mar_actl_trd_promo
            WHEN (a.mo = 4) THEN b.apr_actl_trd_promo
            WHEN (a.mo = 5) THEN b.may_actl_trd_promo
            WHEN (a.mo = 6) THEN b.jun_actl_trd_promo
            WHEN (a.mo = 7) THEN b.jul_actl_trd_promo
            WHEN (a.mo = 8) THEN b.aug_actl_trd_promo
            WHEN (a.mo = 9) THEN b.sep_actl_trd_promo
            WHEN (a.mo = 10) THEN b.oct_actl_trd_promo
            WHEN (a.mo = 11) THEN b.nov_actl_trd_promo
            WHEN (a.mo = 12) THEN b.dec_actl_trd_promo
            ELSE ((0)::numeric)::numeric(18, 0)
        END AS ttl_tp_actual,
        b.to_crncy,
        b.ex_rt
    FROM (
            v_intrm_cust_tp_plan_actl b
            CROSS JOIN (
                SELECT DISTINCT edw_calendar_dim.cal_mo_2 AS mo
                FROM edw_calendar_dim
            ) a
        )
),
final as (
    SELECT a.co_cd,
        COALESCE(a.co_nm, 'Not Available'::character varying) AS co_nm,
        CASE
            WHEN (a.prft_ctr IS NULL) THEN ltrim(
                (b.prft_ctr)::text,
                ((0)::character varying)::text
            )
            ELSE ltrim(
                (a.prft_ctr)::text,
                ((0)::character varying)::text
            )
        END AS prft_ctr,
        CASE
            WHEN (a.cust_num IS NULL) THEN ltrim(
                (b.cust_hdqtr_cd)::text,
                ((0)::character varying)::text
            )
            ELSE ltrim(
                (a.cust_num)::text,
                ((0)::character varying)::text
            )
        END AS cust_hdqtr_cd,
        COALESCE(b.cust_hdqtr_nm, a.cust_hier_l2) AS cust_hdqtr_nm,
        CASE
            WHEN (a.cust_hier_l2 IS NULL) THEN 'Others'::character varying
            WHEN (
                (a.cust_hier_l2)::text = ('#'::character varying)::text
            ) THEN 'Others'::character varying
            ELSE a.cust_hier_l2
        END AS edw_cust_nm,
        CASE
            WHEN (a.ctry_nm IS NULL) THEN CASE
                WHEN (
                    (b.ctry_cd)::text = ('KR'::character varying)::text
                ) THEN 'South Korea'::character varying
                WHEN (
                    (b.ctry_cd)::text = ('TW'::character varying)::text
                ) THEN 'Taiwan'::character varying
                ELSE 'N/A'::character varying
            END
            ELSE a.ctry_nm
        END AS ctry_nm,
        CASE
            WHEN (a.ctry_cd IS NULL) THEN b.ctry_cd
            ELSE a.ctry_cd
        END AS ctry_cd,
        a.sls_grp_desc,
        CASE
            WHEN (a.yr IS NULL) THEN (b.yr)::integer
            ELSE a.yr
        END AS yr,
        CASE
            WHEN (a.mo IS NULL) THEN b.mo
            ELSE a.mo
        END AS mo,
        COALESCE(
            a.mega_brnd_desc,
            'Not Available'::character varying
        ) AS mega_brnd_desc,
        COALESCE(a.brnd_desc, 'Not Available'::character varying) AS brnd_desc,
        CASE
            WHEN (a.crncy_cd IS NULL) THEN b.crncy_cd
            ELSE a.crncy_cd
        END AS crncy_cd,
        a.sls_amt,
        CASE
            WHEN (a.to_crncy IS NULL) THEN b.to_crncy
            ELSE a.to_crncy
        END AS to_crncy,
        a.ex_rt,
        b.ttl_tp_budget,
        b.ttl_tp_actual,
        b.ex_rt AS tp_ex_rt
    FROM v_intrm_copa_trd_prc_tp_spend a
        FULL JOIN b ON ltrim((a.prft_ctr)::text, ((0)::character varying)::text) = ltrim((b.prft_ctr)::text, ((0)::character varying)::text)
        AND ltrim((b.cust_hdqtr_cd)::text, ('0'::character varying)::text) = ltrim((a.cust_num)::text, ((0)::character varying)::text)
        AND (COALESCE(a.cust_hier_l2, 'Others'::character varying))::text = (COALESCE(b.cust_hier_l2, 'Others'::character varying))::text
        AND (((a.yr)::character varying)::text = (b.yr)::text)
        AND ((a.crncy_cd)::text = (b.crncy_cd)::text)
        AND ((a.to_crncy)::text = (b.to_crncy)::text)
        AND ((a.ctry_cd)::text = (b.ctry_cd)::text)
        AND (a.mo = b.mo)
)
select * from final