with edw_trd_promo_pln as (
    select * from snapntaedw_integration.edw_trd_promo_pln
),
edw_trd_promo_actl as (
    select * from snapntaedw_integration.edw_trd_promo_actl
),
edw_customer_attr_hier_dim as (
    select * from snapaspedw_integration.edw_customer_attr_hier_dim
),
v_intrm_crncy_exch as (
    select * from snapntaedw_integration.v_intrm_crncy_exch
),
a as (
    SELECT CASE
            WHEN (pln.prft_ctr IS NOT NULL) THEN pln.prft_ctr
            ELSE actl.prft_ctr
        END AS prft_ctr,
        CASE
            WHEN (pln.cust_hdqtr_cd IS NOT NULL) THEN pln.cust_hdqtr_cd
            ELSE actl.cust_hdqtr_cd
        END AS cust_hdqtr_cd,
        CASE
            WHEN (pln.cust_hdqtr_nm IS NOT NULL) THEN pln.cust_hdqtr_nm
            ELSE '#'::character varying
        END AS cust_hdqtr_nm,
        CASE
            WHEN (pln.ctry_cd IS NOT NULL) THEN pln.ctry_cd
            ELSE actl.ctry_cd
        END AS ctry_cd,
        CASE
            WHEN (pln.yr IS NOT NULL) THEN pln.yr
            ELSE actl.yr
        END AS yr,
        CASE
            WHEN (pln.crncy_cd IS NOT NULL) THEN pln.crncy_cd
            ELSE actl.crncy_cd
        END AS crncy_cd,
        sum(pln.jan_trd_promo_pln) AS jan_trd_promo_pln,
        sum(pln.feb_trd_promo_pln) AS feb_trd_promo_pln,
        sum(pln.mar_trd_promo_pln) AS mar_trd_promo_pln,
        sum(pln.apr_trd_promo_pln) AS apr_trd_promo_pln,
        sum(pln.may_trd_promo_pln) AS may_trd_promo_pln,
        sum(pln.jun_trd_promo_pln) AS jun_trd_promo_pln,
        sum(pln.jul_trd_promo_pln) AS jul_trd_promo_pln,
        sum(pln.aug_trd_promo_pln) AS aug_trd_promo_pln,
        sum(pln.sep_trd_promo_pln) AS sep_trd_promo_pln,
        sum(pln.oct_trd_promo_pln) AS oct_trd_promo_pln,
        sum(pln.nov_trd_promo_pln) AS nov_trd_promo_pln,
        sum(pln.dec_trd_promo_pln) AS dec_trd_promo_pln,
        sum(actl.jan_actl_trd_promo) AS jan_actl_trd_promo,
        sum(actl.feb_actl_trd_promo) AS feb_actl_trd_promo,
        sum(actl.mar_actl_trd_promo) AS mar_actl_trd_promo,
        sum(actl.apr_actl_trd_promo) AS apr_actl_trd_promo,
        sum(actl.may_actl_trd_promo) AS may_actl_trd_promo,
        sum(actl.jun_actl_trd_promo) AS jun_actl_trd_promo,
        sum(actl.jul_actl_trd_promo) AS jul_actl_trd_promo,
        sum(actl.aug_actl_trd_promo) AS aug_actl_trd_promo,
        sum(actl.sep_actl_trd_promo) AS sep_actl_trd_promo,
        sum(actl.oct_actl_trd_promo) AS oct_actl_trd_promo,
        sum(actl.nov_actl_trd_promo) AS nov_actl_trd_promo,
        sum(actl.dec_actl_trd_promo) AS dec_actl_trd_promo
    FROM (
            edw_trd_promo_pln pln
            FULL JOIN edw_trd_promo_actl actl ON (
                (
                    (
                        (
                            ((pln.prft_ctr)::text = (actl.prft_ctr)::text)
                            AND (
                                (pln.cust_hdqtr_cd)::text = (actl.cust_hdqtr_cd)::text
                            )
                        )
                        AND ((pln.ctry_cd)::text = (actl.ctry_cd)::text)
                    )
                    AND ((pln.yr)::text = (actl.yr)::text)
                )
            )
        )
    GROUP BY CASE
            WHEN (pln.prft_ctr IS NOT NULL) THEN pln.prft_ctr
            ELSE actl.prft_ctr
        END,
        CASE
            WHEN (pln.ctry_cd IS NOT NULL) THEN pln.ctry_cd
            ELSE actl.ctry_cd
        END,
        CASE
            WHEN (pln.yr IS NOT NULL) THEN pln.yr
            ELSE actl.yr
        END,
        CASE
            WHEN (pln.crncy_cd IS NOT NULL) THEN pln.crncy_cd
            ELSE actl.crncy_cd
        END,
        CASE
            WHEN (pln.cust_hdqtr_nm IS NOT NULL) THEN pln.cust_hdqtr_nm
            ELSE '#'::character varying
        END,
        CASE
            WHEN (pln.cust_hdqtr_cd IS NOT NULL) THEN pln.cust_hdqtr_cd
            ELSE actl.cust_hdqtr_cd
        END
),
b as (
    SELECT DISTINCT edw_customer_attr_hier_dim.sold_to_party,
        CASE
            WHEN (
                (edw_customer_attr_hier_dim.cntry)::text = ('Korea'::character varying)::text
            ) THEN 'KR'::character varying
            WHEN (
                (edw_customer_attr_hier_dim.cntry)::text = ('Taiwan'::character varying)::text
            ) THEN 'TW'::character varying
            ELSE 'Hong Kong'::character varying
        END AS ctry_cd,
        "max"((edw_customer_attr_hier_dim.cust_hier_l2)::text) AS cust_hier_l2
    FROM edw_customer_attr_hier_dim
    WHERE (
            (edw_customer_attr_hier_dim.trgt_type)::text = ('hierarchy'::character varying)::text
        )
    GROUP BY edw_customer_attr_hier_dim.sold_to_party,
        CASE
            WHEN (
                (edw_customer_attr_hier_dim.cntry)::text = ('Korea'::character varying)::text
            ) THEN 'KR'::character varying
            WHEN (
                (edw_customer_attr_hier_dim.cntry)::text = ('Taiwan'::character varying)::text
            ) THEN 'TW'::character varying
            ELSE 'Hong Kong'::character varying
        END
),
x as (
    SELECT a.prft_ctr,
        a.ctry_cd,
        a.yr,
        a.crncy_cd,
        a.cust_hdqtr_cd,
        a.cust_hdqtr_nm,
        b.cust_hier_l2,
        sum(a.jan_trd_promo_pln) AS jan_trd_promo_pln,
        sum(a.feb_trd_promo_pln) AS feb_trd_promo_pln,
        sum(a.mar_trd_promo_pln) AS mar_trd_promo_pln,
        sum(a.apr_trd_promo_pln) AS apr_trd_promo_pln,
        sum(a.may_trd_promo_pln) AS may_trd_promo_pln,
        sum(a.jun_trd_promo_pln) AS jun_trd_promo_pln,
        sum(a.jul_trd_promo_pln) AS jul_trd_promo_pln,
        sum(a.aug_trd_promo_pln) AS aug_trd_promo_pln,
        sum(a.sep_trd_promo_pln) AS sep_trd_promo_pln,
        sum(a.oct_trd_promo_pln) AS oct_trd_promo_pln,
        sum(a.nov_trd_promo_pln) AS nov_trd_promo_pln,
        sum(a.dec_trd_promo_pln) AS dec_trd_promo_pln,
        sum(a.jan_actl_trd_promo) AS jan_actl_trd_promo,
        sum(a.feb_actl_trd_promo) AS feb_actl_trd_promo,
        sum(a.mar_actl_trd_promo) AS mar_actl_trd_promo,
        sum(a.apr_actl_trd_promo) AS apr_actl_trd_promo,
        sum(a.may_actl_trd_promo) AS may_actl_trd_promo,
        sum(a.jun_actl_trd_promo) AS jun_actl_trd_promo,
        sum(a.jul_actl_trd_promo) AS jul_actl_trd_promo,
        sum(a.aug_actl_trd_promo) AS aug_actl_trd_promo,
        sum(a.sep_actl_trd_promo) AS sep_actl_trd_promo,
        sum(a.oct_actl_trd_promo) AS oct_actl_trd_promo,
        sum(a.nov_actl_trd_promo) AS nov_actl_trd_promo,
        sum(a.dec_actl_trd_promo) AS dec_actl_trd_promo
    FROM (
            a
            LEFT JOIN b ON (
                (
                    (
                        ltrim(
                            (b.sold_to_party)::text,
                            ((0)::character varying)::text
                        ) = (a.cust_hdqtr_cd)::text
                    )
                    AND ((a.ctry_cd)::text = (b.ctry_cd)::text)
                )
            )
        )
    GROUP BY a.prft_ctr,
        a.ctry_cd,
        a.yr,
        a.cust_hdqtr_cd,
        a.cust_hdqtr_nm,
        a.crncy_cd,
        b.cust_hier_l2
),
final as (
    SELECT x.prft_ctr,
        (
            COALESCE(x.cust_hier_l2, ('#'::character varying)::text)
        )::character varying(500) AS cust_hier_l2,
        x.ctry_cd,
        x.cust_hdqtr_cd,
        x.cust_hdqtr_nm,
        x.yr,
        x.crncy_cd,
        sum(x.jan_trd_promo_pln) AS jan_trd_promo_pln,
        sum(x.feb_trd_promo_pln) AS feb_trd_promo_pln,
        sum(x.mar_trd_promo_pln) AS mar_trd_promo_pln,
        sum(x.apr_trd_promo_pln) AS apr_trd_promo_pln,
        sum(x.may_trd_promo_pln) AS may_trd_promo_pln,
        sum(x.jun_trd_promo_pln) AS jun_trd_promo_pln,
        sum(x.jul_trd_promo_pln) AS jul_trd_promo_pln,
        sum(x.aug_trd_promo_pln) AS aug_trd_promo_pln,
        sum(x.sep_trd_promo_pln) AS sep_trd_promo_pln,
        sum(x.oct_trd_promo_pln) AS oct_trd_promo_pln,
        sum(x.nov_trd_promo_pln) AS nov_trd_promo_pln,
        sum(x.dec_trd_promo_pln) AS dec_trd_promo_pln,
        sum(x.jan_actl_trd_promo) AS jan_actl_trd_promo,
        sum(x.feb_actl_trd_promo) AS feb_actl_trd_promo,
        sum(x.mar_actl_trd_promo) AS mar_actl_trd_promo,
        sum(x.apr_actl_trd_promo) AS apr_actl_trd_promo,
        sum(x.may_actl_trd_promo) AS may_actl_trd_promo,
        sum(x.jun_actl_trd_promo) AS jun_actl_trd_promo,
        sum(x.jul_actl_trd_promo) AS jul_actl_trd_promo,
        sum(x.aug_actl_trd_promo) AS aug_actl_trd_promo,
        sum(x.sep_actl_trd_promo) AS sep_actl_trd_promo,
        sum(x.oct_actl_trd_promo) AS oct_actl_trd_promo,
        sum(x.nov_actl_trd_promo) AS nov_actl_trd_promo,
        sum(x.dec_actl_trd_promo) AS dec_actl_trd_promo,
        y.to_crncy,
        y.ex_rt
    FROM x
        LEFT JOIN v_intrm_crncy_exch y ON (((x.crncy_cd)::text = (y.from_crncy)::text))
    GROUP BY x.prft_ctr,
        x.cust_hier_l2,
        x.cust_hdqtr_cd,
        x.cust_hdqtr_nm,
        x.ctry_cd,
        x.yr,
        x.crncy_cd,
        y.to_crncy,
        y.ex_rt
)
select * from final