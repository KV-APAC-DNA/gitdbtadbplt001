{{
    config(
        materialized='view'
    )
}}

with edw_copa_trans_fact as
(
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
v_intrm_reg_crncy_exch as
(
    select * from {{ ref('aspedw_integration__v_intrm_reg_crncy_exch') }}
),
final as
(
    SELECT 
    c.fisc_yr, 
    c.fisc_yr_per, 
    CASE WHEN (c.fisc_yr_per <= 2019004) THEN (
        (
        sum(c.dr) / (
            sum(c.gts)
        ):: numeric(25, 8)
        ) * (
        (100):: numeric
        ):: numeric(18, 0)
    ) ELSE (
        (0):: numeric
    ):: numeric(18, 0) END AS dr_per 
    FROM 
    (
        SELECT 
        b.fisc_yr, 
        b.fisc_yr_per, 
        CASE WHEN (
            (b.gts_dr):: text = ('gts' :: character varying):: text
        ) THEN sum(b.ytd_amt) ELSE (NULL :: numeric):: numeric(18, 0) END AS gts, 
        CASE WHEN (
            (b.gts_dr):: text = ('dr' :: character varying):: text
        ) THEN "max"(b.ytd_amt) ELSE (NULL :: numeric):: numeric(18, 0) END AS dr 
        FROM 
        (
            SELECT 
            a.fisc_yr, 
            a.fisc_yr_per, 
            a.gts_dr, 
            a.acct_hier_shrt_desc, 
            sum(a.amt_obj_crncy) OVER(
                PARTITION BY a.fisc_yr, 
                a.gts_dr 
                ORDER BY 
                a.fisc_yr, 
                a.fisc_yr_per, 
                a.gts_dr, 
                a.acct_hier_shrt_desc) AS ytd_amt 
            FROM 
            (
                SELECT 
                copa.fisc_yr, 
                copa.fisc_yr_per, 
                copa.acct_hier_shrt_desc, 
                copa.obj_crncy_co_obj, 
                sum(
                    (
                    copa.amt_obj_crncy * exch_rate.ex_rt
                    )
                ) AS amt_obj_crncy, 
                CASE WHEN (
                    (copa.acct_hier_shrt_desc):: text = ('GTS' :: character varying):: text
                ) THEN 'gts' :: character varying WHEN (
                    (
                    (
                        (
                        (
                            (
                            (
                                (copa.acct_hier_shrt_desc):: text = ('SA' :: character varying):: text
                            ) 
                            OR (
                                (copa.acct_hier_shrt_desc):: text = ('PSO' :: character varying):: text
                            )
                            ) 
                            OR (
                            (copa.acct_hier_shrt_desc):: text = ('PMA' :: character varying):: text
                            )
                        ) 
                        OR (
                            (copa.acct_hier_shrt_desc):: text = ('VGF' :: character varying):: text
                        )
                        ) 
                        OR (
                        (copa.acct_hier_shrt_desc):: text = ('TLO' :: character varying):: text
                        )
                    ) 
                    OR (
                        (copa.acct_hier_shrt_desc):: text = ('LF' :: character varying):: text
                    )
                    ) 
                    OR (
                    (copa.acct_hier_shrt_desc):: text = ('CD' :: character varying):: text
                    )
                ) THEN 'dr' :: character varying ELSE NULL :: character varying END AS gts_dr 
                FROM 
                (
                    edw_copa_trans_fact copa 
                    LEFT JOIN v_intrm_reg_crncy_exch exch_rate ON (
                    (
                        (
                        (copa.obj_crncy_co_obj):: text = (exch_rate.from_crncy):: text
                        ) 
                        AND (
                        (exch_rate.to_crncy):: text = ('USD' :: character varying):: text
                        )
                    )
                    )
                ) 
                WHERE 
                (
                    (
                    (copa.ctry_key):: text = ('JP' :: character varying):: text
                    ) 
                    AND (
                    (
                        (
                        (
                            (
                            (
                                (
                                (
                                    (copa.acct_hier_shrt_desc):: text = ('GTS' :: character varying):: text
                                ) 
                                OR (
                                    (copa.acct_hier_shrt_desc):: text = ('SA' :: character varying):: text
                                )
                                ) 
                                OR (
                                (copa.acct_hier_shrt_desc):: text = ('PSO' :: character varying):: text
                                )
                            ) 
                            OR (
                                (copa.acct_hier_shrt_desc):: text = ('PMA' :: character varying):: text
                            )
                            ) 
                            OR (
                            (copa.acct_hier_shrt_desc):: text = ('VGF' :: character varying):: text
                            )
                        ) 
                        OR (
                            (copa.acct_hier_shrt_desc):: text = ('TLO' :: character varying):: text
                        )
                        ) 
                        OR (
                        (copa.acct_hier_shrt_desc):: text = ('LF' :: character varying):: text
                        )
                    ) 
                    OR (
                        (copa.acct_hier_shrt_desc):: text = ('CD' :: character varying):: text
                    )
                    )
                ) 
                GROUP BY 
                copa.fisc_yr, 
                copa.fisc_yr_per, 
                copa.obj_crncy_co_obj, 
                copa.acct_hier_shrt_desc 
                ORDER BY 
                copa.fisc_yr, 
                copa.fisc_yr_per, 
                copa.acct_hier_shrt_desc
            ) a
        ) b 
        GROUP BY 
        b.fisc_yr, 
        b.fisc_yr_per, 
        b.gts_dr
    ) c 
    WHERE 
    (c.fisc_yr >= 2018) 
    GROUP BY 
    c.fisc_yr, 
    c.fisc_yr_per 
    ORDER BY 
    c.fisc_yr, 
    c.fisc_yr_per
)
select * from final