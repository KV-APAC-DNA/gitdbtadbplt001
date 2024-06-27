with edw_copa_trans_fact as 
(
    select * from snapaspedw_integration.edw_copa_trans_fact
),
final as
(   
    SELECT 
        CASE
            WHEN ((x.ctry_key)::text = 'HK'::text) THEN 'Hong Kong'::text
            WHEN ((x.ctry_key)::text = 'TW'::text) THEN 'Taiwan'::text
            WHEN ((x.ctry_key)::text = 'KR'::text) THEN 'South Korea'::text
            ELSE NULL::text
        END AS country,
        "max"((x.caln_day)::text) AS last_txn_dt
    FROM edw_copa_trans_fact x
    WHERE 
        (
            (
                (
                    (
                        (
                            (x.acct_hier_shrt_desc)::text = 'GTS'::character varying::text
                            OR (x.acct_hier_shrt_desc)::text = 'CD'::character varying::text
                            OR (x.acct_hier_shrt_desc)::text = 'LF'::character varying::text
                            OR (x.acct_hier_shrt_desc)::text = 'PSO'::character varying::text
                            OR (x.acct_hier_shrt_desc)::text = 'PMA'::character varying::text
                            OR (x.acct_hier_shrt_desc)::text = 'RTN'::character varying::text
                            OR (x.acct_hier_shrt_desc)::text = 'SA'::character varying::text
                            OR (x.acct_hier_shrt_desc)::text = 'TLO'::character varying::text
                            OR (x.acct_hier_shrt_desc)::text = 'VGF'::character varying::text
                            OR (x.acct_hier_shrt_desc)::text = 'NTS'::character varying::text
                        )
                        AND (
                            "substring"(((x.fisc_yr_per)::character varying)::text, 1, 4)::integer >= date_part(
                                year,
                                convert_timezone('UTC', current_timestamp())::timestamp without time zone
                            )::integer - 3
                        )
                    )
                    AND (
                        "substring"(((x.fisc_yr_per)::character varying)::text, 1, 4)::integer <= date_part(
                            year,
                            convert_timezone('UTC', current_timestamp())::timestamp without time zone
                        )::integer
                    )
                )
            )
            AND (
                upper((x.ctry_key)::text) = 'HK'::text
                OR upper((x.ctry_key)::text) = 'TW'::text
                OR upper((x.ctry_key)::text) = 'KR'::text
            )
        )
    GROUP BY x.ctry_key
)
select * from final