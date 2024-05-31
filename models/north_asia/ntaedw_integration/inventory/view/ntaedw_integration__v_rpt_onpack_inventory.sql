with edw_inventory_fact as (
    select * from {{ ref('aspedw_integration__edw_inventory_fact') }}
),
edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
t1 as (
    SELECT t1.co_cd,
        t1.matl_no,
        t1.fisc_per,
        t2.p_fisc_yr_per
    FROM (
            SELECT t1.co_cd,
                t1.matl_no,
                t1.min_fisc_yr_per,
                t1.max_fisc_yr_per,
                t2.fisc_per
            FROM (
                    SELECT DISTINCT edw_inventory_fact.co_cd,
                        edw_inventory_fact.matl_no,
                        min(edw_inventory_fact.fisc_yr_per) AS min_fisc_yr_per,
                        "max"(edw_inventory_fact.fisc_yr_per) AS max_fisc_yr_per
                    FROM edw_inventory_fact,
                        (
                            SELECT edw_calendar_dim.fisc_per
                            FROM edw_calendar_dim
                            WHERE (
                                    to_date(
                                        (edw_calendar_dim.cal_day)::timestamp without time zone
                                    ) = to_date(
                                        current_timestamp()::timestamp without time zone
                                    )
                                )
                        ) derived_table1
                    GROUP BY edw_inventory_fact.co_cd,
                        edw_inventory_fact.matl_no
                ) t1,
                (
                    SELECT DISTINCT edw_calendar_dim.fisc_per
                    FROM edw_calendar_dim
                ) t2
            WHERE (
                    (t2.fisc_per >= t1.min_fisc_yr_per)
                    AND (t2.fisc_per <= t1.max_fisc_yr_per)
                )
        ) t1,
        (
            SELECT derived_table3.co_cd,
                derived_table3.matl_no,
                derived_table3.fisc_yr_per,
                derived_table3.p_fisc_yr_per,
                (
                    months_between(
                        (
                            to_date(
                                (
                                    "substring"(
                                        ((derived_table3.fisc_yr_per)::character varying)::text,
                                        1,
                                        4
                                    ) || "substring"(
                                        ((derived_table3.fisc_yr_per)::character varying)::text,
                                        6,
                                        4
                                    )
                                ),
                                ('YYYYMM'::character varying)::text
                            )
                        )::timestamp without time zone,
                        (
                            to_date(
                                (
                                    "substring"(
                                        (
                                            (derived_table3.p_fisc_yr_per)::character varying
                                        )::text,
                                        1,
                                        4
                                    ) || "substring"(
                                        (
                                            (derived_table3.p_fisc_yr_per)::character varying
                                        )::text,
                                        6,
                                        4
                                    )
                                ),
                                ('YYYYMM'::character varying)::text
                            )
                        )::timestamp without time zone
                    ) - (1)::double precision
                ) AS distance
            FROM (
                    SELECT derived_table2.co_cd,
                        derived_table2.matl_no,
                        derived_table2.fisc_yr_per,
                        lead(derived_table2.fisc_yr_per, 1) OVER(
                            PARTITION BY derived_table2.co_cd,
                            derived_table2.matl_no
                            ORDER BY derived_table2.co_cd DESC,
                                derived_table2.matl_no DESC,
                                derived_table2.fisc_yr_per DESC
                        ) AS p_fisc_yr_per
                    FROM (
                            SELECT DISTINCT edw_inventory_fact.co_cd,
                                edw_inventory_fact.matl_no,
                                edw_inventory_fact.fisc_yr_per
                            FROM edw_inventory_fact
                        ) derived_table2
                ) derived_table3
            WHERE (
                    (derived_table3.p_fisc_yr_per IS NOT NULL)
                    AND (
                        (
                            months_between(
                                (
                                    to_date(
                                        (
                                            "substring"(
                                                ((derived_table3.fisc_yr_per)::character varying)::text,
                                                1,
                                                4
                                            ) || "substring"(
                                                ((derived_table3.fisc_yr_per)::character varying)::text,
                                                6,
                                                4
                                            )
                                        ),
                                        ('YYYYMM'::character varying)::text
                                    )
                                )::timestamp without time zone,
                                (
                                    to_date(
                                        (
                                            "substring"(
                                                (
                                                    (derived_table3.p_fisc_yr_per)::character varying
                                                )::text,
                                                1,
                                                4
                                            ) || "substring"(
                                                (
                                                    (derived_table3.p_fisc_yr_per)::character varying
                                                )::text,
                                                6,
                                                4
                                            )
                                        ),
                                        ('YYYYMM'::character varying)::text
                                    )
                                )::timestamp without time zone
                            ) - (1)::double precision
                        ) > (0)::double precision
                    )
                )
        ) t2
    WHERE (
            (
                (
                    ((t1.matl_no)::text = (t2.matl_no)::text)
                    AND ((t1.co_cd)::text = (t2.co_cd)::text)
                )
                AND (t1.fisc_per > t2.p_fisc_yr_per)
            )
            AND (t1.fisc_per < t2.fisc_yr_per)
        )
),
t2 as (
    SELECT edw_inventory_fact.co_cd,
        edw_inventory_fact.matl_no,
        edw_inventory_fact.fisc_yr_per,
        0 AS tot_stck
    FROM edw_inventory_fact edw_inventory_fact
    GROUP BY edw_inventory_fact.co_cd,
        edw_inventory_fact.matl_no,
        edw_inventory_fact.fisc_yr_per
),
cte1 as (
    SELECT t1.co_cd,
        t1.matl_no,
        t1.fisc_per AS fisc_yr_per,
        t2.tot_stck
    FROM t1,
        t2
    WHERE (
            ((t1.matl_no)::text = (t2.matl_no)::text)
            AND ((t1.co_cd)::text = (t2.co_cd)::text)
        )
        AND (t1.p_fisc_yr_per = t2.fisc_yr_per)
),
cte2 as (
    SELECT edw_inventory_fact.co_cd,
        edw_inventory_fact.matl_no,
        edw_inventory_fact.fisc_yr_per,
        sum(
            (
                edw_inventory_fact.rcpt_tot_stck - edw_inventory_fact.iss_tot_stck
            )
        ) AS tot_stck
    FROM edw_inventory_fact edw_inventory_fact
    GROUP BY edw_inventory_fact.co_cd,
        edw_inventory_fact.matl_no,
        edw_inventory_fact.fisc_yr_per
),
final as (
    select * from cte1
    union all
    select * from cte2
)
select * from final