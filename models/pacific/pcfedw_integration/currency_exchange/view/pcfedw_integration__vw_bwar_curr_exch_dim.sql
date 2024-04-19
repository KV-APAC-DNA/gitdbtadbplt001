with vw_curr_exch_dim as
(
    select * from {{ ref('pcfedw_integration__vw_curr_exch_dim') }}
),
edw_time_dim as
(
    select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
union_1 as
(
    SELECT 
        bwar_curr_exchng_nzd_usd.rate_type,
        bwar_curr_exchng_nzd_usd.from_ccy,
        bwar_curr_exchng_nzd_usd.to_ccy,
        bwar_curr_exchng_nzd_usd.jj_mnth_id,
        CASE
            WHEN (
                bwar_curr_exchng_nzd_usd.jj_mnth_id = dt_control_curr.first_day_cur_yr
            ) THEN exch_control_prev.exch_rate
            ELSE bwar_curr_exchng_nzd_usd.exch_rate
        END AS exch_rate
    FROM 
        (
            SELECT DISTINCT COALESCE(a.rate_type, 'BWAR'::character varying) AS rate_type,
                COALESCE(a.from_ccy, 'NZD'::character varying) AS from_ccy,
                COALESCE(a.to_ccy, 'USD'::character varying) AS to_ccy,
                b.jj_mnth_id,
                COALESCE(a.exch_rate, c.exch_rate) AS exch_rate
            FROM 
                (
                    SELECT "max"(vw_curr_exch_dim.exch_rate) AS exch_rate
                    FROM vw_curr_exch_dim
                    WHERE (
                            (
                                (
                                    (
                                        (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                    )
                                    AND (
                                        (vw_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                                    )
                                )
                                AND (
                                    (vw_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                                )
                            )
                            AND (
                                vw_curr_exch_dim.valid_date = ((20181231)::numeric)::numeric(18, 0)
                            )
                        )
                ) c,
                (
                    (
                        SELECT DISTINCT edw_time_dim.jj_mnth_id
                        FROM edw_time_dim
                    ) b
                    LEFT JOIN 
                    (
                        SELECT t1.rate_type,
                            t1.from_ccy,
                            t1.to_ccy,
                            t2.jj_mnth_id,
                            "max"(t1.exch_rate) AS exch_rate
                        FROM (
                                SELECT vw_curr_exch_dim.rate_type,
                                    vw_curr_exch_dim.from_ccy,
                                    vw_curr_exch_dim.to_ccy,
                                    vw_curr_exch_dim.valid_date,
                                    vw_curr_exch_dim.exch_rate
                                FROM vw_curr_exch_dim
                                WHERE (
                                        (
                                            (
                                                (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                            )
                                            AND (
                                                (vw_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                                            )
                                        )
                                        AND (
                                            (vw_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                                        )
                                    )
                            ) t1,
                            edw_time_dim t2
                        WHERE (t2.time_id = t1.valid_date)
                        GROUP BY t1.rate_type,
                            t1.from_ccy,
                            t1.to_ccy,
                            t2.jj_mnth_id
                    ) a ON ((a.jj_mnth_id = b.jj_mnth_id))
                )
        ) bwar_curr_exchng_nzd_usd,
        (
            SELECT min(edw_time_dim.jj_mnth_id) AS first_day_cur_yr
            FROM edw_time_dim
            WHERE (
                    edw_time_dim.time_id IN (
                        SELECT "max"(vw_curr_exch_dim.valid_date) AS "max"
                        FROM vw_curr_exch_dim vw_curr_exch_dim
                        WHERE (
                                (
                                    (
                                        (
                                            (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                        )
                                        AND (
                                            (vw_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                                        )
                                    )
                                    AND (
                                        (vw_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                                    )
                                )
                                AND (
                                    "substring"(
                                        ((vw_curr_exch_dim.valid_date)::character varying)::text,
                                        5,
                                        2
                                    ) = ('01'::character varying)::text
                                )
                            )
                    )
                )
        ) dt_control_curr,
        (
            SELECT "max"(vw_curr_exch_dim.exch_rate) AS exch_rate
            FROM vw_curr_exch_dim
            WHERE (
                    (
                        (
                            (
                                (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                            )
                            AND (
                                (vw_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                            )
                        )
                        AND (
                            (vw_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                        )
                    )
                    AND (
                        vw_curr_exch_dim.valid_date = ((20181231)::numeric)::numeric(18, 0)
                    )
                )
        ) exch_control_prev
    UNION ALL
    SELECT bwar_curr_exchng_aud_usd.rate_type,
        bwar_curr_exchng_aud_usd.from_ccy,
        bwar_curr_exchng_aud_usd.to_ccy,
        bwar_curr_exchng_aud_usd.jj_mnth_id,
        bwar_curr_exchng_aud_usd.exch_rate
    FROM (
            SELECT DISTINCT COALESCE(a.rate_type, 'BWAR'::character varying) AS rate_type,
                COALESCE(a.from_ccy, 'AUD'::character varying) AS from_ccy,
                COALESCE(a.to_ccy, 'USD'::character varying) AS to_ccy,
                b.jj_mnth_id,
                c.exch_rate
            FROM (
                    SELECT "max"(vw_curr_exch_dim.exch_rate) AS exch_rate
                    FROM vw_curr_exch_dim vw_curr_exch_dim
                    WHERE (
                            (
                                (
                                    (
                                        (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                    )
                                    AND (
                                        (vw_curr_exch_dim.from_ccy)::text = ('AUD'::character varying)::text
                                    )
                                )
                                AND (
                                    (vw_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                                )
                            )
                            AND (
                                vw_curr_exch_dim.valid_date = ((20181231)::numeric)::numeric(18, 0)
                            )
                        )
                ) c,
                (
                    (
                        SELECT DISTINCT edw_time_dim.jj_mnth_id
                        FROM edw_time_dim
                    ) b
                    LEFT JOIN (
                        SELECT t1.rate_type,
                            t1.from_ccy,
                            t1.to_ccy,
                            t2.jj_mnth_id,
                            "max"(t1.exch_rate) AS exch_rate
                        FROM (
                                SELECT vw_curr_exch_dim.rate_type,
                                    vw_curr_exch_dim.from_ccy,
                                    vw_curr_exch_dim.to_ccy,
                                    vw_curr_exch_dim.valid_date,
                                    vw_curr_exch_dim.exch_rate
                                FROM vw_curr_exch_dim vw_curr_exch_dim
                                WHERE (
                                        (
                                            (
                                                (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                            )
                                            AND (
                                                (vw_curr_exch_dim.from_ccy)::text = ('AUD'::character varying)::text
                                            )
                                        )
                                        AND (
                                            (vw_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                                        )
                                    )
                            ) t1,
                            edw_time_dim t2
                        WHERE (t2.time_id = t1.valid_date)
                        GROUP BY t1.rate_type,
                            t1.from_ccy,
                            t1.to_ccy,
                            t2.jj_mnth_id
                    ) a ON ((a.jj_mnth_id = b.jj_mnth_id))
                )
        ) bwar_curr_exchng_aud_usd
),
union_2 as
(
    SELECT bwar_curr_exchng_nzd_nzd.rate_type,
    bwar_curr_exchng_nzd_nzd.from_ccy,
    bwar_curr_exchng_nzd_nzd.to_ccy,
    bwar_curr_exchng_nzd_nzd.jj_mnth_id,
    bwar_curr_exchng_nzd_nzd.exch_rate
    FROM (
        SELECT bwar_curr_exchng_nzd_usd.rate_type,
            bwar_curr_exchng_nzd_usd.from_ccy,
            bwar_curr_exchng_nzd_usd.from_ccy AS to_ccy,
            bwar_curr_exchng_nzd_usd.jj_mnth_id,
            1 AS exch_rate
        FROM (
                SELECT DISTINCT COALESCE(a.rate_type, 'BWAR'::character varying) AS rate_type,
                    COALESCE(a.from_ccy, 'NZD'::character varying) AS from_ccy,
                    COALESCE(a.to_ccy, 'USD'::character varying) AS to_ccy,
                    b.jj_mnth_id,
                    COALESCE(a.exch_rate, c.exch_rate) AS exch_rate
                FROM (
                        SELECT "max"(vw_curr_exch_dim.exch_rate) AS exch_rate
                        FROM vw_curr_exch_dim
                        WHERE (
                                (
                                    (
                                        (
                                            (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                        )
                                        AND (
                                            (vw_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                                        )
                                    )
                                    AND (
                                        (vw_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                                    )
                                )
                                AND (
                                    vw_curr_exch_dim.valid_date = ((20181231)::numeric)::numeric(18, 0)
                                )
                            )
                    ) c,
                    (
                        (
                            SELECT DISTINCT edw_time_dim.jj_mnth_id
                            FROM edw_time_dim
                        ) b
                        LEFT JOIN (
                            SELECT t1.rate_type,
                                t1.from_ccy,
                                t1.to_ccy,
                                t2.jj_mnth_id,
                                "max"(t1.exch_rate) AS exch_rate
                            FROM (
                                    SELECT vw_curr_exch_dim.rate_type,
                                        vw_curr_exch_dim.from_ccy,
                                        vw_curr_exch_dim.to_ccy,
                                        vw_curr_exch_dim.valid_date,
                                        vw_curr_exch_dim.exch_rate
                                    FROM vw_curr_exch_dim
                                    WHERE (
                                            (
                                                (
                                                    (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                                )
                                                AND (
                                                    (vw_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                                                )
                                            )
                                            AND (
                                                (vw_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                                            )
                                        )
                                ) t1,
                                edw_time_dim t2
                            WHERE (t2.time_id = t1.valid_date)
                            GROUP BY t1.rate_type,
                                t1.from_ccy,
                                t1.to_ccy,
                                t2.jj_mnth_id
                        ) a ON ((a.jj_mnth_id = b.jj_mnth_id))
                    )
            ) bwar_curr_exchng_nzd_usd
    ) bwar_curr_exchng_nzd_nzd
),
final as
(
    select * from union_1
    union all
    select * from union_2
)
select * from final