with vw_curr_exch_dim as
(
    select * from {{ ref('pcfedw_integration__vw_curr_exch_dim') }}
),
edw_time_dim as
(
    select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
vw_jjbr_curr_exch_dim as
(
    select * from {{ ref('pcfedw_integration__vw_jjbr_curr_exch_dim') }}   
),
final as
(
    select 
        CASE
            WHEN ((b.from_ccy)::text = ('AUD'::character varying)::text) THEN '7470'::character varying
            WHEN ((b.from_ccy)::text = ('NZD'::character varying)::text) THEN '8361'::character varying
            ELSE '7471'::character varying
        END AS cmp_id,
        a.from_ccy,
        b.from_ccy AS to_ccy,
        b.jj_mnth_id,
        (a.exch_rate * (1/ b.exch_rate::double precision)) AS exch_rate
    from 
        (
            select 
                DISTINCT coalesce(a.from_ccy, 'SGD'::character varying) AS from_ccy,
                coalesce(a.to_ccy, 'AUD'::character varying) AS to_ccy,
                b.jj_year,
                coalesce(a.exch_rate, c.exch_rate) AS exch_rate
            from 
                (
                    select "max"(vw_curr_exch_dim.exch_rate) AS exch_rate
                    from vw_curr_exch_dim
                    WHERE 
                        (
                            (
                                (vw_curr_exch_dim.rate_type)::text = ('DWBP'::character varying)::text
                                AND (vw_curr_exch_dim.from_ccy)::text = ('SGD'::character varying)::text
                                AND (vw_curr_exch_dim.to_ccy)::text = ('AUD'::character varying)::text
                            )
                            AND 
                            (
                                vw_curr_exch_dim.valid_date = 
                                (
                                    select "max"(vw_curr_exch_dim.valid_date) AS "max" from vw_curr_exch_dim
                                    WHERE 
                                        (
                                            (vw_curr_exch_dim.rate_type)::text = ('DWBP'::character varying)::text
                                            AND (vw_curr_exch_dim.from_ccy)::text = ('SGD'::character varying)::text
                                            AND (vw_curr_exch_dim.to_ccy)::text = ('AUD'::character varying)::text
                                        )
                                )
                            )
                        )
                ) c,
                (
                    (
                        select DISTINCT edw_time_dim.jj_year from edw_time_dim
                    ) b
                    LEFT JOIN 
                    (
                        select t1.rate_type,
                            t1.from_ccy,
                            t1.to_ccy,
                            t2.jj_year,
                            "max"(t1.exch_rate) AS exch_rate
                        from 
                            (
                                select vw_curr_exch_dim.rate_type,
                                    vw_curr_exch_dim.from_ccy,
                                    vw_curr_exch_dim.to_ccy,
                                    vw_curr_exch_dim.valid_date,
                                    vw_curr_exch_dim.exch_rate
                                from vw_curr_exch_dim
                                WHERE 
                                    (
                                        (vw_curr_exch_dim.rate_type)::text = ('DWBP'::character varying)::text
                                        AND (vw_curr_exch_dim.from_ccy)::text = ('SGD'::character varying)::text
                                        AND (vw_curr_exch_dim.to_ccy)::text = ('AUD'::character varying)::text
                                    )
                            ) t1,
                            edw_time_dim t2
                        WHERE (t2.time_id = t1.valid_date)
                        GROUP BY t1.rate_type,
                            t1.from_ccy,
                            t1.to_ccy,
                            t2.jj_year
                    ) a ON ((a.jj_year = b.jj_year))
                )
        ) a,
        vw_jjbr_curr_exch_dim b,
        (
            select DISTINCT edw_time_dim.jj_year,edw_time_dim.jj_mnth_id from edw_time_dim
        ) c
    WHERE 
        (
            (b.jj_mnth_id = c.jj_mnth_id) 
            AND (c.jj_year = a.jj_year)
            AND ((b.to_ccy)::text = (a.to_ccy)::text)
        )
)
select * from final