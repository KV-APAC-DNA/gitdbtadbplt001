with
vw_curr_exch_dim as
(
    select * from {{ ref('pcfedw_integration__vw_curr_exch_dim') }}
),
edw_time_dim as
(
    select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
c as
(
SELECT "max"(vw_curr_exch_dim.exch_rate) AS exch_rate
FROM vw_curr_exch_dim
    WHERE (
    (((
    (vw_curr_exch_dim.rate_type)::text = 'JJBR'::text)
    AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text))
    AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text))
    AND (vw_curr_exch_dim.valid_date = 
        (SELECT "max"(vw_curr_exch_dim.valid_date) AS "max"
        FROM vw_curr_exch_dim
        WHERE ((((vw_curr_exch_dim.rate_type)::text = 'JJBR'::text)
        AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text))
        AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text)))))
),
t1 as
 (
SELECT vw_curr_exch_dim.rate_type,
    vw_curr_exch_dim.from_ccy,
    vw_curr_exch_dim.to_ccy,
    vw_curr_exch_dim.valid_date,
    vw_curr_exch_dim.exch_rate
FROM vw_curr_exch_dim
    WHERE ((((vw_curr_exch_dim.rate_type)::text = 'JJBR'::text)
    AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text))
    AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text))
),
a as 
(
SELECT 
    t1.rate_type,
    t1.from_ccy,
    t1.to_ccy,
    t2.jj_mnth_id,
    "max"(t1.exch_rate) AS exch_rate
FROM t1,
    edw_time_dim t2
WHERE (t2.time_id = t1.valid_date)
GROUP BY 
    t1.rate_type,
    t1.from_ccy,
    t1.to_ccy,
    t2.jj_mnth_id
),
jjbr_curr_exchng_nzd_aud as
(
SELECT DISTINCT 
    COALESCE(a.rate_type, 'JJBR'::character varying) AS rate_type,
    COALESCE(a.from_ccy, 'NZD'::character varying) AS from_ccy,
    COALESCE(a.to_ccy, 'AUD'::character varying) AS to_ccy,
    b.jj_mnth_id,
    COALESCE(a.exch_rate, c.exch_rate) AS exch_rate
FROM c,
    ((
    SELECT DISTINCT edw_time_dim.jj_mnth_id
    FROM edw_time_dim) b
    LEFT JOIN a ON (((a.jj_mnth_id = b.jj_mnth_id))))
),
temp_a as
(
    SELECT 
        jjbr_curr_exchng_nzd_aud.rate_type,
        jjbr_curr_exchng_nzd_aud.from_ccy,
        jjbr_curr_exchng_nzd_aud.to_ccy,
        jjbr_curr_exchng_nzd_aud.jj_mnth_id,
        jjbr_curr_exchng_nzd_aud.exch_rate
    FROM  jjbr_curr_exchng_nzd_aud
    UNION ALL
    SELECT 
        jjbr_curr_exchng_aud_aud.rate_type,
        jjbr_curr_exchng_aud_aud.from_ccy,
        jjbr_curr_exchng_aud_aud.to_ccy,
        jjbr_curr_exchng_aud_aud.jj_mnth_id,
        jjbr_curr_exchng_aud_aud.exch_rate
    FROM (
            SELECT jjbr_curr_exchng_nzd_aud.rate_type,
                jjbr_curr_exchng_nzd_aud.to_ccy AS from_ccy,
                jjbr_curr_exchng_nzd_aud.to_ccy,
                jjbr_curr_exchng_nzd_aud.jj_mnth_id,
                1 AS exch_rate
            FROM (
                    SELECT DISTINCT COALESCE(a.rate_type, 'JJBR'::character varying) AS rate_type,
                    COALESCE(a.from_ccy, 'NZD'::character varying) AS from_ccy,
                    COALESCE(a.to_ccy, 'AUD'::character varying) AS to_ccy,
                    b.jj_mnth_id,
                    COALESCE(a.exch_rate, c.exch_rate) AS exch_rate
                    FROM (
                    SELECT "max"(vw_curr_exch_dim.exch_rate) AS exch_rate
                    FROM vw_curr_exch_dim
                    WHERE (((((vw_curr_exch_dim.rate_type)::text = 'JJBR'::text)
                AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text))
                AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text))
                AND (vw_curr_exch_dim.valid_date = (SELECT "max"(vw_curr_exch_dim.valid_date) AS "max"
                FROM vw_curr_exch_dim
                WHERE ((((vw_curr_exch_dim.rate_type)::text = 'JJBR'::text)
                AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text))
                AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text)))))
                        ) c,
                ((SELECT DISTINCT edw_time_dim.jj_mnth_id FROM edw_time_dim
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
        WHERE ((((vw_curr_exch_dim.rate_type)::text = 'JJBR'::text)
        AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text))
        AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text))) t1,edw_time_dim t2
        WHERE (t2.time_id = t1.valid_date)
        GROUP BY 
            t1.rate_type,
            t1.from_ccy,
            t1.to_ccy,
            t2.jj_mnth_id
    ) a ON (((a.jj_mnth_id = b.jj_mnth_id))))) jjbr_curr_exchng_nzd_aud) jjbr_curr_exchng_aud_aud
),
jjbr_curr_exchng_nzd_nzd as
(
        SELECT jjbr_curr_exchng_nzd_aud.rate_type,
            jjbr_curr_exchng_nzd_aud.from_ccy,
            jjbr_curr_exchng_nzd_aud.from_ccy AS to_ccy,
            jjbr_curr_exchng_nzd_aud.jj_mnth_id,
            1 AS exch_rate
        FROM (
                SELECT DISTINCT COALESCE(a.rate_type, 'JJBR'::character varying) AS rate_type,
                    COALESCE(a.from_ccy, 'NZD'::character varying) AS from_ccy,
                    COALESCE(a.to_ccy, 'AUD'::character varying) AS to_ccy,
                    b.jj_mnth_id,
                    COALESCE(a.exch_rate, c.exch_rate) AS exch_rate
                FROM 
                (
                SELECT "max"(vw_curr_exch_dim.exch_rate) AS exch_rate
                FROM vw_curr_exch_dim
                WHERE (((((vw_curr_exch_dim.rate_type)::text = 'JJBR'::text)
                AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text))
                AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text))
                AND (vw_curr_exch_dim.valid_date = (SELECT "max"(vw_curr_exch_dim.valid_date) AS "max"
                FROM vw_curr_exch_dim
                WHERE ((((vw_curr_exch_dim.rate_type)::text = 'JJBR'::text)
                    AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text))
                    AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text)))))
                ) c,
                    ((
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
            SELECT 
                vw_curr_exch_dim.rate_type,
                vw_curr_exch_dim.from_ccy,
                vw_curr_exch_dim.to_ccy,
                vw_curr_exch_dim.valid_date,
                vw_curr_exch_dim.exch_rate
                FROM vw_curr_exch_dim
            WHERE ((((vw_curr_exch_dim.rate_type)::text = 'JJBR'::text)
            AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text))
            AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text))) t1,edw_time_dim t2
            WHERE (t2.time_id = t1.valid_date)
            GROUP BY t1.rate_type,
                t1.from_ccy,
                t1.to_ccy,
                t2.jj_mnth_id
        ) a ON (((a.jj_mnth_id = b.jj_mnth_id))))) jjbr_curr_exchng_nzd_aud
),
temp_b as
(
SELECT jjbr_curr_exchng_nzd_nzd.rate_type,
    jjbr_curr_exchng_nzd_nzd.from_ccy,
    jjbr_curr_exchng_nzd_nzd.to_ccy,
    jjbr_curr_exchng_nzd_nzd.jj_mnth_id,
    jjbr_curr_exchng_nzd_nzd.exch_rate
FROM  jjbr_curr_exchng_nzd_nzd
),
final as
(
    select * from temp_a 
    union all
    select * from temp_b
)
select * from final