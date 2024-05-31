with edw_calendar_dim as (
    select * from aspedw_integration.edw_calendar_dim
),
src1 as (
    SELECT edw_calendar_dim.cal_day,
        edw_calendar_dim.fisc_yr_vrnt,
        edw_calendar_dim.wkday,
        edw_calendar_dim.cal_wk,
        edw_calendar_dim.cal_mo_1,
        edw_calendar_dim.cal_mo_2,
        edw_calendar_dim.cal_qtr_1,
        edw_calendar_dim.cal_qtr_2,
        edw_calendar_dim.half_yr,
        edw_calendar_dim.cal_yr,
        edw_calendar_dim.fisc_per,
        edw_calendar_dim.pstng_per,
        edw_calendar_dim.fisc_yr,
        edw_calendar_dim.rec_mode,
        edw_calendar_dim.crt_dttm,
        edw_calendar_dim.updt_dttm
    FROM edw_calendar_dim
    ORDER BY edw_calendar_dim.cal_day
),
src2 as(
    SELECT a.cal_day,
        row_number() OVER(
            PARTITION BY a.cal_yr
            ORDER BY a.cal_day
        ) AS promo_week
    FROM edw_calendar_dim a
    WHERE (a.wkday = 5)
    ORDER BY a.cal_day
),
x as (
SELECT "max"(derived_table1.cal_day) AS cal_day,
                            derived_table1.mo,
                            derived_table1.cal_yr
                        FROM (
                                SELECT a.cal_day,
                                    "date_part"(month, a.cal_day) AS mo,
                                    a.cal_yr
                                FROM edw_calendar_dim a
                                WHERE (a.wkday = 4)
                                ORDER BY a.cal_day
                            ) derived_table1
                        GROUP BY derived_table1.mo,
                            derived_table1.cal_yr
                        ORDER BY derived_table1.mo,
                            derived_table1.cal_yr
),
y as (
SELECT a.cal_day, row_number() OVER(PARTITION BY a.cal_yr ORDER BY a.cal_day) AS promo_week
                    from edw_calendar_dim a
                    WHERE (a.wkday = 5)
),
src3 as (
    SELECT CASE
            WHEN (src2.promo_week IS NULL) THEN y.promo_week
            ELSE src2.promo_week
        END AS promo_week,
        src1.cal_day,
        src1.fisc_yr_vrnt,
        src1.wkday,
        src1.cal_wk,
        src1.cal_mo_1,
        src1.cal_mo_2,
        src1.cal_qtr_1,
        src1.cal_qtr_2,
        src1.half_yr,
        src1.cal_yr,
        src1.fisc_per,
        src1.pstng_per,
        src1.fisc_yr,
        src1.rec_mode,
        CASE WHEN (src1.cal_day <= x.cal_day) THEN x.mo
                    ELSE (x.mo + 1)
                END AS promo_month
    FROM src1
        LEFT JOIN src2 ON ((src1.cal_day = src2.cal_day))
        left join x on date_part(month, src1.cal_day) = x.mo AND (src1.cal_yr = x.cal_yr)
        left join y on datediff(day, (y.cal_day)::timestamp without time zone, (src1.cal_day)::timestamp without time zone) >= 1 AND datediff(day, (y.cal_day)::timestamp without time zone, (src1.cal_day)::timestamp without time zone) <= 6     
    ORDER BY src1.cal_day
),
final as (
    SELECT src3.cal_day,
        src3.fisc_yr_vrnt,
        src3.wkday,
        src3.cal_wk,
        src3.cal_mo_1,
        src3.cal_mo_2,
        src3.cal_qtr_1,
        src3.cal_qtr_2,
        src3.half_yr,
        src3.cal_yr,
        src3.fisc_per,
        src3.pstng_per,
        src3.fisc_yr,
        src3.rec_mode,
        COALESCE(src3.promo_week, (52)::bigint) AS promo_week,
        CASE
            WHEN (src3.promo_month = 1) THEN 'Jan'::character varying
            WHEN (src3.promo_month = 2) THEN 'Feb'::character varying
            WHEN (src3.promo_month = 3) THEN 'Mar'::character varying
            WHEN (src3.promo_month = 4) THEN 'Apr'::character varying
            WHEN (src3.promo_month = 5) THEN 'May'::character varying
            WHEN (src3.promo_month = 6) THEN 'Jun'::character varying
            WHEN (src3.promo_month = 7) THEN 'Jul'::character varying
            WHEN (src3.promo_month = 8) THEN 'Aug'::character varying
            WHEN (src3.promo_month = 9) THEN 'Sep'::character varying
            WHEN (src3.promo_month = 10) THEN 'Oct'::character varying
            WHEN (src3.promo_month = 11) THEN 'Nov'::character varying
            WHEN (src3.promo_month = 12) THEN 'Dec'::character varying
            WHEN (src3.promo_month = 13) THEN 'Jan'::character varying
            ELSE NULL::character varying
        END AS promo_month,
        (
            (
                (
                    CASE
                        WHEN (src3.promo_month = 13) THEN (src3.cal_yr + 1)
                        ELSE src3.cal_yr
                    END
                )::character varying(10)
            )::text || (
                CASE
                    WHEN (src3.promo_month = 1) THEN '001'::character varying
                    WHEN (src3.promo_month = 2) THEN '002'::character varying
                    WHEN (src3.promo_month = 3) THEN '003'::character varying
                    WHEN (src3.promo_month = 4) THEN '004'::character varying
                    WHEN (src3.promo_month = 5) THEN '005'::character varying
                    WHEN (src3.promo_month = 6) THEN '006'::character varying
                    WHEN (src3.promo_month = 7) THEN '007'::character varying
                    WHEN (src3.promo_month = 8) THEN '008'::character varying
                    WHEN (src3.promo_month = 9) THEN '009'::character varying
                    WHEN (src3.promo_month = 10) THEN '010'::character varying
                    WHEN (src3.promo_month = 11) THEN '011'::character varying
                    WHEN (src3.promo_month = 12) THEN '012'::character varying
                    WHEN (src3.promo_month = 13) THEN '001'::character varying
                    ELSE NULL::character varying
                END
            )::text
        ) AS promo_per,
        (
            ((src3.cal_yr)::character varying)::text || lpad(
                (
                    (
                        CASE
                            WHEN (
                                (
                                    row_number() OVER(
                                        PARTITION BY src3.cal_yr
                                        ORDER BY src3.cal_day
                                    ) % (7)::bigint
                                ) = 0
                            ) THEN trunc((
                                row_number() OVER(
                                    PARTITION BY src3.cal_yr
                                    ORDER BY src3.cal_day
                                ) / 7
                            ),0)
                            ELSE trunc((
                                (
                                    row_number() OVER(
                                        PARTITION BY src3.cal_yr
                                        ORDER BY src3.cal_day
                                    ) / 7
                                ) + 1
                            ),0)
                        END
                    )::character varying
                )::text,
                2,
                ('0'::character varying)::text
            )
        ) AS univ_week,
        CASE
            WHEN (
                (
                    row_number() OVER(
                        PARTITION BY substring(((src3.cal_day)::character varying)::text, 1, 7)
                        ORDER BY src3.cal_day
                    ) % (7)::bigint
                ) = 0
            ) THEN trunc((
                row_number() OVER(
                    PARTITION BY substring(((src3.cal_day)::character varying)::text, 1, 7)
                    ORDER BY src3.cal_day
                ) / 7
            ),0)
            ELSE trunc((
                (
                    row_number() OVER(
                        PARTITION BY substring(((src3.cal_day)::character varying)::text, 1, 7)
                        ORDER BY src3.cal_day
                    ) / 7
                ) + 1
            ),0)
        END AS univ_week_month
    from src3
)
select * from final