with edw_crncy_exch as (
    select * from {{ ref('aspedw_integration__edw_crncy_exch') }}
),
edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),

final as (
    SELECT valid_from,
    fisc_yr_per,
    fisc_yr_per_rep,
    fisc_yr_per_fday,
    fisc_yr_per_lday,
    ex_rt_typ,
    from_crncy,
    to_crncy,
    act_valid_from,
    ex_rt,
    from_ratio,
    to_ratio FROM (
        SELECT valid_from,
            fisc_yr_per,
            CAST(fisc_yr_per_rep AS DATE) as fisc_yr_per_rep,
            fisc_yr_per_fday,
            fisc_yr_per_lday,
            ex_rt_typ,
            from_crncy,
            to_crncy,
            CAST(act_valid_from AS DATE) as act_valid_from,
            ex_rt,
            from_ratio,
            to_ratio,
            row_number() OVER (
                PARTITION BY valid_from,
                fisc_yr_per,
                fisc_yr_per_rep,
                fisc_yr_per_fday,
                fisc_yr_per_lday,
                ex_rt_typ,
                from_crncy,
                to_crncy,
                from_ratio,
                to_ratio
                ORDER BY act_valid_from DESC
            ) AS rn
        FROM (
                SELECT MAX(B.date) AS valid_from,
                    B.fiscal_period AS fisc_yr_per,
                    MAX(B.fisc_period_date) AS fisc_yr_per_rep,
                    MAX(B.first_date) AS fisc_yr_per_fday,
                    MAX(B.last_date) AS fisc_yr_per_lday,
                    A.ex_rt_typ,
                    A.from_crncy,
                    A.to_crncy,
                    A.vld_from_date AS act_valid_from,
                    MAX(A.ex_rt) AS ex_rt,
                    MAX(A.from_ratio) AS from_ratio,
                    MAX(A.to_ratio) AS to_ratio
                FROM (
                        SELECT exrt.ex_rt_typ,
                            exrt.from_crncy,
                            exrt.to_crncy,
                            (99999999 - exrt.vld_from) AS valid_from,
                            (
                                SUBSTRING((99999999 - exrt.vld_from),1,4) || '-' || SUBSTRING((99999999 - exrt.vld_from),5,2) || '-' || SUBSTRING((99999999 - exrt.vld_from),7,2)
                            ) AS vld_from_date,
                            exrt.ex_rt,
                            exrt.from_ratio,
                            exrt.to_ratio
                        FROM edw_crncy_exch exrt
                            INNER JOIN edw_calendar_dim cal 
                            ON cal.cal_day = (SUBSTRING ((99999999 - exrt.vld_from),1,4) || '-' || SUBSTRING ((99999999 - exrt.vld_from),5,2) || '-' || SUBSTRING ((99999999 - exrt.vld_from),7,2))
                        WHERE exrt.ex_rt_typ = 'BWAR'
                            AND cal.fisc_yr >= DATE_PART(YEAR, CURRENT_DATE) -2
                ORDER BY from_crncy,
                    to_crncy,
                    valid_from DESC
) A,
(
    SELECT a.cal_day AS DATE,
        a.fisc_per AS fiscal_period,
        (SUBSTRING((a.fisc_per),1,4) || '-' || SUBSTRING((a.fisc_per),6,2) || '-' || '01') AS fisc_period_date,
        b.cal_day AS First_Date,
        c.cal_day AS Last_date
    FROM edw_calendar_dim a
        LEFT OUTER JOIN (
            SELECT MIN(cal_day) AS cal_day,
                fisc_per
            FROM edw_calendar_dim
            GROUP BY fisc_per
        ) b ON a.fisc_per = b.fisc_per
        LEFT OUTER JOIN (
            SELECT MAX(cal_day) AS cal_day,
                fisc_per
            FROM edw_calendar_dim
            GROUP BY fisc_per
        ) c ON a.fisc_per = c.fisc_per
    ORDER BY a.cal_day DESC
) B
WHERE (DATEDIFF(DAY, CAST(A.vld_from_date AS DATE), B.date) >= 0 AND DATEDIFF(DAY, B.date, B.date) <= 0)
GROUP BY B.fiscal_period,
    A.ex_rt_typ,
    A.from_crncy,
    A.to_crncy,
    A.vld_from_date
ORDER BY A.ex_rt_typ,
    A.from_crncy,
    A.to_crncy DESC
)
)
WHERE rn = 1
)

select * from final