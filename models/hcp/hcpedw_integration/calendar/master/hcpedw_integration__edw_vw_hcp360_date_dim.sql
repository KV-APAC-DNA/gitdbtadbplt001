{{
    config(
        materialized='view'
    )
}}

with edw_calendar_dim as
(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
final as
(
    SELECT 
    ecd.fisc_yr AS "year", 
    CASE WHEN (ecd.pstng_per = 1) THEN 1 WHEN (ecd.pstng_per = 2) THEN 1 WHEN (ecd.pstng_per = 3) THEN 1 WHEN (ecd.pstng_per = 4) THEN 2 WHEN (ecd.pstng_per = 5) THEN 2 WHEN (ecd.pstng_per = 6) THEN 2 WHEN (ecd.pstng_per = 7) THEN 3 WHEN (ecd.pstng_per = 8) THEN 3 WHEN (ecd.pstng_per = 9) THEN 3 WHEN (ecd.pstng_per = 10) THEN 4 WHEN (ecd.pstng_per = 11) THEN 4 WHEN (ecd.pstng_per = 12) THEN 4 ELSE NULL :: integer END AS qrtr_no, 
    (
        (
        (
            (ecd.fisc_yr):: character varying
        ):: text || ('/' :: character varying):: text
        ) || (
        CASE WHEN (ecd.pstng_per = 1) THEN 'Q1' :: character varying WHEN (ecd.pstng_per = 2) THEN 'Q1' :: character varying WHEN (ecd.pstng_per = 3) THEN 'Q1' :: character varying WHEN (ecd.pstng_per = 4) THEN 'Q2' :: character varying WHEN (ecd.pstng_per = 5) THEN 'Q2' :: character varying WHEN (ecd.pstng_per = 6) THEN 'Q2' :: character varying WHEN (ecd.pstng_per = 7) THEN 'Q3' :: character varying WHEN (ecd.pstng_per = 8) THEN 'Q3' :: character varying WHEN (ecd.pstng_per = 9) THEN 'Q3' :: character varying WHEN (ecd.pstng_per = 10) THEN 'Q4' :: character varying WHEN (ecd.pstng_per = 11) THEN 'Q4' :: character varying WHEN (ecd.pstng_per = 12) THEN 'Q4' :: character varying ELSE NULL :: character varying END
        ):: text
    ) AS qrtr, 
    (
        (
        (ecd.fisc_yr):: character varying
        ):: text || trim(
        to_char(
            ecd.pstng_per, 
            ('00' :: character varying):: text
        )
        )
    ) AS mnth_id, 
    (
        (
        (
            (ecd.fisc_yr):: character varying
        ):: text || ('/' :: character varying):: text
        ) || (
        CASE WHEN (ecd.pstng_per = 1) THEN 'JAN' :: character varying WHEN (ecd.pstng_per = 2) THEN 'FEB' :: character varying WHEN (ecd.pstng_per = 3) THEN 'MAR' :: character varying WHEN (ecd.pstng_per = 4) THEN 'APR' :: character varying WHEN (ecd.pstng_per = 5) THEN 'MAY' :: character varying WHEN (ecd.pstng_per = 6) THEN 'JUN' :: character varying WHEN (ecd.pstng_per = 7) THEN 'JUL' :: character varying WHEN (ecd.pstng_per = 8) THEN 'AUG' :: character varying WHEN (ecd.pstng_per = 9) THEN 'SEP' :: character varying WHEN (ecd.pstng_per = 10) THEN 'OCT' :: character varying WHEN (ecd.pstng_per = 11) THEN 'NOV' :: character varying WHEN (ecd.pstng_per = 12) THEN 'DEC' :: character varying ELSE NULL :: character varying END
        ):: text
    ) AS mnth_desc, 
    ecd.pstng_per AS mnth_no, 
    CASE WHEN (ecd.pstng_per = 1) THEN 'JAN' :: character varying WHEN (ecd.pstng_per = 2) THEN 'FEB' :: character varying WHEN (ecd.pstng_per = 3) THEN 'MAR' :: character varying WHEN (ecd.pstng_per = 4) THEN 'APR' :: character varying WHEN (ecd.pstng_per = 5) THEN 'MAY' :: character varying WHEN (ecd.pstng_per = 6) THEN 'JUN' :: character varying WHEN (ecd.pstng_per = 7) THEN 'JUL' :: character varying WHEN (ecd.pstng_per = 8) THEN 'AUG' :: character varying WHEN (ecd.pstng_per = 9) THEN 'SEP' :: character varying WHEN (ecd.pstng_per = 10) THEN 'OCT' :: character varying WHEN (ecd.pstng_per = 11) THEN 'NOV' :: character varying WHEN (ecd.pstng_per = 12) THEN 'DEC' :: character varying ELSE NULL :: character varying END AS mnth_shrt, 
    CASE WHEN (ecd.pstng_per = 1) THEN 'JANUARY' :: character varying WHEN (ecd.pstng_per = 2) THEN 'FEBRUARY' :: character varying WHEN (ecd.pstng_per = 3) THEN 'MARCH' :: character varying WHEN (ecd.pstng_per = 4) THEN 'APRIL' :: character varying WHEN (ecd.pstng_per = 5) THEN 'MAY' :: character varying WHEN (ecd.pstng_per = 6) THEN 'JUNE' :: character varying WHEN (ecd.pstng_per = 7) THEN 'JULY' :: character varying WHEN (ecd.pstng_per = 8) THEN 'AUGUST' :: character varying WHEN (ecd.pstng_per = 9) THEN 'SEPTEMBER' :: character varying WHEN (ecd.pstng_per = 10) THEN 'OCTOBER' :: character varying WHEN (ecd.pstng_per = 11) THEN 'NOVEMBER' :: character varying WHEN (ecd.pstng_per = 12) THEN 'DECEMBER' :: character varying ELSE NULL :: character varying END AS mnth_long, 
    cyrwkno.yr_wk_num AS wk, 
    cmwkno.mnth_wk_num AS mnth_wk_no, 
    ecd.cal_yr AS cal_year, 
    ecd.cal_qtr_1 AS cal_qrtr_no, 
    ecd.cal_mo_1 AS cal_mnth_id, 
    ecd.cal_mo_2 AS cal_mnth_no, 
    CASE WHEN (ecd.cal_mo_2 = 1) THEN 'JANUARY' :: character varying WHEN (ecd.cal_mo_2 = 2) THEN 'FEBRUARY' :: character varying WHEN (ecd.cal_mo_2 = 3) THEN 'MARCH' :: character varying WHEN (ecd.cal_mo_2 = 4) THEN 'APRIL' :: character varying WHEN (ecd.cal_mo_2 = 5) THEN 'MAY' :: character varying WHEN (ecd.cal_mo_2 = 6) THEN 'JUNE' :: character varying WHEN (ecd.cal_mo_2 = 7) THEN 'JULY' :: character varying WHEN (ecd.cal_mo_2 = 8) THEN 'AUGUST' :: character varying WHEN (ecd.cal_mo_2 = 9) THEN 'SEPTEMBER' :: character varying WHEN (ecd.cal_mo_2 = 10) THEN 'OCTOBER' :: character varying WHEN (ecd.cal_mo_2 = 11) THEN 'NOVEMBER' :: character varying WHEN (ecd.cal_mo_2 = 12) THEN 'DECEMBER' :: character varying ELSE NULL :: character varying END AS cal_mnth_nm, 
    to_date(
        (ecd.cal_day):: date
    ) AS cal_date, 
    "replace"(
        (
        (ecd.cal_day):: character varying
        ):: text, 
        ('-' :: character varying):: text, 
        ('' :: character varying):: text
    ) AS cal_date_id 
    FROM 
    edw_calendar_dim ecd, 
    (
        SELECT 
        row_number() OVER(
            PARTITION BY a.fisc_per 
            ORDER BY 
            a.cal_wk
        ) AS mnth_wk_num, 
        to_date(
            DATEADD(
            DAY, 
            (
                - (6):: bigint
            ), 
            (a.cal_day):: date
            )
        ) AS cal_day_first, 
        a.cal_day AS cal_day_last 
        FROM 
        edw_calendar_dim a 
        WHERE 
        (
            a.cal_day IN (
            SELECT 
                edw_calendar_dim.cal_day 
            FROM 
                edw_calendar_dim 
            WHERE 
                (edw_calendar_dim.wkday = 7)
            )
        ) 
        ORDER BY 
        a.cal_wk
    ) cmwkno, 
    (
        SELECT 
        row_number() OVER(
            PARTITION BY a.fisc_yr 
            ORDER BY 
            a.cal_wk
        ) AS yr_wk_num, 
        to_date(
            DATEADD(
            DAY, 
            (
                - (6):: bigint
            ), 
            (a.cal_day):: date
            )
        ) AS cal_day_first, 
        a.cal_day AS cal_day_last 
        FROM 
        edw_calendar_dim a 
        WHERE 
        (
            a.cal_day IN (
            SELECT 
                edw_calendar_dim.cal_day 
            FROM 
                edw_calendar_dim 
            WHERE 
                (edw_calendar_dim.wkday = 7)
            )
        ) 
        ORDER BY 
        a.cal_wk
    ) cyrwkno 
    WHERE 
    (
        (
        (
            (
            ecd.cal_day >= cmwkno.cal_day_first
            ) 
            AND (
            ecd.cal_day <= cmwkno.cal_day_last
            )
        ) 
        AND (
            ecd.cal_day >= cyrwkno.cal_day_first
        )
        ) 
        AND (
        ecd.cal_day <= cyrwkno.cal_day_last
        )
    )
)
select * from final