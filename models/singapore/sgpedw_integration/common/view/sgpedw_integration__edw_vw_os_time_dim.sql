with edw_calendar_dim as(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
cmwkno as
(
    select
        row_number() over (partition by a.fisc_per order by a.cal_wk) as mnth_wk_num,
        dateadd(day,(-cast((6) as bigint)),cast((a.cal_day) as timestampntz) ) as cal_day_first,
        a.cal_day as cal_day_last
    from edw_calendar_dim as a
    where
    (
      a.cal_day in (
        select
          edw_calendar_dim.cal_day
        from edw_calendar_dim
        where
          (
            edw_calendar_dim.wkday = 7
          )
      )
    )
    order by
    a.cal_wk
),
cyrwkno as
(
    select
        row_number() over (partition by a.fisc_yr order by a.cal_wk) as yr_wk_num,
        dateadd(day,(-cast((6) as bigint)),
        cast((a.cal_day) as timestampntz)) as cal_day_first,
        a.cal_day as cal_day_last
    from edw_calendar_dim as a
    where
    (
      a.cal_day in 
      (
        select
            edw_calendar_dim.cal_day
        from edw_calendar_dim
        where
          (
            edw_calendar_dim.wkday = 7
          )
      )
    )
    order by
    a.cal_wk
),
transformed as 
(   
    select
        ecd.fisc_yr as "year",
        case 
            when (ecd.pstng_per = 1) then 1
            when (ecd.pstng_per = 2) then 1
            when (ecd.pstng_per = 3) then 1
            when (ecd.pstng_per = 4) then 2
            when (ecd.pstng_per = 5) then 2
            when (ecd.pstng_per = 6) then 2
            when (ecd.pstng_per = 7) then 3
            when (ecd.pstng_per = 8) then 3
            when (ecd.pstng_per = 9) then 3
            when (ecd.pstng_per = 10) then 4
            when (ecd.pstng_per = 11) then 4
            when (ecd.pstng_per = 12) then 4
            else cast(null as int)
        end as qrtr_no,
        (
            (
                cast((cast((ecd.fisc_yr) as varchar)) as text) 
                || cast((cast('/' as varchar)) as text)) 
                || cast(
                    (
                    case
                        when (ecd.pstng_per = 1) then cast('Q1' as varchar)
                        when (ecd.pstng_per = 2) then cast('Q1' as varchar)
                        when (ecd.pstng_per = 3) then cast('Q1' as varchar)
                        when (ecd.pstng_per = 4) then cast('Q2' as varchar)
                        when (ecd.pstng_per = 5) then cast('Q2' as varchar)
                        when (ecd.pstng_per = 6) then cast('Q2' as varchar)
                        when (ecd.pstng_per = 7) then cast('Q3' as varchar)
                        when (ecd.pstng_per = 8) then cast('Q3' as varchar)
                        when (ecd.pstng_per = 9) then cast('Q3' as varchar)
                        when (ecd.pstng_per = 10) then cast('Q4' as varchar)
                        when (ecd.pstng_per = 11) then cast('Q4' as varchar)
                        when (ecd.pstng_per = 12) then cast('Q4' as varchar)
                        else cast(null as varchar)
                    end
                ) as text
            )
        ) as qrtr,
        (
            cast((cast((ecd.fisc_yr) as varchar)) as text) 
            || trim(to_char(ecd.pstng_per, cast((cast('00' as varchar)) as text)))
        ) as mnth_id,
        (
            (
                cast((cast((ecd.fisc_yr) as varchar)) as text) || cast((cast('/' as varchar)) as text)
            ) 
            || cast
            (
                (
                    case
                        when (ecd.pstng_per = 1) then cast('JAN' as varchar)
                        when (ecd.pstng_per = 2) then cast('FEB' as varchar)
                        when (ecd.pstng_per = 3) then cast('MAR' as varchar)
                        when (ecd.pstng_per = 4) then cast('APR' as varchar)
                        when (ecd.pstng_per = 5) then cast('MAY' as varchar)
                        when (ecd.pstng_per = 6) then cast('JUN' as varchar)
                        when (ecd.pstng_per = 7) then cast('JUL' as varchar)
                        when (ecd.pstng_per = 8) then cast('AUG' as varchar)
                        when (ecd.pstng_per = 9) then cast('SEP' as varchar)
                        when (ecd.pstng_per = 10) then cast('OCT' as varchar)
                        when (ecd.pstng_per = 11) then cast('NOV' as varchar)
                        when (ecd.pstng_per = 12) then cast('DEC' as varchar)
                        else cast(null as varchar)
                    end
                ) 
            as text
            )
        ) as mnth_desc,
        ecd.pstng_per as mnth_no,
        case
            when (ecd.pstng_per = 1) then cast('JAN' as varchar)
            when (ecd.pstng_per = 2) then cast('FEB' as varchar)
            when (ecd.pstng_per = 3) then cast('MAR' as varchar)
            when (ecd.pstng_per = 4) then cast('APR' as varchar)
            when (ecd.pstng_per = 5) then cast('MAY' as varchar)
            when (ecd.pstng_per = 6) then cast('JUN' as varchar)
            when (ecd.pstng_per = 7) then cast('JUL' as varchar)
            when (ecd.pstng_per = 8) then cast('AUG' as varchar)
            when (ecd.pstng_per = 9) then cast('SEP' as varchar)
            when (ecd.pstng_per = 10) then cast('OCT' as varchar)
            when (ecd.pstng_per = 11) then cast('NOV' as varchar)
            when (ecd.pstng_per = 12) then cast('DEC' as varchar)
            else cast(null as varchar)
        end as mnth_shrt,
        case
            when (ecd.pstng_per = 1) then cast('JANUARY' as varchar)
            when (ecd.pstng_per = 2) then cast('FEBRUARY' as varchar)
            when (ecd.pstng_per = 3) then cast('MARCH' as varchar)
            when (ecd.pstng_per = 4) then cast('APRIL' as varchar)
            when (ecd.pstng_per = 5) then cast('MAY' as varchar)
            when (ecd.pstng_per = 6) then cast('JUNE' as varchar)
            when (ecd.pstng_per = 7) then cast('JULY' as varchar)
            when (ecd.pstng_per = 8) then cast('AUGUST' as varchar)
            when (ecd.pstng_per = 9) then cast('SEPTEMBER' as varchar)
            when (ecd.pstng_per = 10) then cast('OCTOBER' as varchar)
            when (ecd.pstng_per = 11) then cast('NOVEMBER' as varchar)
            when (ecd.pstng_per = 12) then cast('DECEMBER' as varchar)
            else cast(null as varchar)
        end as mnth_long,
        cyrwkno.yr_wk_num as wk,
        cmwkno.mnth_wk_num as mnth_wk_no,
        ecd.cal_yr as cal_year,
        ecd.cal_qtr_1 as cal_qrtr_no,
        ecd.cal_mo_1 as cal_mnth_id,
        ecd.cal_mo_2 as cal_mnth_no,
        case
            when (ecd.cal_mo_2 = 1) then cast('JANUARY' as varchar)
            when (ecd.cal_mo_2 = 2) then cast('FEBRUARY' as varchar)
            when (ecd.cal_mo_2 = 3) then cast('MARCH' as varchar)
            when (ecd.cal_mo_2 = 4) then cast('APRIL' as varchar)
            when (ecd.cal_mo_2 = 5) then cast('MAY' as varchar)
            when (ecd.cal_mo_2 = 6) then cast('JUNE' as varchar)
            when (ecd.cal_mo_2 = 7) then cast('JULY' as varchar)
            when (ecd.cal_mo_2 = 8) then cast('AUGUST' as varchar)
            when (ecd.cal_mo_2 = 9) then cast('SEPTEMBER' as varchar)
            when (ecd.cal_mo_2 = 10) then cast('OCTOBER' as varchar)
            when (ecd.cal_mo_2 = 11) then cast('NOVEMBER' as varchar)
            when (ecd.cal_mo_2 = 12) then cast('DECEMBER' as varchar)
            else cast(null as varchar)
        end as cal_mnth_nm,
        cast((ecd.cal_day) as date) as cal_date,
        replace
        (
            cast((cast((ecd.cal_day) as varchar)) as text),
            cast((cast('-' as varchar)) as text),
            cast((cast('' as varchar)) as text)
        ) as cal_date_id
    from edw_calendar_dim as ecd, cmwkno,cyrwkno
    where
    (
        (
            (
                (ecd.cal_day >= cmwkno.cal_day_first)
                and (ecd.cal_day <= cmwkno.cal_day_last)
            )
            and (ecd.cal_day >= cyrwkno.cal_day_first)
        )
    and (ecd.cal_day <= cyrwkno.cal_day_last)
    )
),

final as
(
    select
        "year",
        qrtr_no,
        qrtr,
        mnth_id,
        mnth_desc,
        mnth_no,
        mnth_shrt,
        mnth_long,
        wk,
        mnth_wk_no,
        cal_year,
        cal_qrtr_no,
        cal_mnth_id,
        cal_mnth_no,
        cal_mnth_nm,
        cal_date,
        cal_date_id
        from transformed
)
select * from final