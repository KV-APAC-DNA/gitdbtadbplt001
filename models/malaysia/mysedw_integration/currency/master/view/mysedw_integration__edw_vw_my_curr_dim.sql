with edw_crncy_exch as(
    select * from {{ ref('aspedw_integration__edw_crncy_exch') }}
),
edw_vw_os_time_dim as(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
curr_exch as
(
    select
        edw_crncy_exch.ex_rt_typ,
        edw_crncy_exch.from_crncy,
        edw_crncy_exch.to_crncy,
        (cast((99999999) as decimal) - cast((edw_crncy_exch.vld_from) as decimal)) as valid_date,
        edw_crncy_exch.ex_rt
    from edw_crncy_exch
    where
    (
        (cast((edw_crncy_exch.from_crncy) as text) = cast('MYR' as text))
        and (cast((edw_crncy_exch.to_crncy) as text) in ('USD', 'MYR'))
        and (cast((edw_crncy_exch.ex_rt_typ) as text) = cast('DWBP' as text))
    )
),
eotd as 
(
    select distinct
        edw_vw_os_time_dim."year",
        edw_vw_os_time_dim.mnth_id,
        edw_vw_os_time_dim.cal_date_id
    from edw_vw_os_time_dim
),
eotd2 as
(
    select distinct
        edw_vw_os_time_dim."year",
        edw_vw_os_time_dim.mnth_id
    from edw_vw_os_time_dim
),
combined_curr_exch as
(
    select
        curr_exch.ex_rt_typ as rate_type,
        curr_exch.from_crncy as from_ccy,
        curr_exch.to_crncy as to_ccy,
        eotd2.mnth_id,
        eotd."year",
        curr_exch.valid_date,
        curr_exch.ex_rt as exch_rate
    from 
    curr_exch,            
    eotd,
    eotd2
    where
    (
        (cast((cast((curr_exch.valid_date) as varchar)) as text) = eotd.cal_date_id)
        and 
        (eotd."year" = eotd2."year")
    )

),
max_mnth as
(   
    select
        rate_type,
        from_ccy,
        to_ccy,
        max(mnth_id) as mnth_id
    from
    combined_curr_exch
    group by
    rate_type,
    from_ccy,
    to_ccy
),
max_currency_exchng as 
(
    select
        curr_exch.rate_type,
        curr_exch.from_ccy,
        curr_exch.to_ccy,
        curr_exch.mnth_id,
        curr_exch.exch_rate
    from
    combined_curr_exch as curr_exch,
    max_mnth
    where
    (
        (
            (
                (
                    (cast((curr_exch.rate_type) as text) = cast((max_mnth.rate_type) as text))
                    and (cast((curr_exch.from_ccy) as text) = cast((max_mnth.from_ccy) as text))
                )
                and (cast((curr_exch.to_ccy) as text) = cast((max_mnth.to_ccy) as text))
            )
        )
        and (curr_exch.mnth_id = max_mnth.mnth_id)
    )   
),
t2_max as
(

    select
        max_currency_exchng.rate_type,
        max_currency_exchng.from_ccy,
        max_currency_exchng.to_ccy,
        max_currency_exchng.mnth_id,
        max_currency_exchng.exch_rate
    from 
    max_currency_exchng
),
combined_max_eotd2 as
(
            select
                t1."year",
                t1.mnth_id,
                t2.rate_type,
                t2.from_ccy,
                t2.to_ccy,
                t2.mnth_id as max_mnth_id,
                t2.exch_rate as max_mnth_exch_rate
            from 
            eotd2 as t1, 
            t2_max as t2
),
ce as
(  
    select
        currency_exchng.rate_type,
        currency_exchng.from_ccy,
        currency_exchng.to_ccy,
        currency_exchng."year",
        currency_exchng.mnth_id,
        currency_exchng.valid_date,
        currency_exchng.exch_rate
    from 
    combined_curr_exch as currency_exchng
),
t1 as
(
    select
        case
            when 
            (
                (
                    (cast((coalesce(ce.from_ccy, eotd.from_ccy)) as text) = cast('MYR' as text))
                    or 
                    (
                        (coalesce(ce.from_ccy, eotd.from_ccy) IS NULL) 
                        and ('MYR' IS NULL)
                    )
                )
            )    
            then cast('MY' as text)
            else cast(NULL as text)
        end as cntry_key,
        case
            when
            (
                (cast((coalesce(ce.from_ccy, eotd.from_ccy)) as text) = cast('MYR' as text))
                or (
                        (coalesce(ce.from_ccy, eotd.from_ccy) IS NULL) 
                        and ('MYR' IS NULL)
                    )
            )
            then cast('MALAYSIA' as text)
            else cast(NULL as text)
        end as cntry_nm,
        coalesce(ce.rate_type, eotd.rate_type) as rate_type,
        coalesce(ce.from_ccy, eotd.from_ccy) as from_ccy,
        coalesce(ce.to_ccy, eotd.to_ccy) as to_ccy,
        coalesce(ce."year", eotd."year") as "year",
        coalesce(ce.mnth_id, eotd.mnth_id) as mnth_id,
        coalesce(ce.exch_rate, eotd.max_mnth_exch_rate) as exch_rate
    from 
    combined_max_eotd2 as eotd
    LEFT JOIN ce
    ON ce.mnth_id = eotd.mnth_id
),
t2 as 
(
    select distinct
        edw_vw_os_time_dim.mnth_id,
        min(edw_vw_os_time_dim.cal_date_id) over (partition by edw_vw_os_time_dim."year" order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as cal_date_id
    from edw_vw_os_time_dim
),
transformed_set1 as
(
    select
        distinct
        cast((t1.cntry_key) as varchar) as cntry_key,
        cast((t1.cntry_nm) as varchar) as cntry_nm,
        t1.rate_type,
        t1.from_ccy,
        t1.to_ccy,
        cast((cast((t2.cal_date_id) as decimal)) as decimal(9, 0)) as valid_date,
        t1."year" as jj_year,
        cast((t1.mnth_id) as varchar) as jj_mnth_id,
        t1.exch_rate
    from t1, t2
    where t1.mnth_id = t2.mnth_id
),
transformed_set2 as
(
    select
        distinct
        cast((t1.cntry_key) as varchar) as cntry_key,
        cast((t1.cntry_nm) as varchar) as cntry_nm,
        t1.rate_type,
        t1.from_ccy,
        t1.from_ccy as to_ccy,
        cast((cast((t2.cal_date_id) as decimal)) as decimal(9, 0)) as valid_date,
        t1."year" as jj_year,
        cast((t1.mnth_id) as varchar) as jj_mnth_id,
        1 as exch_rate
    from t1, t2
    where t1.mnth_id = t2.mnth_id
),

final as (
    select * from transformed_set1
    union all
    select * from transformed_set2
)
select * from final