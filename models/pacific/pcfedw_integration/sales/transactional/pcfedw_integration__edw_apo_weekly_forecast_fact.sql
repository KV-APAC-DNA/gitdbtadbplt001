with itg_weekly_forecast as(
    select * from {{ ref('pcfitg_integration__itg_weekly_forecast') }}
),
edw_time_dim as(
    select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
transformed as(
    select
        salesorg::varchar(4) as sales_org,
        mercia_reference::varchar(5) as channel,
        material::varchar(18) as material,
        (substring(fiscper, 1, 4) || substring(fiscper, 6, 7))::number(7,0) as period,
        calweek::number(6,0) as week_no,
        to_char(cast(jnj_fiscal_week as timestampntz), 'YYYYMMDD')::varchar(16) as week_date,
        tot_forecast::number(17,3) as total_fcst,
        tot_bas_fct::number(17,3) as tot_bas_fct,
        tot_forecast - tot_bas_fct::number(17,3) as promo_fcst
    from itg_weekly_forecast
    where to_char(snap_dt,'YYYYMMDD')=
        (select to_char(max(snap_dt),'YYYYMMDD') 
        from itg_weekly_forecast where fiscyear = 
        (select jj_year from edw_time_dim 
        where (cal_date)::date=current_date())) 
    and salesorg in ('3300','3410')

)
select * from transformed

