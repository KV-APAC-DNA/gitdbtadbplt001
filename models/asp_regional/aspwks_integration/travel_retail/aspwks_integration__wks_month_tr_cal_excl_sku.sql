{{
    config(
        transient=true
    )
}}
with edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_rg_travel_retail as (
    select * from {{ ref('aspedw_integration__edw_rg_travel_retail') }}
)
{% set query %} 
select distinct cal_mo_1 
                 from edw_calendar_dim where cal_mo_1 >= (select min(year_month) from edw_rg_travel_retail)
                  and cal_mo_1 <=  (select to_char(add_months (to_date(max(year_month),'YYYYMM'),1),'YYYYMM') from edw_rg_travel_retail) 
                order by 1 
                
{% endset %}

{% set results = run_query(query) %}

{% if execute %}
    {% set mnth = results.columns[0].values() %}
{% else %}
    {% set mnth = [] %}
{% endif %}
,
{% for item in mnth %}

    wks_month_tr_cal_{{loop.index}}
    as 
        (
            select distinct cal_mo_1, 
            cast(to_char(to_date(dateadd ('MONTH',-1,to_date(cast(cal_mo_1 as varchar),'YYYYMM'))),'YYYYMM') as integer)  prev1
            from edw_calendar_dim 
            where edw_calendar_dim.cal_mo_1 = {{item}}
        )
    {%- if not loop.last -%},
    {%endif%}
{%endfor%}  
{% for item in mnth %}
    select * from wks_month_tr_cal_{{loop.index}}
    {% if not loop.last %}
    union all
    {%endif%}
{%endfor%}  