{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["year_month"],
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )
    
}}

--Source CTE
with edw_rg_travel_retail as 
(
    select * from {{ ref('aspedw_integration__edw_rg_travel_retail') }}
),
wks_month_tr_cal as 
(
    select * from {{ ref('aspwks_integration__wks_month_tr_cal') }}
),

--Logical CTE
curr as (
    select ctry_cd,
             crncy_cd,
             country_name,
             retailer_name,
             year,
             month,
             year_month,
             dcl_code,
             matl_num,
             cust_num,
             sales_channel,
             max(curr_inv_qty) as curr_inv_qty,
             max(curr_inv_value_usd) as curr_inv_value_usd,
             sum(0) as opening_inv_qty,
             sum(0) as opening_inv_value_usd
      from (select ctry_cd,
                   crncy_cd,
                   country_name,
                   retailer_name,
                   "year" as year,
                   "month" as month,
                   year_month,
                   dcl_code,
                   matl_num,
                   cust_num,
                   sales_channel,
                   nvl(round(cast(case when (upper(identifier) = 'SELLIN' or upper(identifier) = 'SELLOUT') and upper(ctry_cd) in ('HK','SG','CN','KR','CM') then (sum(sellin_qty) over (partition by ctry_cd,year_month,retailer_name,cust_num,matl_num,sales_channel) - sum(sellout_qty) over (partition by ctry_cd,year_month,retailer_name,cust_num,matl_num,sales_channel)) else '0' end as numeric(38,5)),5),0) as curr_inv_qty,
                   nvl(round(cast(case when (upper(identifier) = 'SELLIN' or upper(identifier) = 'SELLOUT') and upper(ctry_cd) in ('HK','SG','CN','KR','CM') then (sum(sellin_qty) over (partition by ctry_cd,year_month,retailer_name,cust_num,matl_num,sales_channel) - sum(sellout_qty) over (partition by ctry_cd,year_month,retailer_name,cust_num,matl_num,sales_channel))*(srp_usd) else '0' end as numeric(38,5)),5),0) as curr_inv_value_usd
            from edw_rg_travel_retail tr1,
                 wks_month_tr_cal cal
            where tr1.year_month = cal.cal_mo_1)
      group by ctry_cd,
               crncy_cd,
               country_name,
               retailer_name,
               year,
               month,
               year_month,
               dcl_code,
               matl_num,
               cust_num,
               sales_channel
),
{% if is_incremental() %}
prev as (
    select ctry_cd,
             crncy_cd,
             country_name,
             retailer_name,
             year,
             month,
             year_month,
             dcl_code,
             matl_num,
             cust_num,
             sales_channel,
             sum(0) as curr_inv_qty,
             sum(0) as curr_inv_value_usd,
             max(opening_inv_qty) as opening_inv_qty,
             max(opening_inv_value_usd) as opening_inv_value_usd
      from (
	  select   tr2.ctry_cd,
             tr2.crncy_cd,
             tr2.country_name,
             tr2.retailer_name,
             cast(substring(cal.cal_mo_1,1,4) as int) as year,
             cast(substring(cal.cal_mo_1,5,6) as int) as month,
             cast(cal.cal_mo_1 as varchar) as year_month,
             tr2.dcl_code,
             tr2.matl_num,
             tr2.cust_num,
             tr2.sales_channel,
             0 as curr_inv_qty,
             0 as curr_inv_value_usd,
             nvl(round(cast((sum(tr2.closing_inv_qty) over (partition by tr2.ctry_cd,tr2.year_month,tr2.retailer_name,tr2.dcl_code,tr2.cust_num,tr2.matl_num,tr2.sales_channel)) as numeric(38,5)),5),0) as opening_inv_qty,
             nvl(round(cast((sum(tr2.closing_inv_value_usd) over (partition by tr2.ctry_cd,tr2.year_month,tr2.retailer_name,tr2.dcl_code,tr2.cust_num,tr2.matl_num,tr2.sales_channel)) as numeric(38,5)),5),0) as opening_inv_value_usd
       from {{this}} tr2,
           wks_month_tr_cal cal,
          (select distinct year_month,dcl_code from edw_rg_travel_retail) tr3
      where tr2.year_month = cal.prev1
      and tr2.dcl_code=tr3.dcl_code
      and cal.prev1=tr3.year_month)
	  group by ctry_cd,
               crncy_cd,
               country_name,
               retailer_name,
               year,
               month,
               year_month,
               dcl_code,
               matl_num,
               cust_num,
               sales_channel
),
{% endif %}
merged as (
    select * from curr
    {% if is_incremental() %}
    union all
    select * from prev
    {% endif %}
),

transformed as (
    select ctry_cd,
        crncy_cd,
        country_name,
        retailer_name,
        year,
        month,
        year_month,
        dcl_code,
        matl_num,
        cust_num,
        sales_channel,
        sum(opening_inv_qty) as opening_inv_qty,
        (sum(opening_inv_qty) + sum(curr_inv_qty)) as closing_inv_qty,
        sum(opening_inv_value_usd) as opening_inv_value_usd,
        (sum(opening_inv_value_usd) + sum(curr_inv_value_usd)) as closing_inv_value_usd
    from merged
    group by
        ctry_cd,
        crncy_cd,
        country_name,
        retailer_name,
        year,
        month,
        year_month,
        dcl_code,
        matl_num,
        cust_num,
        sales_channel
),

final as (
    select
        ctry_cd::varchar(150) as ctry_cd,
        crncy_cd::varchar(3) as crncy_cd,
        country_name::varchar(30) as country_name,
        retailer_name::varchar(150) as retailer_name,
        year::number(18,0) as year,
        month::number(18,0) as month,
        year_month::varchar(22) as year_month,
        dcl_code::varchar(50) as dcl_code,
        matl_num::varchar(30) as matl_num,
        cust_num::varchar(30) as cust_num,
        sales_channel::varchar(100) as sales_channel,
        opening_inv_qty::number(38,5) as opening_inv_qty,
        closing_inv_qty::number(38,5) as closing_inv_qty,
        opening_inv_value_usd::number(38,5) as opening_inv_value_usd,
        closing_inv_value_usd::number(38,5) as closing_inv_value_usd
    from transformed
)
select * from final