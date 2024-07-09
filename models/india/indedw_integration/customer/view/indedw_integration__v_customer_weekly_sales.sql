with edw_dailysales_fact as
(
    select * from {{ ref('indedw_integration__edw_dailysales_fact') }}
),
edw_retailer_calendar_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
transformed as
(
    SELECT dsf.customer_code,
    dsf.salesman_code,
    dsf.salesman_name,
    dsf.product_code,
    dsf.route_code,
    dsf.route_name,
    rcd.fisc_yr AS inv_year,
    rcd.mth_yyyymm AS inv_month,
    rcd.week AS inv_week,
    sum(dsf.achievement_nr_val) AS achievement_nr_val,
    (((((dsf.customer_code)::TEXT || ('-'::CHARACTER VARYING)::TEXT) || (dsf.retailer_code)::TEXT) || ('_'::CHARACTER VARYING)::TEXT) || (dsf.invoice_no)::TEXT) AS bills,
    sum(dsf.no_of_lines) AS packs,
    dsf.retailer_code,
    dsf.retailer_name,
    sum(dsf.achievement_amt) AS achievement_amt,
    rcd.day AS inv_day,
    dsf.STATUS AS salesman_status
FROM edw_dailysales_fact dsf,
    edw_retailer_calendar_dim rcd
WHERE (dsf.invoice_date = rcd.day)
GROUP BY rcd.fisc_yr,
    rcd.mth_yyyymm,
    rcd.week,
    dsf.customer_code,
    dsf.salesman_code,
    dsf.salesman_name,
    dsf.product_code,
    dsf.route_code,
    dsf.route_name,
    dsf.retailer_code,
    dsf.retailer_name,
    rcd.day,
    dsf.STATUS,
    dsf.invoice_no
),
final as 
(   
    select
        customer_code::varchar(50) as customer_code,
        salesman_code::varchar(100) as salesman_code,
        salesman_name::varchar(200) as salesman_name,
        product_code::varchar(50) as product_code,
        route_code::varchar(100) as route_code,
        route_name::varchar(200) as route_name,
        inv_year::number(18,0) as inv_year,
        inv_month::number(18,0) as inv_month,
        inv_week::number(18,0) as inv_week,
        achievement_nr_val::number(38,6) as achievement_nr_val,
        bills::varchar(16777216) as bills,
        packs::number(38,0) as packs,
        retailer_code::varchar(100) as retailer_code,
        retailer_name::varchar(100) as retailer_name,
        achievement_amt::number(38,6) as achievement_amt,
        inv_day::number(18,0) as inv_day,
        salesman_status::varchar(20) as salesman_status
    from transformed 
)
select * from final
