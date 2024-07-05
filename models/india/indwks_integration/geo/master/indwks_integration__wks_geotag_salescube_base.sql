with 
v_rpt_sales_details as 
(
    select * from {{ ref('indedw_integration__v_rpt_sales_details') }}
),
edw_customer_dim as 
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
itg_query_parameters as 
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
final as 
(
SELECT (sd.fisc_yr) as fisc_yr,
       (sd.qtr) as qtr,
       sd.mth_mm,
       sd.customer_code,
       sd.retailer_code,
       sd.retailer_name,
       sd.rtruniquecode,
       sd.channel_name,
       sd.retailer_channel_3,
       sd.latest_customer_code,
       sd.latest_customer_name,
       sd.region_name,
       sd.zone_name,
       sd.territory_name,
       sd.latest_salesman_code,
       sd.latest_salesman_name,
       sd.latest_uniquesalescode,
       cd.region_name AS latest_region_name,
       cd.zone_name AS latest_zone_name,
       sd.latest_territory_name,
       sd.nielsen_popstrata,
       sd.nielsen_statename,
       SUM(sd.achievement_nr) AS mthly_achievement_nr
from  v_rpt_sales_details sd                
left join edw_customer_dim cd
       on sd.latest_customer_code = cd.customer_code
where  sd.fisc_yr > extract(year from current_timestamp()) -(select cast(parameter_value as integer) as parameter_value
                                                 from itg_query_parameters
                                                 where upper(country_code) = 'IN'
                                                   and upper(parameter_type) = 'DATA_RETENTION_PERIOD'
                                                   and upper(parameter_name) = 'IN_GEO_TAG_DATA_RETENTION_PERIOD')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
)
select * from final