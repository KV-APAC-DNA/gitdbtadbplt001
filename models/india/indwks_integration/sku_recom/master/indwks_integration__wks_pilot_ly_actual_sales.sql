{{
    config
    (
        post_hook = "INSERT INTO {% if target.name=='prod' %}
                    indwks_integration.wks_pilot_sku_recom_tbl2
                {% else %}
                    {{schema}}.indwks_integration__wks_pilot_sku_recom_tbl2
                {% endif %}
        (mth_mm,qtr,fisc_yr,month,cust_cd,customer_name,retailer_cd,rtruniquecode,
        region_name,zone_name,territory_name,class_desc,class_desc_a_sa_flag,channel_name,
        business_channel,retailer_name,retailer_category_name,status_desc,salesman_code,salesman_name,
        unique_sales_code,route_code,route_name,nup_actual_ly,sales_flag)
        SELECT mth_mm::integer,qtr,fisc_yr,month,customer_code,customer_name,retailer_code,
        rtruniquecode,region_name,zone_name,territory_name,class_desc,class_desc_a_sa_flag,
        channel_name,business_channel,retailer_name,retailer_category_name,status_desc,salesman_code,
        salesman_name,unique_sales_code,route_code,route_name,nup_actual_ly,'SALES_YES_LY'
        FROM {{this}}"
    )
}}

with v_rpt_sales_details as
(
    select * from indedw_integration.v_rpt_sales_details
    --{{ ref('indedw_integration__v_rpt_sales_details') }}
),
wks_pilot_ly_actual_nup as
(
    select * from {{ ref('indwks_integration__wks_pilot_ly_actual_nup') }}
),
final as
(
    SELECT LEFT(sf.mth_mm,4)::integer +1 || RIGHT(sf.mth_mm,2) AS mth_mm,
       sf.qtr,
       sf.fisc_yr + 1  AS fisc_yr,
       sf.month,
       NVL(sf.latest_customer_code,sf.customer_code) AS customer_code,
       NVL(sf.latest_customer_name,sf.customer_name) AS customer_name,
       sf.retailer_code,
       sf.rtruniquecode,
       sf.region_name,
       sf.zone_name,
       NVL(sf.latest_territory_name,sf.territory_name) AS territory_name,
       sf.class_desc,
       CASE WHEN sf.class_desc IN ('A','SA') THEN 1 ELSE 0 END AS class_desc_a_sa_flag,
       sf.channel_name,
       sf.business_channel,
       sf.retailer_name,
       sf.retailer_category_name,
       sf.status_desc,
       NVL(sf.latest_salesman_code,sf.salesman_code) AS salesman_code,
       NVL(sf.latest_salesman_name,sf.salesman_name) AS salesman_name,
       NVL(sf.latest_uniquesalescode,sf.unique_sales_code) AS unique_sales_code,
       sf.route_code,
       sf.route_name,
       nup_ly.nup AS nup_actual_ly
    FROM  v_rpt_sales_details sf
    LEFT JOIN wks_pilot_ly_actual_nup nup_ly
        ON sf.rtruniquecode = nup_ly.rtruniquecode
        AND sf.mth_mm = nup_ly.mth_mm
    WHERE sf.mth_mm IN (SELECT mth_mm
                        FROM wks_pilot_ly_actual_nup
                        GROUP BY 1)
    GROUP BY sf.mth_mm,
        sf.qtr,
        sf.fisc_yr,
        sf.month,
        NVL(sf.latest_customer_code,sf.customer_code),
        NVL(sf.latest_customer_name,sf.customer_name),
        sf.retailer_code,
        sf.rtruniquecode,
        sf.region_name,
        sf.zone_name,
        NVL(sf.latest_territory_name,sf.territory_name),
        sf.class_desc,
        CASE WHEN sf.class_desc IN ('A','SA') THEN 1 ELSE 0 END,
        sf.channel_name,
        sf.business_channel,
        sf.retailer_name,
        sf.retailer_category_name,
        sf.status_desc,
        NVL(sf.latest_salesman_code,sf.salesman_code),
        NVL(sf.latest_salesman_name,sf.salesman_name),
        NVL(sf.latest_uniquesalescode,sf.unique_sales_code),
        sf.route_code,
        sf.route_name,
        nup_ly.nup
)
select * from final