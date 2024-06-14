{{
    config(
        materialized='view'
    )
}}

with v_rpt_rt_sales as
(
    select * from {{ ref('indedw_integration__v_rpt_rt_sales') }}
),
itg_query_parameters as
(
    select * from inditg_integration.itg_query_parameters
),
edw_mi_sales_details as
(
    select * from {{ ref('indedw_integration__edw_mi_sales_details') }}
),
itg_mds_in_subdtown_district_master as
(
    select * from {{ ref('inditg_integration__itg_mds_in_subdtown_district_master') }}
),
final as
(
    SELECT   'RT_CUBE'::character VARYING AS data_source,
         v_rpt_rt_sales.week,
         v_rpt_rt_sales.month,
         v_rpt_rt_sales.qtr,
         v_rpt_rt_sales.year,
         ("substring"(((v_rpt_rt_sales.day)::character VARYING)::text, 1, 6))::integer AS mth_mm,
         v_rpt_rt_sales.superstockiest_code,
         (upper(rtrim((v_rpt_rt_sales.superstockiest_name)::text, ('.'::character VARYING)::text)))::character VARYING AS superstockiest_name,
         v_rpt_rt_sales.rtr_code  AS subd_cmp_code,
         v_rpt_rt_sales.subd_name,
         v_rpt_rt_sales.retailer_code,
         v_rpt_rt_sales.retailer_name,
         v_rpt_rt_sales.region_name,
         v_rpt_rt_sales.zone_name,
         v_rpt_rt_sales.territory_name,
         v_rpt_rt_sales.psr_code AS salesman_code,
         v_rpt_rt_sales.psr_name AS salesman_name,
         NULL::character VARYING AS salesman_status,
         v_rpt_rt_sales.district_name,
         v_rpt_rt_sales.town_name,
         v_rpt_rt_sales.retailer_status      AS rt_retailer_status,
         v_rpt_rt_sales.subd_status          AS rt_subd_status,
         NULL::character VARYING             AS sc_status_desc,
         NULL::character VARYING             AS sc_active_flag,
         sum(v_rpt_rt_sales.achievement_amt) AS achievement_amt,
         v_rpt_rt_sales.franchise_name,
         v_rpt_rt_sales.product_category_name,
         v_rpt_rt_sales.variant_name
        FROM v_rpt_rt_sales
        WHERE ((v_rpt_rt_sales.year)::DOUBLE PRECISION >= (date_part('year', current_timestamp()) - ((SELECT (itg_query_parameters.parameter_value)::integer AS parameter_value 
        FROM itg_query_parameters WHERE(((upper((itg_query_parameters.country_code)::text) = ('IN'::character VARYING)::text) 
        AND ( upper((itg_query_parameters.parameter_name)::text) = ('IN_MI_TDE_DATA_RETENTION_PERIOD'::character VARYING)::text)) 
        AND (upper((itg_query_parameters.parameter_type)::text) = ('DATA_RETENTION_PERIOD'::character VARYING)::text))))::DOUBLE PRECISION))
        GROUP BY 1,
         v_rpt_rt_sales.week,
         v_rpt_rt_sales.month,
         v_rpt_rt_sales.qtr,
         v_rpt_rt_sales.year,
         ("substring"(((v_rpt_rt_sales.day)::character VARYING)::text, 1, 6))::integer,
         v_rpt_rt_sales.superstockiest_code,
         v_rpt_rt_sales.superstockiest_name,
         v_rpt_rt_sales.rtr_code,
         v_rpt_rt_sales.subd_name,
         v_rpt_rt_sales.retailer_code,
         v_rpt_rt_sales.retailer_name,
         v_rpt_rt_sales.region_name,
         v_rpt_rt_sales.zone_name,
         v_rpt_rt_sales.territory_name,
         v_rpt_rt_sales.psr_code,
         v_rpt_rt_sales.psr_name,
         18,
         v_rpt_rt_sales.district_name,
         v_rpt_rt_sales.town_name,
         v_rpt_rt_sales.retailer_status,
         v_rpt_rt_sales.subd_status,
         23,
         24,
         v_rpt_rt_sales.franchise_name,
         v_rpt_rt_sales.product_category_name,
         v_rpt_rt_sales.variant_name
        UNION ALL
        SELECT 'SALES_CUBE'::character VARYING AS data_source,
          sc.week,
          sc.month,
          sc.qtr,
          sc.fisc_yr AS year,
          sc.mth_mm,
          sc.customer_code  AS superstockiest_code,
          (upper(rtrim((sc.customer_name)::text, ('.'::character VARYING)::text)))::character VARYING AS superstockiest_name,
          sc.retailer_code  AS subd_cmp_code,
          sc.retailer_name  AS subd_name,
          NULL::character VARYING AS retailer_code,
          NULL::character VARYING AS retailer_name,
          sc.region_name,
          sc.zone_name,
          sc.territory_name,
          ((((sc.salesman_code)::text|| ('-'::character VARYING)::text)|| (sc.customer_code)::text))::character VARYING AS salesman_code,
          sc.salesman_name,
          sc.salesman_status,
          dm.district_name,
          dm.town_name,
          NULL::character VARYING AS rt_retailer_status,
          NULL::character VARYING AS rt_subd_status,
          sc.status_desc          AS sc_status_desc,
          sc.active_flag          AS sc_active_flag,
          sum(sc.achievement_amt) AS achievement_amt,
          sc.franchise_name,
          sc.product_category_name,
          sc.variant_name
        FROM (edw_mi_sales_details sc
        LEFT JOIN itg_mds_in_subdtown_district_master dm
        ON ((((sc.customer_code)::text = (dm.cust_code)::text)
        AND ((sc.retailer_code)::text = (dm.retailer_code)::text))))
        WHERE ((sc.channel_name IN (SELECT itg_query_parameters.parameter_value FROM   itg_query_parameters
        WHERE (((upper((itg_query_parameters.country_code)::text) = ('IN'::character VARYING)::text)
        AND (upper((itg_query_parameters.parameter_name)::text) = ('IN_MI_TDE_SALES_CHANNEL_FR_RT'::character VARYING)::text))
        AND (upper((itg_query_parameters.parameter_type)::text) = ('CHANNEL_NAME'::character VARYING)::text))))
        AND ((sc.fisc_yr)::DOUBLE PRECISION >= (date_part('year', current_timestamp()) - ((SELECT (itg_query_parameters.parameter_value)::integer AS parameter_value FROM   itg_query_parameters
        WHERE (((upper((itg_query_parameters.country_code)::text) = ('IN'::character VARYING)::text)
        AND (upper((itg_query_parameters.parameter_name)::text) = ('IN_MI_TDE_DATA_RETENTION_PERIOD'::character VARYING)::text))
        AND (upper((itg_query_parameters.parameter_type)::text) = ('DATA_RETENTION_PERIOD'::character VARYING)::text))))::DOUBLE PRECISION)))
        GROUP BY  1,
          sc.week,
          sc.month,
          sc.qtr,
          sc.fisc_yr,
          sc.mth_mm,
          sc.customer_code,
          sc.customer_name,
          sc.retailer_code,
          sc.retailer_name,
          11,
          sc.region_name,
          sc.zone_name,
          sc.territory_name,
          sc.salesman_code,
          sc.salesman_name,
          sc.salesman_status,
          dm.district_name,
          dm.town_name,
          21,
          22,
          sc.status_desc,
          sc.active_flag,
          sc.franchise_name,
          sc.product_category_name,
          sc.variant_name
)

select * from final