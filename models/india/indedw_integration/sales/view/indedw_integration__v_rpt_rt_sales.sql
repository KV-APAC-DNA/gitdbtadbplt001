{{
    config(
        materialized='view'
    )
}}

with edw_rt_sales_fact as
(
    select * from snapindedw_integration.edw_rt_sales_fact
    --{{ ref('indedw_integration__edw_rt_sales_fact') }}
),
edw_subd_retailer_dim as
(
    select * from snapindedw_integration.edw_subd_retailer_dim
    --{{ ref('indedw_integration__edw_subd_retailer_dim') }}
),
itg_ruralstoreorderheader as
(
    select * from snapinditg_integration.itg_ruralstoreorderheader
),
itg_townmaster as
(
    select * from snapinditg_integration.itg_townmaster
),
edw_product_dim as
(
    select * from snapindedw_integration.edw_product_dim
    --{{ ref('indedw_integration__edw_product_dim') }}
),
edw_customer_dim as
(
    select * from snapindedw_integration.edw_customer_dim
    --{{ ref('indedw_integration__edw_customer_dim') }}
),
edw_retailer_calendar_dim as
(
    select * from snapindedw_integration.edw_retailer_calendar_dim
    --{{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
edw_retailer_dim as
(
    select * from snapindedw_integration.edw_retailer_dim
),
edw_retailer_geocoordinates as
(
    select * from snapindedw_integration.edw_retailer_geocoordinates
    --{{ ref('indedw_integration__edw_retailer_geocoordinates') }}
),
itg_udcmapping as
(
    select * from snapinditg_integration.itg_udcmapping
    --{{ ref('inditg_integration__itg_udcmapping') }}
),
itg_rrl_udcmaster as
(
    select * from snapinditg_integration.itg_rrl_udcmaster
    --{{ ref('inditg_integration__itg_rrl_udcmaster') }}
),
itg_salesmanmaster as
(
    select * from snapinditg_integration.itg_salesmanmaster
),
v_edw_subd_retailer_dim as
(
    select * from {{ ref('indedw_integration__v_edw_subd_retailer_dim') }}
),
final as 
(
    SELECT base.quantity,
          base.achievement_amt,
          base.no_of_lines,
          base.no_of_buying_retailers,
          base.no_of_bills,
          base.no_of_packs,
          base.retailer_code,
          base.retailer_name,
          base.retailer_status,
          base.order_date,
          base.order_id,
          base.subd_code,
          base.subd_name,
          base.subd_status,
          base.rtr_code,
          base.route_code,
          base.route_name,
          base.superstockiest_code,
          base.superstockiest_name,
          base.retailer_category,
          base.retailer_channel,
          base.retailer_class,
          base.mobile,
          base.owner_name,
          base.psr_code,
          base.psr_name,
          base.user_ret_code,
          base.region_name,
          base.zone_name,
          base.territory_name,
          base.village_code,
          base.village_name,
          base.village,
          base.brand_name,
          base.franchise_name,
          base.mothersku_name,
          base.product_category_name,
          base.product_name,
          base.variant_name,
          base.day,
          base.week,
          base."month",
          base.qtr,
          base."year",
          base.csrtrcode,
          base.rgc_latitude,
          base.rgc_longtitude,
          base.rgc_geouniqueid,
          base.abi_ntid,
          base.flm_ntid,
          base.bdm_ntid,
          base.rsm_ntid,
          base.udc_kings,
          base.udc_baby_shelf_visibility,
          base.udc_sanpro_shelf_visibility,
          base.udc_sanpro_basket,
          base.udc_mi_champion_2021,
          COALESCE(dim.district_name, 'NAME NOT AVAILABLE'::character VARYING) AS district_name,
          COALESCE(dim.town_name, 'NAME NOT AVAILABLE'::character VARYING)     AS town_name,
          base.rtruniquecode,
          base.uniquesalescode,
          base.latest_subd_code
    FROM ((SELECT sum(rt.qty) AS quantity,
                  sum(rt.achievement_amt)             AS achievement_amt,
                  count(rt.lines)                   AS no_of_lines,
                  count(DISTINCT rt.buying_retailers) AS no_of_buying_retailers,
                  count(DISTINCT rt.bills)            AS no_of_bills,
                  count(DISTINCT rt.product_code)     AS no_of_packs,
                  s.subd_ret_code                     AS retailer_code,
                  s.subd_ret_name                     AS retailer_name,
                  s.subd_ret_status                   AS retailer_status,
                  rt.order_date,
                  rt.order_id,
                  s.subd_code,
                  s.subd_name,
                  s.subd_status,
                  s.subd_rtr_code                 AS rtr_code,
                  s.subd_route_code               AS route_code,
                  s.subd_route_name               AS route_name,
                  s.customer_code                 AS superstockiest_code,
                  s.customer_name                 AS superstockiest_name,
                  lat_subd.subd_ret_category_name AS retailer_category,
                  lat_subd.subd_ret_channel_name  AS retailer_channel,
                  lat_subd.subd_ret_class_name    AS retailer_class,
                  s.mobile,
                  s.subd_ret_owner     AS owner_name,
                  s.subd_salesman_code AS psr_code,
                  s.subd_salesman_name AS psr_name,
                  (((((s.customer_code)::text|| ('_'::character VARYING)::text)|| (s.subd_code)::text)|| ('_'::character VARYING)::text)||(s.subd_ret_code)::text) AS user_ret_code,
                  c.region_name,
                  c.zone_name,
                  c.territory_name,
                  rso.route_code          AS village_code,
                  tm.villagename          AS village_name,
                  s.subd_ret_village_name AS village,
                  p.brand_name,
                  p.franchise_name,
                  p.mothersku_name,
                  p.product_category_name,
                  p.product_name,
                  p.variant_name,
                  d.day,
                  d.week,
                  d.month_nm_shrt AS "month",
                  d.qtr,
                  d.fisc_yr AS "year",
                  ret.csrtrcode,
                  geo.rgc_latitude,
                  geo.rgc_longtitude,
                  geo.rgc_geouniqueid,
                  NULL::character VARYING                                                            AS abi_ntid,
                  NULL::character VARYING                                                            AS flm_ntid,
                  NULL::character VARYING                                                            AS bdm_ntid,
                  NULL::character VARYING                                                            AS rsm_ntid,
                  COALESCE(udc_details.udc_kings, ('NA'::character VARYING)::text)                   AS udc_kings,
                  COALESCE(udc_details.udc_baby_shelf_visibility, ('NA'::character VARYING)::text)   AS udc_baby_shelf_visibility,
                  COALESCE(udc_details.udc_sanpro_shelf_visibility, ('NA'::character VARYING)::text) AS udc_sanpro_shelf_visibility,
                  COALESCE(udc_details.udc_sanpro_basket, ('NA'::character VARYING)::text)           AS udc_sanpro_basket,
                  COALESCE(udc_details.udc_mi_champion_2021, ('NA'::character VARYING)::text)        AS udc_mi_champion_2021,
                  ret.rtruniquecode,
                  ism.uniquesalescode,
                  lat_subd.subd_code AS latest_subd_code
            FROM (((((((((((edw_rt_sales_fact rt 
                            LEFT JOIN (SELECT DISTINCT derived_table1.subd_ret_code,
                                                       derived_table1.subd_ret_name,
                                                       derived_table1.subd_ret_status,
                                                       derived_table1.subd_code,
                                                       derived_table1.subd_name,
                                                       derived_table1.subd_status,
                                                       derived_table1.subd_rtr_code,
                                                       derived_table1.subd_route_code,
                                                       derived_table1.subd_route_name,
                                                       derived_table1.customer_code,
                                                       derived_table1.customer_name,
                                                       derived_table1.subd_ret_category_name,
                                                       derived_table1.subd_ret_channel_name,
                                                       derived_table1.subd_ret_class_name,
                                                       derived_table1.mobile,
                                                       derived_table1.subd_ret_owner,
                                                       derived_table1.subd_salesman_code,
                                                       derived_table1.subd_salesman_name,
                                                       derived_table1.subd_ret_village_name 
                                        FROM (SELECT DISTINCT edw_subd_retailer_dim.subd_ret_code,
                                                              edw_subd_retailer_dim.subd_ret_name,
                                                              edw_subd_retailer_dim.subd_ret_status,
                                                              edw_subd_retailer_dim.subd_code,
                                                              edw_subd_retailer_dim.subd_name,
                                                              edw_subd_retailer_dim.subd_status,
                                                              edw_subd_retailer_dim.subd_rtr_code,
                                                              edw_subd_retailer_dim.subd_route_code,
                                                              edw_subd_retailer_dim.subd_route_name,
                                                              edw_subd_retailer_dim.customer_code,
                                                              edw_subd_retailer_dim.customer_name,
                                                              edw_subd_retailer_dim.subd_ret_category_name,
                                                              edw_subd_retailer_dim.subd_ret_channel_name,
                                                              edw_subd_retailer_dim.subd_ret_class_name,
                                                              edw_subd_retailer_dim.mobile,
                                                              edw_subd_retailer_dim.subd_ret_owner,
                                                              edw_subd_retailer_dim.subd_salesman_code,
                                                              edw_subd_retailer_dim.subd_salesman_name,
                                                              edw_subd_retailer_dim.subd_ret_village_name,
                                                              row_number() OVER( partition BY edw_subd_retailer_dim.customer_code, edw_subd_retailer_dim.subd_ret_code, edw_subd_retailer_dim.subd_code, edw_subd_retailer_dim.subd_salesman_code order by null) AS rn
                                              FROM edw_subd_retailer_dim edw_subd_retailer_dim) derived_table1
                                        WHERE (derived_table1.rn = 1)) s ON ((((((rt.customer_code)::text = (s.customer_code)::text) 
                                        AND ((rt.subd_ret_code)::text = (s.subd_ret_code)::text))
                                        AND (upper((rt.subd_code)::text) = upper((s.subd_code)::text)))
                                        AND ((rt.user_code)::text = (s.subd_salesman_code)::text))))
                                LEFT JOIN (SELECT derived_table1.customer_code,
                                                derived_table1.subd_ret_code,
                                                derived_table1.subd_rtr_code,
                                                derived_table1.subd_salesman_code,
                                                derived_table1.subd_code,
                                                derived_table1.subd_ret_channel_name,
                                                derived_table1.subd_ret_category_name,
                                                derived_table1.subd_ret_class_name
                                                FROM (SELECT edw_subd_retailer_dim.customer_code,
                                                            edw_subd_retailer_dim.subd_ret_code,
                                                            edw_subd_retailer_dim.subd_rtr_code,
                                                            edw_subd_retailer_dim.subd_salesman_code,
                                                            edw_subd_retailer_dim.subd_code,
                                                            edw_subd_retailer_dim.subd_ret_channel_name,
                                                            edw_subd_retailer_dim.subd_ret_category_name,
                                                            edw_subd_retailer_dim.subd_ret_class_name,
                                                            row_number() OVER( partition BY edw_subd_retailer_dim.subd_ret_code ORDER BY edw_subd_retailer_dim.createddate DESC) AS rn
                                              FROM  edw_subd_retailer_dim edw_subd_retailer_dim) derived_table1
                                              WHERE (derived_table1.rn = 1)) lat_subd
                                              ON (((rt.subd_ret_code)::text = (lat_subd.subd_ret_code)::text)))
                                LEFT JOIN itg_ruralstoreorderheader rso ON ((((rt.order_id)::text = (rso.orderid)::text)
                                AND ((rt.subd_ret_code)::text = (rso.retailerid)::text))))
                                LEFT JOIN (SELECT derived_table1.routecode,
                                                derived_table1.villagecode,
                                                derived_table1.villagename,
                                                derived_table1.population,
                                                derived_table1.rsrcode,
                                                derived_table1.rsdcode,
                                                derived_table1.distributorcode,
                                                derived_table1.sarpanchname,
                                                derived_table1.sarpanchno,
                                                derived_table1.isactive,
                                                derived_table1.createddate,
                                                derived_table1.createdby,
                                                derived_table1.updateddate,
                                                derived_table1.updatedby,
                                                derived_table1.filename,
                                                derived_table1.crt_dttm,
                                                derived_table1.updt_dttm,
                                                derived_table1.rn
                                                FROM (SELECT itg_townmaster.routecode,
                                                            itg_townmaster.villagecode,
                                                            itg_townmaster.villagename,
                                                            itg_townmaster.population,
                                                            itg_townmaster.rsrcode,
                                                            itg_townmaster.rsdcode,
                                                            itg_townmaster.distributorcode,
                                                            itg_townmaster.sarpanchname,
                                                            itg_townmaster.sarpanchno,
                                                            itg_townmaster.isactive,
                                                            itg_townmaster.createddate,
                                                            itg_townmaster.createdby,
                                                            itg_townmaster.updateddate,
                                                            itg_townmaster.updatedby,
                                                            itg_townmaster.filename,
                                                            itg_townmaster.crt_dttm,
                                                            itg_townmaster.updt_dttm,
                                                            row_number() OVER( partition BY itg_townmaster.villagecode, itg_townmaster.distributorcode, upper((itg_townmaster.rsdcode)::text), itg_townmaster.rsrcode, itg_townmaster.routecode ORDER BY itg_townmaster.createddate DESC) AS rn
                                                    FROM itg_townmaster) derived_table1
                                                    WHERE (derived_table1.rn = 1)) tm
                                                    ON (((((((rso.route_code)::text = (tm.villagecode)::text)
                                                    AND ((rso.ord_distributorcode)::text = (tm.distributorcode)::text))
                                                    AND (upper((rso.rsd_code)::text) = upper((tm.rsdcode)::text)))
                                                    AND (upper((tm.routecode)::text) = upper((s.subd_route_code)::text)))
                                                    AND (upper((rso.usercode)::text) = upper((tm.rsrcode)::text)))))
                                LEFT JOIN edw_product_dim p ON (((rt.product_code)::text = (p.product_code)::text)))
                                LEFT JOIN edw_customer_dim c ON (((rt.customer_code)::text = (c.customer_code)::text)))
                                LEFT JOIN edw_retailer_calendar_dim d ON (((to_char(rt.order_date, ('YYYYMMDD'::character VARYING)::text))::integer = d.day)))
                                LEFT JOIN (SELECT derived_table2.customer_code,
                                                derived_table2.retailer_code,
                                                derived_table2.csrtrcode,
                                                derived_table2.rtruniquecode
                                            FROM (SELECT edw_retailer_dim.customer_code,
                                                        edw_retailer_dim.retailer_code,
                                                        edw_retailer_dim.csrtrcode,
                                                        edw_retailer_dim.rtruniquecode,
                                                        row_number() OVER( partition BY edw_retailer_dim.customer_code, edw_retailer_dim.retailer_code order by null) AS rn
                                                FROM edw_retailer_dim 
                                                WHERE  (edw_retailer_dim.file_rec_dt = (SELECT "max"(edw_retailer_dim.file_rec_dt) AS file_rec_dt
                                                                                            FROM edw_retailer_dim))) derived_table2
                                WHERE (derived_table2.rn = 1)) ret ON ((((rt.customer_code)::text = (ret.customer_code)::text)
                                AND (upper((rt.subd_code)::text) = upper((ret.csrtrcode)::text)))))
                                LEFT JOIN (SELECT DISTINCT edw_retailer_geocoordinates.rgc_usercode,
                                                        edw_retailer_geocoordinates.rgc_code,
                                                        edw_retailer_geocoordinates.rgc_latitude,
                                                        edw_retailer_geocoordinates.rgc_longtitude,
                                                        edw_retailer_geocoordinates.rgc_geouniqueid
                                            FROM edw_retailer_geocoordinates) geo 
                                            ON ((((rt.user_code)::text = (geo.rgc_usercode)::text)
                                            AND ((rt.subd_ret_code)::text = (geo.rgc_code)::text))))
                                LEFT JOIN (SELECT derived_table1.distcode   AS customer_code,
                                                derived_table1.rsdcode    AS subd_code,
                                                derived_table1.outletcode AS subd_ret_code,
                                                derived_table1.usercode   AS user_code,
                                                "max"((CASE WHEN (((upper((derived_table1.udccode)::text))::character VARYING)::text = ((upper(('UDC001'::character VARYING)::text))::character VARYING)::text) 
                                                            THEN
                                                            CASE WHEN (((((derived_table1.isactive)::integer)::character VARYING)::text = ('1'::character VARYING)::text) OR ((((derived_table1.isactive)::integer)::character VARYING IS NULL) AND ('1' IS NULL))) THEN 'YES'::character VARYING
                                                                WHEN (((((derived_table1.isactive)::integer)::character VARYING)::text = ('0'::character VARYING)::text) OR ((((derived_table1.isactive)::integer)::character VARYING IS NULL) AND ('0' IS NULL))) THEN 'NO'::character VARYING
                                                                ELSE NULL::character VARYING
                                                                END
                                                        ELSE NULL::character VARYING END)::text) AS udc_kings,
                                                "max"((CASE WHEN (((upper((derived_table1.udccode)::text))::character VARYING)::text = ((upper(('UDC002'::character VARYING)::text))::character VARYING)::text) 
                                                            THEN
                                                            CASE WHEN (((((derived_table1.isactive)::integer)::character VARYING)::text = ('1'::character VARYING)::text) OR ((((derived_table1.isactive)::integer)::character VARYING IS NULL) AND ('1' IS NULL))) THEN 'YES'::character VARYING
                                                                WHEN (((((derived_table1.isactive)::integer)::character VARYING)::text = ('0'::character VARYING)::text) OR ((((derived_table1.isactive)::integer)::character VARYING IS NULL) AND ('0' IS NULL))) THEN 'NO'::character VARYING
                                                                ELSE NULL::character VARYING
                                                                END
                                                        ELSE NULL::character VARYING END)::text) AS udc_baby_shelf_visibility,
                                                "max"((CASE WHEN (((upper((derived_table1.udccode)::text))::character VARYING)::text = ((upper(('UDC003'::character VARYING)::text))::character VARYING)::text) 
                                                            THEN
                                                            CASE WHEN (((((derived_table1.isactive)::integer)::character VARYING)::text = ('1'::character VARYING)::text) OR ((((derived_table1.isactive)::integer)::character VARYING IS NULL) AND ('1' IS NULL))) THEN 'YES'::character VARYING
                                                                WHEN (((((derived_table1.isactive)::integer)::character VARYING)::text = ('0'::character VARYING)::text) OR ((((derived_table1.isactive)::integer)::character VARYING IS NULL) AND ('0' IS NULL))) THEN 'NO'::character VARYING
                                                                ELSE NULL::character VARYING
                                                                END
                                                        ELSE NULL::character VARYING END)::text) AS udc_sanpro_shelf_visibility,
                                                "max"((CASE WHEN (((upper((derived_table1.udccode)::text))::character VARYING)::text = ((upper(('UDC004'::character VARYING)::text))::character VARYING)::text) 
                                                            THEN
                                                            CASE WHEN (((((derived_table1.isactive)::integer)::character VARYING)::text = ('1'::character VARYING)::text) OR ((((derived_table1.isactive)::integer)::character VARYING IS NULL) AND ('1' IS NULL))) THEN 'YES'::character VARYING
                                                                WHEN (((((derived_table1.isactive)::integer)::character VARYING)::text = ('0'::character VARYING)::text) OR ((((derived_table1.isactive)::integer)::character VARYING IS NULL) AND ('0' IS NULL))) THEN 'NO'::character VARYING
                                                                ELSE NULL::character VARYING
                                                                END
                                                            ELSE NULL::character VARYING END)::text) AS udc_sanpro_basket,
                                                "max"((CASE WHEN (((upper((derived_table1.udccode)::text))::character VARYING)::text = ((upper(('UDC005'::character VARYING)::text))::character VARYING)::text) 
                                                            THEN 
                                                            CASE WHEN (((((derived_table1.isactive)::integer)::character VARYING)::text = ('1'::character VARYING)::text) OR ((((derived_table1.isactive)::integer)::character VARYING IS NULL) AND ('1' IS NULL))) THEN 'YES'::character VARYING
                                                                WHEN (((((derived_table1.isactive)::integer)::character VARYING)::text = ('0'::character VARYING)::text) OR ((((derived_table1.isactive)::integer)::character VARYING IS NULL) AND ('0' IS NULL))) THEN 'NO'::character VARYING
                                                                ELSE NULL::character VARYING
                                                                END
                                                            ELSE NULL::character VARYING END)::text) AS udc_mi_champion_2021
                                        FROM (SELECT udc_1.distcode,
                                                        udc_1.rsdcode,
                                                        udc_1.outletcode,
                                                        udc_1.usercode,
                                                        udc_1.udccode,
                                                        udc_1.isactive,
                                                        udc_1.rn 
                                                FROM (SELECT itg_udcdetails.distcode,
                                                            itg_udcdetails.rsdcode,
                                                            itg_udcdetails.outletcode,
                                                            itg_udcdetails.usercode,
                                                            udcmaster.udccode,
                                                            itg_udcdetails.isactive,
                                                            row_number() OVER(partition BY itg_udcdetails.udccode, itg_udcdetails.distcode, itg_udcdetails.rsdcode, itg_udcdetails.outletcode, itg_udcdetails.usercode ORDER BY itg_udcdetails.createddate DESC, itg_udcdetails.isactive DESC nulls last) AS rn
                                                    FROM (itg_udcmapping itg_udcdetails
                                                    LEFT JOIN itg_rrl_udcmaster udcmaster
                                                    ON (((itg_udcdetails.udccode)::text = (udcmaster.udccode)::text)))) udc_1
                                                    WHERE (udc_1.rn = 1)) derived_table1
                                        GROUP BY derived_table1.distcode,
                                                    derived_table1.rsdcode,
                                                    derived_table1.outletcode,
                                                    derived_table1.usercode) udc_details
                        ON ((((((rt.customer_code)::text = (udc_details.customer_code)::text)
                        AND ((rt.subd_code)::text = (udc_details.subd_code)::text))
                        AND ((rt.subd_ret_code)::text = (udc_details.subd_ret_code)::text))
                        AND ((rt.user_code)::text = (udc_details.user_code)::text))))
                        LEFT JOIN (SELECT derived_table1.distcode,
                                        derived_table1.smcode,
                                        derived_table1.uniquesalescode
                                FROM (SELECT DISTINCT itg_salesmanmaster.distcode,
                                                        itg_salesmanmaster.smcode,
                                                        itg_salesmanmaster.uniquesalescode,
                                                        row_number() OVER( partition BY itg_salesmanmaster.distcode, itg_salesmanmaster.smcode ORDER BY to_date(itg_salesmanmaster.createddate) DESC nulls last, itg_salesmanmaster.uniquesalescode DESC nulls last) AS rn
                                        FROM itg_salesmanmaster) derived_table1
                                        WHERE (derived_table1.rn = 1)) ism
                                        ON ((((s.customer_code)::text = (ism.distcode)::text)
                                        AND ((s.subd_salesman_code)::text = (((ism.smcode)::text|| ('-'::character VARYING)::text)|| (ism.distcode)::text)))))
                        GROUP BY  rt.order_date,
                                rt.order_id,
                                s.subd_ret_code,
                                s.subd_ret_name,
                                s.subd_ret_status,
                                s.subd_code,
                                s.subd_name,
                                s.subd_status,
                                s.subd_rtr_code,
                                s.subd_route_code,
                                s.subd_route_name,
                                s.customer_code,
                                s.customer_name,
                                s.mobile,
                                s.subd_ret_owner,
                                s.subd_salesman_code,
                                s.subd_salesman_name,
                                c.region_name,
                                c.zone_name,
                                c.territory_name,
                                rso.route_code,
                                tm.villagename,
                                s.subd_ret_village_name,
                                p.brand_name,
                                p.franchise_name,
                                p.mothersku_name,
                                p.product_category_name,
                                p.product_name,
                                p.variant_name,
                                d.day,
                                d.week,
                                d.month_nm_shrt,
                                d.qtr,
                                d.fisc_yr,
                                ret.csrtrcode,
                                geo.rgc_latitude,
                                geo.rgc_longtitude,
                                geo.rgc_geouniqueid,
                                udc_details.udc_kings,
                                udc_details.udc_baby_shelf_visibility,
                                udc_details.udc_sanpro_shelf_visibility,
                                udc_details.udc_sanpro_basket,
                                udc_details.udc_mi_champion_2021,
                                ret.rtruniquecode,
                                ism.uniquesalescode,
                                lat_subd.subd_code,
                                lat_subd.subd_ret_channel_name,
                                lat_subd.subd_ret_category_name,
                                lat_subd.subd_ret_class_name) base
    LEFT JOIN (SELECT derived_table1.customer_code,
                    derived_table1.subd_ret_code,
                    derived_table1.subd_rtr_code,
                    derived_table1.district_name,
                    derived_table1.town_name
            FROM (SELECT v_edw_subd_retailer_dim.customer_code,
                            v_edw_subd_retailer_dim.subd_ret_code,
                            v_edw_subd_retailer_dim.subd_rtr_code,
                            v_edw_subd_retailer_dim.district_name,
                            v_edw_subd_retailer_dim.town_name,
                            row_number() OVER( partition BY v_edw_subd_retailer_dim.customer_code, v_edw_subd_retailer_dim.subd_ret_code, v_edw_subd_retailer_dim.subd_rtr_code ORDER BY v_edw_subd_retailer_dim.crt_dttm DESC) AS rnk
                    FROM v_edw_subd_retailer_dim) derived_table1
                    WHERE (derived_table1.rnk = 1)) dim 
                    ON (((((base.superstockiest_code)::text = (dim.customer_code)::text)
                    AND ((base.rtr_code)::text = (dim.subd_rtr_code)::text))
                    AND ((base.retailer_code)::text = (dim.subd_ret_code)::text))))
)

select * from final