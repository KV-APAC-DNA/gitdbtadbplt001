{{
    config(
        materialized='view'
    )
}}

with edw_subd_retailer_dim as
(
    select * from snapindedw_integration.edw_subd_retailer_dim
    --{{ ref('indedw_integration__edw_subd_retailer_dim') }}
),
itg_mds_in_subdtown_district_master as 
(
    select * from snapinditg_integration.itg_mds_in_subdtown_district_master
    --{{ ref('inditg_integration__itg_mds_in_subdtown_district_master') }}
),
final as 
(
    SELECT
    dim.subd_ret_code,
    dim.start_date,
    dim.end_date,
    dim.subd_rtr_code,
    dim.subd_ret_name,
    dim.subd_ret_address1,
    dim.subd_ret_address2,
    dim.subd_ret_address3,
    dim.subd_ret_class_code,
    dim.subd_ret_class_name,
    dim.subd_ret_category_code,
    dim.subd_ret_category_name,
    dim.subd_ret_status,
    dim.subd_ret_owner,
    dim.subd_ret_channel_code,
    dim.subd_ret_channel_name,
    dim.subd_route_code,
    dim.subd_route_name,
    dim.subd_ret_village_code,
    dim.subd_ret_village_name,
    dim.subd_code,
    dim.subd_name,
    dim.subd_status,
    dim.customer_code,
    dim.customer_name,
    dim.distributor_status,
    dim.subd_salesman_code,
    dim.subd_salesman_name,
    dim.region,
    dim.zone,
    dim.territory,
    dim.salesman_status,
    dim.mobile,
    dim.display_status,
    dim.crt_dttm,
    dim.updt_dttm,
    dim.actv_flg,
    COALESCE(mapp.district_name, 'NAME NOT AVAILABLE'::character varying) AS district_name,
    COALESCE(mapp.town_name, 'NAME NOT AVAILABLE'::character varying) AS town_name,
    dim.createddate
    FROM
        edw_subd_retailer_dim dim
    LEFT JOIN
        itg_mds_in_subdtown_district_master mapp
    ON
        dim.customer_code::text = mapp.cust_code::text
        AND dim.subd_rtr_code::text = mapp.retailer_code::text
)

select * from final