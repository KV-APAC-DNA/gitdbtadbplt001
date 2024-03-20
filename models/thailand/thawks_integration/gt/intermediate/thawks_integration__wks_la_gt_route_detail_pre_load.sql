{{
    config(
        pre_hook="{{build_wks_la_gt_route_detail_hashkey()}}"
    )
}}


with wks_la_gt_route_detail_hashkey as (
    select * from {{ source('thawks_integration', 'wks_la_gt_route_detail_hashkey') }}
),
sdl_la_gt_route_detail as (
    --select * from {{ source('thasdl_raw', 'sdl_la_gt_route_detail') }}
    select * from DEV_DNA_LOAD.snaposesdl_raw.sdl_la_gt_route_detail
)

SELECT wks.route_id,
       wks.customer_id,
       wks.route_no,
       wks.saleunit,
       wks.ship_to,
       wks.contact_person,
       wks.created_date,
       wks.file_upload_date,
       to_date(sdl.created_date ,'YYYYMMDD') AS last_modified_date,
       'D' AS flag,
       wks.filename,
       wks.run_id,
       wks.crt_dttm
FROM wks_la_gt_route_detail_hashkey wks,
     sdl_la_gt_route_detail sdl
WHERE wks.hashkey = sdl.hashkey
AND   COALESCE(wks.created_date,'9999-12-31') <>COALESCE(to_date(sdl.created_date,'YYYYMMDD'),'9999-12-31')
AND   UPPER(wks.flag) IN ('I','U')
UNION ALL
--------------------******keeps the historic data in ITG itself which is not matched against staging & mark it as 'D'*****-------
SELECT wks.route_id,
       wks.customer_id,
       wks.route_no,
       wks.saleunit,
       wks.ship_to,
       wks.contact_person,
       wks.created_date,
       wks.file_upload_date,
       sdl.file_upload_date AS last_modified_date,
       'D' AS flag,
       wks.filename,
       wks.run_id,
       wks.crt_dttm
FROM wks_la_gt_route_detail_hashkey wks,
     (SELECT MAX(file_upload_date) file_upload_date
      FROM sdl_la_gt_route_detail) sdl
WHERE NOT EXISTS (SELECT 1
                  FROM sdl_la_gt_route_detail sdl
                  WHERE wks.hashkey = sdl.hashkey)
AND   UPPER(wks.flag) IN ('I','U')
UNION ALL
-----------*****insert the new routes for the same pk's with staging created date(Not Required Scenario)*****----------
SELECT sdl.route_id,
       sdl.customer_id,
       sdl.route_no,
       sdl.saleunit,
       sdl.ship_to,
       sdl.contact_person,
       to_date(sdl.created_date,'YYYYMMDD') as created_date,
       sdl.file_upload_date,
       '9999-12-31' AS last_modified_date,
       'I' AS flag,
       sdl.filename,
       sdl.run_id,
       sdl.crt_dttm
FROM wks_la_gt_route_detail_hashkey wks,
     sdl_la_gt_route_detail sdl
WHERE wks.hashkey = sdl.hashkey
AND   COALESCE(wks.created_date,'9999-12-31') <>COALESCE(to_date(sdl.created_date,'YYYYMMDD'),'9999-12-31')
AND   UPPER(wks.flag) IN ('I','U')
UNION ALL
-------------*****for matched pk's update the records from staging to ITG*****--------------------------
SELECT wks.route_id,
       wks.customer_id,
       wks.route_no,
       wks.saleunit,
       sdl.ship_to,
       sdl.contact_person,
        to_date(sdl.created_date ,'YYYYMMDD')  as created_date,
        sdl.file_upload_date,
       '9999-12-31' AS last_modified_date,
       'U' AS flag,
       sdl.filename,
       sdl.run_id,
       sdl.crt_dttm
FROM wks_la_gt_route_detail_hashkey wks,
     sdl_la_gt_route_detail sdl
WHERE wks.hashkey = sdl.hashkey
AND   COALESCE(wks.created_date,'9999-12-31') = COALESCE(to_date(sdl.created_date,'YYYYMMDD'),'9999-12-31')
AND   UPPER(wks.flag) IN ('I','U')
UNION ALL
------------------*****insert new records from staging to ITG*****-------------
SELECT sdl.route_id,
       sdl.customer_id,
       sdl.route_no,
       sdl.saleunit,
       sdl.ship_to,
       sdl.contact_person,
       to_date(sdl.created_date , 'YYYYMMDD') as created_date,
       sdl.file_upload_date,
       '9999-12-31' AS last_modified_date,
       'I' AS flag,
       sdl.filename,
       sdl.run_id,
       sdl.crt_dttm
FROM sdl_la_gt_route_detail sdl
WHERE NOT EXISTS (SELECT 1
                  FROM wks_la_gt_route_detail_hashkey wks
                  WHERE wks.hashkey = sdl.hashkey
                  AND   UPPER(wks.flag) IN ('I','U'))