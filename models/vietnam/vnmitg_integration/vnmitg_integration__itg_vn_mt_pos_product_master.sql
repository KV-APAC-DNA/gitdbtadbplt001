with sdl_mds_vn_pos_products as (
    select * from select * from DEV_DNA_LOAD.SNAPOSESDL_RAW.SDL_MDS_VN_POS_PRODUCTS

),
wks
AS
(SELECT--- case 1.a: PK present in ITG, record is updated, insert the old record from ITG keeping active as 'N'
       POS.barcode,
       POS.changetrackingmask,
       POS.code,
       POS.customer,
       POS.customer_sku,
       POS.enterdatetime,
       POS.enterusername,
       POS.enterversionnumber,
       POS.id,
       POS.lastchgdatetime,
       POS.lastchgusername,
       POS.lastchgversionnumber,
       POS.muid,
       POS.name,
       POS.validationstatus,
       POS.version_id,
       POS.versionflag,
       POS.versionname,
       POS.versionnumber,
       POS.effective_from,
       CASE
         WHEN POS.effective_to IS NULL THEN dateadd (DAY,-1,current_timestamp())
         ELSE POS.effective_to
       END AS effective_to,
       'N' AS active,
       POS.run_id,
       POS.crtd_dttm,
       POS.updt_dttm
FROM (SELECT itg.*,
             ROW_NUMBER() OVER (PARTITION BY sdl.customer_sku,sdl.customer order by sdl.lastchgdatetime) AS rn
      FROM sdl_mds_vn_pos_products sdl,
           {{this}} itg
      WHERE sdl.lastchgdatetime != itg.lastchgdatetime
      AND   sdl.customer_sku = itg.customer_sku
	  AND   SDL.customer = itg.customer) POS
WHERE POS.rn = 1
UNION ALL
SELECT--- case 1.b: PK is present in ITG, record is updated, insert new record from SDL keeping active as 'Y'
       POS.barcode,
       POS.changetrackingmask,
       POS.code,
       POS.customer,
       POS.customer_sku,
       POS.enterdatetime,
       POS.enterusername,
       POS.enterversionnumber,
       POS.id,
       POS.lastchgdatetime,
       POS.lastchgusername,
       POS.lastchgversionnumber,
       POS.muid,
       POS.name,
       POS.validationstatus,
       POS.version_id,
       POS.versionflag,
       POS.versionname,
       POS.versionnumber,
       current_timestamp() AS effective_from,
       NULL AS effective_to,
       'Y' AS active,
       null as run_id,
       POS.enterdatetime,
       --- taking from SDL enterdatetime
       current_timestamp() AS updt_dttm
FROM (SELECT sdl.*,
             ROW_NUMBER() OVER (PARTITION BY sdl.customer_sku, sdl.customer order by sdl.lastchgdatetime) AS rn
      FROM sdl_mds_vn_pos_products sdl,
           {{this}} itg
      WHERE sdl.lastchgdatetime != itg.lastchgdatetime
      AND   sdl.customer_sku = itg.customer_sku
	  AND   sdl.customer = itg.customer
      AND   itg.active = 'Y') POS
WHERE POS.rn = 1
UNION ALL
SELECT--- case 2: PK present in ITG and active = 'Y', record is not updated in SDL, hence insert SDL record in ITG keeping active 'Y'
       POS.barcode,
       POS.changetrackingmask,
       POS.code,
       POS.customer,
       POS.customer_sku,
       POS.enterdatetime,
       POS.enterusername,
       POS.enterversionnumber,
       POS.id,
       POS.lastchgdatetime,
       POS.lastchgusername,
       POS.lastchgversionnumber,
       POS.muid,
       POS.name,
       POS.validationstatus,
       POS.version_id,
       POS.versionflag,
       POS.versionname,
       POS.versionnumber,
       POS.effective_from,
       NULL AS effective_to,
       'Y' AS active,
       null as run_id,
       POS.crtd_dttm,
       current_timestamp() AS updt_dttm
FROM (SELECT sdl.*,
             itg.effective_from,
             itg.crtd_dttm,
             ROW_NUMBER() OVER (PARTITION BY sdl.customer_sku,sdl.customer order by sdl.lastchgdatetime) AS rn
      FROM sdl_mds_vn_pos_products sdl,
           {{this}} itg
      WHERE sdl.lastchgdatetime = itg.lastchgdatetime
      AND   sdl.customer_sku = itg.customer_sku
	  AND sdl.customer = itg.customer
      AND   itg.active = 'Y') POS
WHERE POS.rn = 1
UNION ALL
SELECT--- case 3: PK present in ITG and active = 'N', record is not updated in SDL, hence insert ITG record in ITG keeping active 'N'
       POS.barcode,
       POS.changetrackingmask,
       POS.code,
       POS.customer,
       POS.customer_sku,
       POS.enterdatetime,
       POS.enterusername,
       POS.enterversionnumber,
       POS.id,
       POS.lastchgdatetime,
       POS.lastchgusername,
       POS.lastchgversionnumber,
       POS.muid,
       POS.name,
       POS.validationstatus,
       POS.version_id,
       POS.versionflag,
       POS.versionname,
       POS.versionnumber,
       POS.effective_from,
       POS.effective_to,
       POS.active,
       null as run_id,
       POS.crtd_dttm,
       POS.updt_dttm
FROM (SELECT itg.*,
             ROW_NUMBER() OVER (PARTITION BY sdl.customer_sku, sdl.customer order by sdl.lastchgdatetime) AS rn
      FROM sdl_mds_vn_pos_products sdl,
           {{this}} itg
      WHERE sdl.lastchgdatetime = itg.lastchgdatetime
      AND   sdl.customer_sku = itg.customer_sku
	  AND   sdl.customer = itg.customer
      AND   itg.active = 'N') POS
WHERE POS.rn = 1
UNION ALL
SELECT--- case 4: PK not present in ITG, insert the whole new record in ITG from SDL keeping active 'Y'
       POS.barcode,
       POS.changetrackingmask,
       POS.code,
       POS.customer,
       POS.customer_sku,
       POS.enterdatetime,
       POS.enterusername,
       POS.enterversionnumber,
       POS.id,
       POS.lastchgdatetime,
       POS.lastchgusername,
       POS.lastchgversionnumber,
       POS.muid,
       POS.name,
       POS.validationstatus,
       POS.version_id,
       POS.versionflag,
       POS.versionname,
       POS.versionnumber,
       current_timestamp() AS effective_from,
       NULL AS effective_to,
       'Y' AS active,
       null as run_id,
       POS.enterdatetime,
       -- taking from SDL enterdatetime
       current_timestamp() AS updt_dttm
FROM (SELECT sdl.*,
             ROW_NUMBER() OVER (PARTITION BY sdl.customer_sku, sdl.customer order by sdl.lastchgdatetime) AS rn
      FROM sdl_mds_vn_pos_products sdl
      WHERE (sdl.customer_sku,sdl.customer) NOT IN (SELECT customer_sku, customer FROM {{this}})) POS
WHERE POS.rn = 1) SELECT*FROM wks
UNION ALL
SELECT *
FROM {{this}} dksh
WHERE (dksh.customer_sku, dksh.customer) NOT IN (SELECT customer_sku,customer FROM wks)