with sdl_mds_vn_pos_customers as (
    select * from DEV_DNA_LOAD.SNAPOSESDL_RAW.SDL_MDS_VN_POS_CUSTOMERS
),
wks
AS
(SELECT--- case 1.a: PK present in ITG, record is updated, insert the old record from ITG keeping active as 'N'
       POS.chain,
       POS.changetrackingmask,
       POS.code,
       POS.customer_code,
       POS.customer_name,
       POS.customer_store_code,
       POS.district,
       POS.enterdatetime,
       POS.enterusername,
       POS.enterversionnumber,
       POS.format,
       POS.id,
       POS.lastchgdatetime,
       POS.lastchgusername,
       POS.lastchgversionnumber,
       POS.muid,
       POS.name,
       POS.note_closed_store,
       POS.plant,
       POS.status,
       POS.store_code,
       POS.store_name,
       POS.store_name_2,
       POS.validationstatus,
       POS.version_id,
       POS.versionflag,
       POS.versionname,
       POS.versionnumber,
       POS.wh,
       POS.zone,
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
             ROW_NUMBER() OVER (PARTITION BY sdl.store_code,sdl.customer_name order by sdl.lastchgdatetime) AS rn
      FROM sdl_mds_vn_pos_customers sdl,
           {{this}} itg
      WHERE sdl.lastchgdatetime != itg.lastchgdatetime
      AND   sdl.store_code = itg.store_code 
	    AND   sdl.customer_name = itg.customer_name) POS
WHERE POS.rn = 1
UNION ALL
SELECT--- case 1.b: PK is present in ITG, record is updated, insert new record from SDL keeping active as 'Y'
       POS.chain,
       POS.changetrackingmask,
       POS.code,
       POS.customer_code,
       POS.customer_name,
       POS.customer_store_code,
       POS.district,
       POS.enterdatetime,
       POS.enterusername,
       POS.enterversionnumber,
       POS.format,
       POS.id,
       POS.lastchgdatetime,
       POS.lastchgusername,
       POS.lastchgversionnumber,
       POS.muid,
       POS.name,
       POS.note_closed_store,
       POS.plant,
       POS.status,
       POS.store_code,
       POS.store_name,
       POS.store_name_2,
       POS.validationstatus,
       POS.version_id,
       POS.versionflag,
       POS.versionname,
       POS.versionnumber,
       POS.wh,
       POS.zone,
       current_timestamp() AS effective_from,
       NULL AS effective_to,
       'Y' AS active,
      null as run_id,
       POS.enterdatetime,
       --- taking from SDL enterdatetime
       current_timestamp() AS updt_dttm
FROM (SELECT itg.*,
             ROW_NUMBER() OVER (PARTITION BY sdl.store_code,sdl.customer_name order by sdl.lastchgdatetime) AS rn
      FROM sdl_mds_vn_pos_customers sdl,
           {{this}} itg
      WHERE sdl.lastchgdatetime != itg.lastchgdatetime
      AND   sdl.store_code = itg.store_code
	  AND   sdl.customer_name = itg.customer_name
      AND   itg.active = 'Y') POS
WHERE POS.rn = 1
UNION ALL
SELECT--- case 2: PK present in ITG and active = 'Y', record is not updated in SDL, hence insert SDL record in ITG keeping active 'Y'
       POS.chain,
       POS.changetrackingmask,
       POS.code,
       POS.customer_code,
       POS.customer_name,
       POS.customer_store_code,
       POS.district,
       POS.enterdatetime,
       POS.enterusername,
       POS.enterversionnumber,
       POS.format,
       POS.id,
       POS.lastchgdatetime,
       POS.lastchgusername,
       POS.lastchgversionnumber,
       POS.muid,
       POS.name,
       POS.note_closed_store,
       POS.plant,
       POS.status,
       POS.store_code,
       POS.store_name,
       POS.store_name_2,
       POS.validationstatus,
       POS.version_id,
       POS.versionflag,
       POS.versionname,
       POS.versionnumber,
       POS.wh,
       POS.zone,
       POS.effective_from,
       NULL AS effective_to,
       'Y' AS active,
      null as run_id,
       POS.crtd_dttm,
       current_timestamp() AS updt_dttm
FROM (SELECT sdl.*,
             itg.effective_from,
             itg.crtd_dttm,
             ROW_NUMBER() OVER (PARTITION BY sdl.store_code,sdl.customer_name order by sdl.lastchgdatetime desc) AS rn
      FROM sdl_mds_vn_pos_customers sdl,
           {{this}} itg
      WHERE sdl.lastchgdatetime = itg.lastchgdatetime
      AND   sdl.store_code = itg.store_code
	  AND   sdl.customer_name = itg.customer_name
      AND   itg.active = 'Y') POS
WHERE POS.rn = 1
UNION ALL
SELECT--- case 3: PK present in ITG and active = 'N', record is not updated in SDL, hence insert ITG record in ITG keeping active 'N'
       POS.chain,
       POS.changetrackingmask,
       POS.code,
       POS.customer_code,
       POS.customer_name,
       POS.customer_store_code,
       POS.district,
       POS.enterdatetime,
       POS.enterusername,
       POS.enterversionnumber,
       POS.format,
       POS.id,
       POS.lastchgdatetime,
       POS.lastchgusername,
       POS.lastchgversionnumber,
       POS.muid,
       POS.name,
       POS.note_closed_store,
       POS.plant,
       POS.status,
       POS.store_code,
       POS.store_name,
       POS.store_name_2,
       POS.validationstatus,
       POS.version_id,
       POS.versionflag,
       POS.versionname,
       POS.versionnumber,
       POS.wh,
       POS.zone,
       POS.effective_from,
       POS.effective_to,
       POS.active,
      null as run_id,
       POS.crtd_dttm,
       POS.updt_dttm
FROM (SELECT itg.*,
             ROW_NUMBER() OVER (PARTITION BY sdl.store_code,sdl.customer_name order by sdl.lastchgdatetime desc) AS rn
      FROM sdl_mds_vn_pos_customers sdl,
           {{this}} itg
      WHERE sdl.lastchgdatetime = itg.lastchgdatetime
      AND   sdl.store_code = itg.store_code
	  AND   sdl.customer_name = itg.customer_name
      AND   itg.active = 'N') POS
WHERE POS.rn = 1
UNION ALL
SELECT--- case 4: PK not present in ITG, insert the whole new record in ITG from SDL keeping active 'Y'
       POS.chain,
       POS.changetrackingmask,
       POS.code,
       POS.customer_code,
       POS.customer_name,
       POS.customer_store_code,
       POS.district,
       POS.enterdatetime,
       POS.enterusername,
       POS.enterversionnumber,
       POS.format,
       POS.id,
       POS.lastchgdatetime,
       POS.lastchgusername,
       POS.lastchgversionnumber,
       POS.muid,
       POS.name,
       POS.note_closed_store,
       POS.plant,
       POS.status,
       POS.store_code,
       POS.store_name,
       POS.store_name_2,
       POS.validationstatus,
       POS.version_id,
       POS.versionflag,
       POS.versionname,
       POS.versionnumber,
       POS.wh,
       POS.zone,
       current_timestamp() AS effective_from,
       NULL AS effective_to,
       'Y' AS active,
      null as run_id,
       POS.enterdatetime,
       -- taking from SDL enterdatetime
      current_timestamp() AS updt_dttm
FROM (SELECT sdl.*,
             case when sdl.store_code is not null then ROW_NUMBER() OVER (PARTITION BY sdl.store_code, sdl.customer_name order by sdl.lastchgdatetime desc) 
             when sdl.store_code is null then ROW_NUMBER() OVER (PARTITION BY sdl.store_name, sdl.customer_name order by sdl.lastchgdatetime desc) end as rn
      FROM sdl_mds_vn_pos_customers sdl
      WHERE (nvl(sdl.store_code,''),sdl.customer_name) NOT IN (SELECT nvl(store_code,''),customer_name FROM {{this}})) POS
WHERE POS.rn = 1) 

SELECT*FROM wks
UNION ALL
SELECT *
FROM {{this}} pos
WHERE (pos.store_code,pos.customer_name) NOT IN (SELECT store_code,customer_name FROM wks)