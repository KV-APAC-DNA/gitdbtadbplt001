with sdl_mds_vn_pos_customers as (
    select * from DEV_DNA_LOAD.SNAposESDL_RAW.SDL_MDS_VN_pos_CUSTOMERS
),
wks as
(select--- case 1.a: PK present in ITG, record is updated, insert the old record from ITG keeping active as 'N'
       pos.chain,
       pos.changetrackingmask,
       pos.code,
       pos.customer_code,
       pos.customer_name,
       pos.customer_store_code,
       pos.district,
       pos.enterdatetime,
       pos.enterusername,
       pos.enterversionnumber,
       pos.format,
       pos.id,
       pos.lastchgdatetime,
       pos.lastchgusername,
       pos.lastchgversionnumber,
       pos.muid,
       pos.name,
       pos.note_closed_store,
       pos.plant,
       pos.status,
       pos.store_code,
       pos.store_name,
       pos.store_name_2,
       pos.validationstatus,
       pos.version_id,
       pos.versionflag,
       pos.versionname,
       pos.versionnumber,
       pos.wh,
       pos.zone,
       pos.effective_from,
       case
         when pos.effective_to IS NULL then dateadd (DAY,-1,current_timestamp())
         else pos.effective_to
       end as effective_to,
       'N' as active,
       pos.run_id,
       pos.crtd_dttm,
       pos.updt_dttm
from (select itg.*,
             row_number() over (partition by sdl.store_code,sdl.customer_name order by sdl.lastchgdatetime) as rn
      from sdl_mds_vn_pos_customers sdl,
           {{this}} itg
      where sdl.lastchgdatetime != itg.lastchgdatetime
      AND   sdl.store_code = itg.store_code 
	    AND   sdl.customer_name = itg.customer_name) pos
where pos.rn = 1
UNION ALL
select--- case 1.b: PK is present in ITG, record is updated, insert new record from SDL keeping active as 'Y'
       pos.chain,
       pos.changetrackingmask,
       pos.code,
       pos.customer_code,
       pos.customer_name,
       pos.customer_store_code,
       pos.district,
       pos.enterdatetime,
       pos.enterusername,
       pos.enterversionnumber,
       pos.format,
       pos.id,
       pos.lastchgdatetime,
       pos.lastchgusername,
       pos.lastchgversionnumber,
       pos.muid,
       pos.name,
       pos.note_closed_store,
       pos.plant,
       pos.status,
       pos.store_code,
       pos.store_name,
       pos.store_name_2,
       pos.validationstatus,
       pos.version_id,
       pos.versionflag,
       pos.versionname,
       pos.versionnumber,
       pos.wh,
       pos.zone,
       current_timestamp() as effective_from,
       NULL as effective_to,
       'Y' as active,
      null as run_id,
       pos.enterdatetime,
       --- taking from SDL enterdatetime
       current_timestamp() as updt_dttm
from (select itg.*,
             row_number() over (partition by sdl.store_code,sdl.customer_name order by sdl.lastchgdatetime) as rn
      from sdl_mds_vn_pos_customers sdl,
           {{this}} itg
      where sdl.lastchgdatetime != itg.lastchgdatetime
      AND   sdl.store_code = itg.store_code
	  AND   sdl.customer_name = itg.customer_name
      AND   itg.active = 'Y') pos
where pos.rn = 1
UNION ALL
select--- case 2: PK present in ITG and active = 'Y', record is not updated in SDL, hence insert SDL record in ITG keeping active 'Y'
       pos.chain,
       pos.changetrackingmask,
       pos.code,
       pos.customer_code,
       pos.customer_name,
       pos.customer_store_code,
       pos.district,
       pos.enterdatetime,
       pos.enterusername,
       pos.enterversionnumber,
       pos.format,
       pos.id,
       pos.lastchgdatetime,
       pos.lastchgusername,
       pos.lastchgversionnumber,
       pos.muid,
       pos.name,
       pos.note_closed_store,
       pos.plant,
       pos.status,
       pos.store_code,
       pos.store_name,
       pos.store_name_2,
       pos.validationstatus,
       pos.version_id,
       pos.versionflag,
       pos.versionname,
       pos.versionnumber,
       pos.wh,
       pos.zone,
       pos.effective_from,
       NULL as effective_to,
       'Y' as active,
      null as run_id,
       pos.crtd_dttm,
       current_timestamp() as updt_dttm
from (select sdl.*,
             itg.effective_from,
             itg.crtd_dttm,
             row_number() over (partition by sdl.store_code,sdl.customer_name order by sdl.lastchgdatetime desc) as rn
      from sdl_mds_vn_pos_customers sdl,
           {{this}} itg
      where sdl.lastchgdatetime = itg.lastchgdatetime
      AND   sdl.store_code = itg.store_code
	  AND   sdl.customer_name = itg.customer_name
      AND   itg.active = 'Y') pos
where pos.rn = 1
UNION ALL
select--- case 3: PK present in ITG and active = 'N', record is not updated in SDL, hence insert ITG record in ITG keeping active 'N'
       pos.chain,
       pos.changetrackingmask,
       pos.code,
       pos.customer_code,
       pos.customer_name,
       pos.customer_store_code,
       pos.district,
       pos.enterdatetime,
       pos.enterusername,
       pos.enterversionnumber,
       pos.format,
       pos.id,
       pos.lastchgdatetime,
       pos.lastchgusername,
       pos.lastchgversionnumber,
       pos.muid,
       pos.name,
       pos.note_closed_store,
       pos.plant,
       pos.status,
       pos.store_code,
       pos.store_name,
       pos.store_name_2,
       pos.validationstatus,
       pos.version_id,
       pos.versionflag,
       pos.versionname,
       pos.versionnumber,
       pos.wh,
       pos.zone,
       pos.effective_from,
       pos.effective_to,
       pos.active,
      null as run_id,
       pos.crtd_dttm,
       pos.updt_dttm
from (select itg.*,
             row_number() over (partition by sdl.store_code,sdl.customer_name order by sdl.lastchgdatetime desc) as rn
      from sdl_mds_vn_pos_customers sdl,
           {{this}} itg
      where sdl.lastchgdatetime = itg.lastchgdatetime
      AND   sdl.store_code = itg.store_code
	  AND   sdl.customer_name = itg.customer_name
      AND   itg.active = 'N') pos
where pos.rn = 1
UNION ALL
select--- case 4: PK not present in ITG, insert the whole new record in ITG from SDL keeping active 'Y'
       pos.chain,
       pos.changetrackingmask,
       pos.code,
       pos.customer_code,
       pos.customer_name,
       pos.customer_store_code,
       pos.district,
       pos.enterdatetime,
       pos.enterusername,
       pos.enterversionnumber,
       pos.format,
       pos.id,
       pos.lastchgdatetime,
       pos.lastchgusername,
       pos.lastchgversionnumber,
       pos.muid,
       pos.name,
       pos.note_closed_store,
       pos.plant,
       pos.status,
       pos.store_code,
       pos.store_name,
       pos.store_name_2,
       pos.validationstatus,
       pos.version_id,
       pos.versionflag,
       pos.versionname,
       pos.versionnumber,
       pos.wh,
       pos.zone,
       current_timestamp() as effective_from,
       NULL as effective_to,
       'Y' as active,
      null as run_id,
       pos.enterdatetime,
       -- taking from SDL enterdatetime
      current_timestamp() as updt_dttm
from (select sdl.*,
             case when sdl.store_code is not null then row_number() over (partition by sdl.store_code, sdl.customer_name order by sdl.lastchgdatetime desc) 
             when sdl.store_code is null then row_number() over (partition by sdl.store_name, sdl.customer_name order by sdl.lastchgdatetime desc) end as rn
      from sdl_mds_vn_pos_customers sdl
      where (nvl(sdl.store_code,''),sdl.customer_name) NOT IN (select nvl(store_code,''),customer_name from {{this}})) pos
where pos.rn = 1) ,
transformed as(
select * from wks
UNION ALL
select *
from {{this}} pos
where (pos.store_code,pos.customer_name) NOT IN (select store_code,customer_name from wks)
),
final as 
(
select 
chain::varchar(200) as chain,
changetrackingmask::number(18,0) as changetrackingmask,
code::varchar(500) as code,
customer_code::varchar(200) as customer_code,
customer_name::varchar(200) as customer_name,
customer_store_code::varchar(200) as customer_store_code,
district::varchar(200) as district,
enterdatetime::timestamp_ntz(9) as enterdatetime,
enterusername::varchar(200) as enterusername,
enterversionnumber::number(18,0) as enterversionnumber,
format::varchar(200) as format,
id::number(18,0) as id,
lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
lastchgusername::varchar(200) as lastchgusername,
lastchgversionnumber::number(18,0) as lastchgversionnumber,
muid::varchar(36) as muid,
name::varchar(500) as name,
note_closed_store::varchar(200) as note_closed_store,
plant::varchar(200) as plant,
status::varchar(200) as status,
store_code::varchar(200) as store_code,
store_name::varchar(200) as store_name,
store_name_2::varchar(200) as store_name_2,
validationstatus::varchar(500) as validationstatus,
version_id::number(18,0) as version_id,
versionflag::varchar(100) as versionflag,
versionname::varchar(100) as versionname,
versionnumber::number(18,0) as versionnumber,
wh::varchar(200) as wh,
zone::varchar(200) as zone,
effective_from::timestamp_ntz(9) as effective_from,
effective_to::timestamp_ntz(9) as effective_to,
active::varchar(2) as active,
run_id::number(14,0) as run_id,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final