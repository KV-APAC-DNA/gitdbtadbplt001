with sdl_mds_vn_pos_products as (
    select * from DEV_DNA_LOAD.VNMSDL_RAW.SDL_MDS_VN_pos_PRODUCTS
),
wks as
(select--- case 1.a: PK present in ITG, record is updated, insert the old record from ITG keeping active as 'N'
       pos.barcode,
       pos.changetrackingmask,
       pos.code,
       pos.customer,
       pos.customer_sku,
       pos.enterdatetime,
       pos.enterusername,
       pos.enterversionnumber,
       pos.id,
       pos.lastchgdatetime,
       pos.lastchgusername,
       pos.lastchgversionnumber,
       pos.muid,
       pos.name,
       pos.validationstatus,
       pos.version_id,
       pos.versionflag,
       pos.versionname,
       pos.versionnumber,
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
             row_number() over (partition by sdl.customer_sku,sdl.customer order by sdl.lastchgdatetime) as rn
      from sdl_mds_vn_pos_products sdl,
           {{this}} itg
      where sdl.lastchgdatetime != itg.lastchgdatetime
      and   sdl.customer_sku = itg.customer_sku
	  and   SDL.customer = itg.customer) pos
where pos.rn = 1
union all
select--- case 1.b: PK is present in ITG, record is updated, insert new record from SDL keeping active as 'Y'
       pos.barcode,
       pos.changetrackingmask,
       pos.code,
       pos.customer,
       pos.customer_sku,
       pos.enterdatetime,
       pos.enterusername,
       pos.enterversionnumber,
       pos.id,
       pos.lastchgdatetime,
       pos.lastchgusername,
       pos.lastchgversionnumber,
       pos.muid,
       pos.name,
       pos.validationstatus,
       pos.version_id,
       pos.versionflag,
       pos.versionname,
       pos.versionnumber,
       current_timestamp() as effective_from,
       NULL as effective_to,
       'Y' as active,
       null as run_id,
       pos.enterdatetime,
       --- taking from SDL enterdatetime
       current_timestamp() as updt_dttm
from (select sdl.*,
             row_number() over (partition by sdl.customer_sku, sdl.customer order by sdl.lastchgdatetime) as rn
      from sdl_mds_vn_pos_products sdl,
           {{this}} itg
      where sdl.lastchgdatetime != itg.lastchgdatetime
      and   sdl.customer_sku = itg.customer_sku
	  and   sdl.customer = itg.customer
      and   itg.active = 'Y') pos
where pos.rn = 1
union all
select--- case 2: PK present in ITG and active = 'Y', record is not updated in SDL, hence insert SDL record in ITG keeping active 'Y'
       pos.barcode,
       pos.changetrackingmask,
       pos.code,
       pos.customer,
       pos.customer_sku,
       pos.enterdatetime,
       pos.enterusername,
       pos.enterversionnumber,
       pos.id,
       pos.lastchgdatetime,
       pos.lastchgusername,
       pos.lastchgversionnumber,
       pos.muid,
       pos.name,
       pos.validationstatus,
       pos.version_id,
       pos.versionflag,
       pos.versionname,
       pos.versionnumber,
       pos.effective_from,
       NULL as effective_to,
       'Y' as active,
       null as run_id,
       pos.crtd_dttm,
       current_timestamp() as updt_dttm
from (select sdl.*,
             itg.effective_from,
             itg.crtd_dttm,
             row_number() over (partition by sdl.customer_sku,sdl.customer order by sdl.lastchgdatetime) as rn
      from sdl_mds_vn_pos_products sdl,
           {{this}} itg
      where sdl.lastchgdatetime = itg.lastchgdatetime
      and   sdl.customer_sku = itg.customer_sku
	  and sdl.customer = itg.customer
      and   itg.active = 'Y') pos
where pos.rn = 1
union all
select--- case 3: PK present in ITG and active = 'N', record is not updated in SDL, hence insert ITG record in ITG keeping active 'N'
       pos.barcode,
       pos.changetrackingmask,
       pos.code,
       pos.customer,
       pos.customer_sku,
       pos.enterdatetime,
       pos.enterusername,
       pos.enterversionnumber,
       pos.id,
       pos.lastchgdatetime,
       pos.lastchgusername,
       pos.lastchgversionnumber,
       pos.muid,
       pos.name,
       pos.validationstatus,
       pos.version_id,
       pos.versionflag,
       pos.versionname,
       pos.versionnumber,
       pos.effective_from,
       pos.effective_to,
       pos.active,
       null as run_id,
       pos.crtd_dttm,
       pos.updt_dttm
from (select itg.*,
             row_number() over (partition by sdl.customer_sku, sdl.customer order by sdl.lastchgdatetime) as rn
      from sdl_mds_vn_pos_products sdl,
           {{this}} itg
      where sdl.lastchgdatetime = itg.lastchgdatetime
      and   sdl.customer_sku = itg.customer_sku
	  and   sdl.customer = itg.customer
      and   itg.active = 'N') pos
where pos.rn = 1
union all
select--- case 4: PK not present in ITG, insert the whole new record in ITG from SDL keeping active 'Y'
       pos.barcode,
       pos.changetrackingmask,
       pos.code,
       pos.customer,
       pos.customer_sku,
       pos.enterdatetime,
       pos.enterusername,
       pos.enterversionnumber,
       pos.id,
       pos.lastchgdatetime,
       pos.lastchgusername,
       pos.lastchgversionnumber,
       pos.muid,
       pos.name,
       pos.validationstatus,
       pos.version_id,
       pos.versionflag,
       pos.versionname,
       pos.versionnumber,
       current_timestamp() as effective_from,
       NULL as effective_to,
       'Y' as active,
       null as run_id,
       pos.enterdatetime,
       -- taking from SDL enterdatetime
       current_timestamp() as updt_dttm
from (select sdl.*,
             row_number() over (partition by sdl.customer_sku, sdl.customer order by sdl.lastchgdatetime) as rn
      from sdl_mds_vn_pos_products sdl
      where (sdl.customer_sku,sdl.customer) NOT IN (select customer_sku, customer from {{this}})) pos
where pos.rn = 1) ,
transformed as (
select * from wks
union all
select *
from {{this}} dksh
where (dksh.customer_sku, dksh.customer) NOT IN (select customer_sku,customer from wks)
),
final as (
select 
    barcode::varchar(200) as barcode,
    changetrackingmask::number(18,0) as changetrackingmask,
    code::varchar(500) as code,
    customer::varchar(200) as customer,
    customer_sku::varchar(200) as customer_sku,
    enterdatetime::timestamp_ntz(9) as enterdatetime,
    enterusername::varchar(200) as enterusername,
    enterversionnumber::number(18,0) as enterversionnumber,
    id::number(18,0) as id,
    lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
    lastchgusername::varchar(200) as lastchgusername,
    lastchgversionnumber::number(18,0) as lastchgversionnumber,
    muid::varchar(36) as muid,
    name::varchar(500) as name,
    validationstatus::varchar(500) as validationstatus,
    version_id::number(18,0) as version_id,
    versionflag::varchar(100) as versionflag,
    versionname::varchar(100) as versionname,
    versionnumber::number(18,0) as versionnumber,
    effective_from::timestamp_ntz(9) as effective_from,
    effective_to::timestamp_ntz(9) as effective_to,
    active::varchar(2) as active,
    run_id::number(14,0) as run_id,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final