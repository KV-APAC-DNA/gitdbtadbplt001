{{
    config(
        pre_hook="{{build_itg_vn_mt_customer_sales_organization()}}"
    )
}}

WITH sdl_mds_vn_customer_sales_organization as (
    select * from {{ source('vnmsdl_raw', 'sdl_mds_vn_customer_sales_organization') }}
),
union_1 as (
    select --- case 1.a: PK present in ITG, record is updated, insert the old record from ITG keeping active as 'N'
                sales_org.address,
                sales_org.code,
                sales_org.code_sr_pg,
                sales_org.code_ss,
                sales_org.customer_name,
                sales_org.district_name,
                sales_org.dksh_jnj,
                sales_org.id,
                sales_org.kam,
                sales_org.mtd_code,
                sales_org.mti_code,
                sales_org.name,
                sales_org.rom,
                sales_org.sales_man,
                sales_org.sales_supervisor,
                sales_org.status,
                sales_org.changetrackingmask,
                sales_org.enterdatetime,
                sales_org.enterusername,
                sales_org.enterversionnumber,
                sales_org.lastchgdatetime,
                sales_org.lastchgusername,
                sales_org.lastchgversionnumber,
                sales_org.muid,
                sales_org.validationstatus,
                sales_org.version_id,
                sales_org.versionflag,
                sales_org.versionname,
                sales_org.versionnumber,
                sales_org.effective_from,
                CASE WHEN sales_org.effective_to = to_date('2999-12-01','YYYY-MM-DD')
                            THEN to_date(extract(month from CURRENT_DATE) - 1 || '-'|| extract(year from CURRENT_DATE), 'MM-YYYY')
                    ELSE sales_org.effective_to
                END AS effective_to,
               'N' AS active,
                sales_org.run_id,
                sales_org.crtd_dttm,
                sales_org.updt_dttm
                from (select itg.*, row_number() over (partition by nvl(sdl.mti_code, sdl.mtd_code) order by null) as rn
                              from sdl_mds_vn_customer_sales_organization sdl,
                                   {{this}} itg
                              where sdl.lastchgdatetime != itg.lastchgdatetime
                              AND datediff(month, itg.effective_from, sdl.lastchgdatetime) > 0
                              AND   nvl(sdl.mti_code, sdl.mtd_code)=nvl(itg.mti_code, itg.mtd_code)
                              ) sales_org
              --where sales_org.rn = 1 --remove this as it will not take every thing
),
union_2 as (
    select --- case 1.b: PK is present in ITG, record is updated, insert new record from SDL keeping active as 'Y'
                sales_org.address,
                sales_org.code,
                sales_org.code_sr_pg,
                sales_org.code_ss,
                sales_org.customer_name,
                sales_org.district_name,
                sales_org.dksh_jnj,
                sales_org.id,
                sales_org.kam,
                sales_org.mtd_code,
                sales_org.mti_code,
                sales_org.name,
                sales_org.rom,
                sales_org.sales_man,
                sales_org.sales_supervisor,
                sales_org.status,
                sales_org.changetrackingmask,
                sales_org.enterdatetime,
                sales_org.enterusername,
                sales_org.enterversionnumber,
                sales_org.lastchgdatetime,
                sales_org.lastchgusername,
                sales_org.lastchgversionnumber,
                sales_org.muid,
                sales_org.validationstatus,
                sales_org.version_id,
                sales_org.versionflag,
                sales_org.versionname,
                sales_org.versionnumber,
                to_date(extract(month from CURRENT_DATE)|| '-'|| extract(year from CURRENT_DATE),'MM-YYYY') AS effective_from,
                to_date('12-2999','MM-YYYY') AS effective_to,
                'Y' AS active,
                null as run_id,
                sales_org.enterdatetime,  --- taking from SDL enterdatetime
                current_timestamp() AS updt_dttm
                from (select sdl.*, row_number() over (partition by nvl(sdl.mti_code, sdl.mtd_code) order by null) as rn
                      from sdl_mds_vn_customer_sales_organization sdl,
                           {{this}} itg
                      where sdl.lastchgdatetime != itg.lastchgdatetime
                       AND   nvl(sdl.mti_code, sdl.mtd_code)=nvl(itg.mti_code, itg.mtd_code)
                      AND   itg.active = 'Y') sales_org
              where sales_org.rn = 1
),
union_3 as (
    select  --- case 2: PK present in ITG and active = 'Y', record is not updated in SDL, hence insert SDL record in ITG keeping active 'Y'
                sales_org.address,
                sales_org.code,
                sales_org.code_sr_pg,
                sales_org.code_ss,
                sales_org.customer_name,
                sales_org.district_name,
                sales_org.dksh_jnj,
                sales_org.id,
                sales_org.kam,
                sales_org.mtd_code,
                sales_org.mti_code,
                sales_org.name,
                sales_org.rom,
                sales_org.sales_man,
                sales_org.sales_supervisor,
                sales_org.status,
                sales_org.changetrackingmask,
                sales_org.enterdatetime,
                sales_org.enterusername,
                sales_org.enterversionnumber,
                sales_org.lastchgdatetime,
                sales_org.lastchgusername,
                sales_org.lastchgversionnumber,
                sales_org.muid,
                sales_org.validationstatus,
                sales_org.version_id,
                sales_org.versionflag,
                sales_org.versionname,
                sales_org.versionnumber,
                sales_org.effective_from,
                to_date('12-2999','MM-YYYY') AS effective_to,
                'Y' AS active,
                null as run_id,
                sales_org.crtd_dttm,
                current_timestamp() AS updt_dttm
                from (select sdl.*,itg.effective_from ,itg.crtd_dttm, row_number() over (partition by nvl(sdl.mti_code, sdl.mtd_code) order by null) as rn
                      from sdl_mds_vn_customer_sales_organization sdl,
                           {{this}} itg
                      where sdl.lastchgdatetime = itg.lastchgdatetime
                      AND   nvl(sdl.mti_code, sdl.mtd_code)=nvl(itg.mti_code, itg.mtd_code)
                      AND   itg.active = 'Y'
                      ) sales_org
                where sales_org.rn = 1  
),
union_4 as (
select  --- case 3: PK present in ITG and active = 'N', record is not updated in SDL, hence insert ITG record in ITG keeping active 'N'
                sales_org.address,
                sales_org.code,
                sales_org.code_sr_pg,
                sales_org.code_ss,
                sales_org.customer_name,
                sales_org.district_name,
                sales_org.dksh_jnj,
                sales_org.id,
                sales_org.kam,
                sales_org.mtd_code,
                sales_org.mti_code,
                sales_org.name,
                sales_org.rom,
                sales_org.sales_man,
                sales_org.sales_supervisor,
                sales_org.status,
                sales_org.changetrackingmask,
                sales_org.enterdatetime,
                sales_org.enterusername,
                sales_org.enterversionnumber,
                sales_org.lastchgdatetime,
                sales_org.lastchgusername,
                sales_org.lastchgversionnumber,
                sales_org.muid,
                sales_org.validationstatus,
                sales_org.version_id,
                sales_org.versionflag,
                sales_org.versionname,
                sales_org.versionnumber,
                sales_org.effective_from,
                sales_org.effective_to,
                sales_org.active,
                null as run_id,
                sales_org.crtd_dttm,
                sales_org.updt_dttm
                from (select itg.*, row_number() over (partition by nvl(sdl.mti_code, sdl.mtd_code) order by null) as rn
                      from sdl_mds_vn_customer_sales_organization sdl,
                           {{this}} itg
                      where sdl.lastchgdatetime = itg.lastchgdatetime
                      AND   nvl(sdl.mti_code, sdl.mtd_code)=nvl(itg.mti_code, itg.mtd_code)
                      AND   itg.active = 'N') sales_org
                where sales_org.rn = 1
),
union_5 as (
select   --- case 4: PK not present in ITG, insert the whole new record in ITG from SDL keeping active 'Y'
                sales_org.address,
                sales_org.code,
                sales_org.code_sr_pg,
                sales_org.code_ss,
                sales_org.customer_name,
                sales_org.district_name,
                sales_org.dksh_jnj,
                sales_org.id,
                sales_org.kam,
                sales_org.mtd_code,
                sales_org.mti_code,
                sales_org.name,
                sales_org.rom,
                sales_org.sales_man,
                sales_org.sales_supervisor,
                sales_org.status,
                sales_org.changetrackingmask,
                sales_org.enterdatetime,
                sales_org.enterusername,
                sales_org.enterversionnumber,
                sales_org.lastchgdatetime,
                sales_org.lastchgusername,
                sales_org.lastchgversionnumber,
                sales_org.muid,
                sales_org.validationstatus,
                sales_org.version_id,
                sales_org.versionflag,
                sales_org.versionname,
                sales_org.versionnumber,
                to_date(extract(month from CURRENT_DATE)|| '-' || extract(year from CURRENT_DATE),'MM-YYYY') AS effective_from,
                to_date('12-2999','MM-YYYY') AS effective_to,
                'Y' AS active,
                null as run_id,
                sales_org.enterdatetime, -- taking from SDL enterdatetime
                current_timestamp() AS updt_dttm
                from (select sdl.*, row_number() over (partition by nvl(sdl.mti_code, sdl.mtd_code) order by null) as rn
                      from sdl_mds_vn_customer_sales_organization sdl where
                      nvl(sdl.mti_code, sdl.mtd_code) NOT IN (select nvl(mti_code, mtd_code) from {{this}})
                      ) sales_org   
                where sales_org.rn = 1    
),
 wks
    AS
        (
            select --- case 1.a: PK present in ITG, record is updated, insert the old record from ITG keeping active as 'N'
                sales_org.address,
                sales_org.code,
                sales_org.code_sr_pg,
                sales_org.code_ss,
                sales_org.customer_name,
                sales_org.district_name,
                sales_org.dksh_jnj,
                sales_org.id,
                sales_org.kam,
                sales_org.mtd_code,
                sales_org.mti_code,
                sales_org.name,
                sales_org.rom,
                sales_org.sales_man,
                sales_org.sales_supervisor,
                sales_org.status,
                sales_org.changetrackingmask,
                sales_org.enterdatetime,
                sales_org.enterusername,
                sales_org.enterversionnumber,
                sales_org.lastchgdatetime,
                sales_org.lastchgusername,
                sales_org.lastchgversionnumber,
                sales_org.muid,
                sales_org.validationstatus,
                sales_org.version_id,
                sales_org.versionflag,
                sales_org.versionname,
                sales_org.versionnumber,
                sales_org.effective_from,
                CASE WHEN sales_org.effective_to = to_date('2999-12-01','YYYY-MM-DD')
                            THEN to_date(extract(month from CURRENT_DATE )|| '-'|| extract(year from CURRENT_DATE), 'MM-YYYY')
                    ELSE sales_org.effective_to
                END AS effective_to,
               'N' AS active,
                sales_org.run_id,
                sales_org.crtd_dttm,
                sales_org.updt_dttm
                from (select itg.*, row_number() over (partition by nvl(sdl.mti_code, sdl.mtd_code) order by null) as rn
                              from sdl_mds_vn_customer_sales_organization sdl,
                                   {{this}} itg
                              where sdl.lastchgdatetime != itg.lastchgdatetime
                              AND datediff(month,itg.effective_from, sdl.lastchgdatetime) = 0
                              AND   nvl(sdl.mti_code, sdl.mtd_code)=nvl(itg.mti_code, itg.mtd_code)
                    ) sales_org
              --where sales_org.rn = 1  --remove this as it will not take every thing
            union all
            select * from union_1
            union all
            select * from union_2
            union all
            select * from union_3
            union all
            select * from union_4
            union all
            select * from union_5
                    
                    
        ),
mtd_code as (select mtd_code from wks),
mti_code as (select mti_code from wks),
transformed as (
    select *
    from wks
    union all
    select *          
    from {{this}} sales_org
    where sales_org.mtd_code NOT IN (mtd_code)
    OR sales_org.mti_code NOT IN (mti_code)
),
final as (
select
address::varchar(1200) as address,
code::varchar(500) as code,
code_sr_pg::varchar(60) as code_sr_pg,
code_ss::varchar(60) as code_ss,
customer_name::varchar(600) as customer_name,
district_name::varchar(200) as district_name,
dksh_jnj::varchar(200) as dksh_jnj,
id::number(18,0) as id,
kam::varchar(200) as kam,
mtd_code::number(31,0) as mtd_code,
mti_code::number(31,0) as mti_code,
name::varchar(500) as name,
rom::varchar(60) as rom,
sales_man::varchar(400) as sales_man,
sales_supervisor::varchar(200) as sales_supervisor,
status::varchar(60) as status,
changetrackingmask::number(18,0) as changetrackingmask,
enterdatetime::timestamp_ntz(9) as enterdatetime,
enterusername::varchar(200) as enterusername,
enterversionnumber::number(18,0) as enterversionnumber,
lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
lastchgusername::varchar(200) as lastchgusername,
lastchgversionnumber::number(18,0) as lastchgversionnumber,
muid::varchar(36) as muid,
validationstatus::varchar(500) as validationstatus,
version_id::number(18,0) as version_id,
versionflag::varchar(100) as versionflag,
versionname::varchar(100) as versionname,
versionnumber::number(18,0) as versionnumber,
effective_from::date as effective_from,
effective_to::date as effective_to,
active::varchar(2) as active,
run_id::number(14,0) as run_id,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final