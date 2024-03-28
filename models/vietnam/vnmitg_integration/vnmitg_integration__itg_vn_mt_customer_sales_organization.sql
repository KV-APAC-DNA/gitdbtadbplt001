WITH sdl_mds_vn_customer_sales_organization as (
    select * from DEV_DNA_LOAD.vnmsdl_raw.sdl_mds_vn_customer_sales_organization
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
                            THEN to_date(EXTRACT(month FROM CURRENT_DATE) - 1 || '-'|| EXTRACT(year FROM CURRENT_DATE), 'MM-YYYY')
                    ELSE sales_org.effective_to
                END AS effective_to,
               'N' AS active,
                sales_org.run_id,
                sales_org.crtd_dttm,
                sales_org.updt_dttm
                FROM (SELECT itg.*, row_number() over (partition by nvl(sdl.mti_code, sdl.mtd_code) order by null) as rn
                              FROM sdl_mds_vn_customer_sales_organization sdl,
                                   {{this}} itg
                              WHERE sdl.lastchgdatetime != itg.lastchgdatetime
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
                to_date(EXTRACT(month FROM CURRENT_DATE)|| '-'|| EXTRACT(year FROM CURRENT_DATE),'MM-YYYY') AS effective_from,
                to_date('12-2999','MM-YYYY') AS effective_to,
                'Y' AS active,
                null as run_id,
                sales_org.enterdatetime,  --- taking from SDL enterdatetime
                current_timestamp() AS updt_dttm
                FROM (SELECT sdl.*, row_number() over (partition by nvl(sdl.mti_code, sdl.mtd_code) order by null) as rn
                      FROM sdl_mds_vn_customer_sales_organization sdl,
                           {{this}} itg
                      WHERE sdl.lastchgdatetime != itg.lastchgdatetime
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
                FROM (SELECT sdl.*,itg.effective_from ,itg.crtd_dttm, row_number() over (partition by nvl(sdl.mti_code, sdl.mtd_code) order by null) as rn
                      FROM sdl_mds_vn_customer_sales_organization sdl,
                           {{this}} itg
                      WHERE sdl.lastchgdatetime = itg.lastchgdatetime
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
                FROM (SELECT itg.*, row_number() over (partition by nvl(sdl.mti_code, sdl.mtd_code) order by null) as rn
                      FROM sdl_mds_vn_customer_sales_organization sdl,
                           {{this}} itg
                      WHERE sdl.lastchgdatetime = itg.lastchgdatetime
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
                to_date(EXTRACT(month FROM CURRENT_DATE)|| '-' || EXTRACT(year FROM CURRENT_DATE),'MM-YYYY') AS effective_from,
                to_date('12-2999','MM-YYYY') AS effective_to,
                'Y' AS active,
                null as run_id,
                sales_org.enterdatetime, -- taking from SDL enterdatetime
                current_timestamp() AS updt_dttm
                FROM (SELECT sdl.*, row_number() over (partition by nvl(sdl.mti_code, sdl.mtd_code) order by null) as rn
                      FROM sdl_mds_vn_customer_sales_organization sdl where
                      nvl(sdl.mti_code, sdl.mtd_code) NOT IN (SELECT nvl(mti_code, mtd_code) FROM {{this}})
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
                            THEN to_date(EXTRACT(month FROM CURRENT_DATE )|| '-'|| EXTRACT(year FROM CURRENT_DATE), 'MM-YYYY')
                    ELSE sales_org.effective_to
                END AS effective_to,
               'N' AS active,
                sales_org.run_id,
                sales_org.crtd_dttm,
                sales_org.updt_dttm
                FROM (SELECT itg.*, row_number() over (partition by nvl(sdl.mti_code, sdl.mtd_code) order by null) as rn
                              FROM sdl_mds_vn_customer_sales_organization sdl,
                                   {{this}} itg
                              WHERE sdl.lastchgdatetime != itg.lastchgdatetime
                              AND datediff(month,itg.effective_from, sdl.lastchgdatetime) = 0
                              AND   nvl(sdl.mti_code, sdl.mtd_code)=nvl(itg.mti_code, itg.mtd_code)
                    ) sales_org
              --where sales_org.rn = 1  --remove this as it will not take every thing
            UNION ALL
            select * from union_1
            UNION ALL
            select * from union_2
            UNION ALL
            select * from union_3
            UNION ALL
            select * from union_4
            UNION ALL
            select * from union_5
                    
                    
        ),
mtd_code as (SELECT mtd_code FROM wks),
mti_code as (SELECT mti_code FROM wks)

    SELECT *
    FROM wks
    UNION ALL
    SELECT *          
    FROM {{this}} sales_org
    WHERE sales_org.mtd_code NOT IN (mtd_code)
    OR sales_org.mti_code NOT IN (mti_code)