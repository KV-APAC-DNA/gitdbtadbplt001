{{
    config(
        pre_hook="{{built_itg_mds_in_sv_winculum_master()}}"
    )
}}
with sdl_mds_in_sv_winculum_master as(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_sv_winculum_master') }}
),
wks AS (
			SELECT --- case 1.a: PK present in ITG, record is updated, insert the old record from ITG keeping active as 'N'
				winm.customercode,
				winm.customername,
				winm.regioncode,
				winm.zonecode,
				winm.territorycode,
				winm.statecode,
				winm.towncode,
				winm.psnonps,
				winm.isactive,
				winm.wholesalercode,
				winm.parentcustomercode,
				winm.isdirectacct,
				winm.abicode,
				winm.distributorsapid,
				winm.name,
				winm.code,
				winm.changetrackingmask,
				winm.validationstatus,
				winm.version_id,
				winm.versionflag,
				winm.versionname,
				winm.versionnumber,
				winm.lastchgdatetime,
				winm.lastchgusername,
				winm.lastchgversionnumber,
				winm.effective_from,
				CASE 
					WHEN winm.effective_to IS NULL
						THEN dateadd (DAY,-1, current_timestamp())
					ELSE winm.effective_to
					END AS effective_to,
				'N' AS active,
				winm.run_id,
				winm.crtd_dttm,
				winm.updt_dttm
			FROM (
				SELECT itg.*,
					ROW_NUMBER() OVER (
						PARTITION BY sdl.wholesalercode,
						sdl.distributorsapid order by null
						) AS rn
				FROM sdl_mds_in_sv_winculum_master sdl,
					{{this}} itg
				WHERE sdl.lastchgdatetime != itg.lastchgdatetime
					AND sdl.wholesalercode = itg.wholesalercode
					AND sdl.distributorsapid = itg.distributorsapid
				) winm
			WHERE winm.rn = 1
			
			UNION ALL
			
			SELECT --- case 1.b: PK is present in ITG, record is updated, insert new record from SDL keeping active as 'Y'
				winm.customercode,
				winm.customername,
				winm.regioncode,
				winm.zonecode,
				winm.territorycode,
				winm.statecode,
				winm.towncode,
				winm.psnonps,
				winm.isactive,
				winm.wholesalercode,
				winm.parentcustomercode,
				winm.isdirectacct,
				winm.abicode,
				winm.distributorsapid,
				winm.name,
				winm.code,
				winm.changetrackingmask,
				winm.validationstatus,
				winm.version_id,
				winm.versionflag,
				winm.versionname,
				winm.versionnumber,
				winm.lastchgdatetime,
				winm.lastchgusername,
				winm.lastchgversionnumber,
				current_timestamp() AS effective_from,
				NULL AS effective_to,
				'Y' AS active,
				NULL,
				winm.enterdatetime,
				--- taking from SDL enterdatetime
				current_timestamp() AS updt_dttm
			FROM (
				SELECT sdl.*,
					ROW_NUMBER() OVER (
						PARTITION BY sdl.wholesalercode,
						sdl.distributorsapid order by null
						) AS rn
				FROM sdl_mds_in_sv_winculum_master sdl,
					{{this}} itg
				WHERE sdl.lastchgdatetime != itg.lastchgdatetime
					AND sdl.wholesalercode = itg.wholesalercode
					AND sdl.distributorsapid = itg.distributorsapid
					AND itg.active = 'Y'
				) winm
			WHERE winm.rn = 1
			
			UNION ALL
			
			SELECT --- case 2: PK present in ITG and active = 'Y', record is not updated in SDL, hence insert SDL record in ITG keeping active 'Y'
				winm.customercode,
				winm.customername,
				winm.regioncode,
				winm.zonecode,
				winm.territorycode,
				winm.statecode,
				winm.towncode,
				winm.psnonps,
				winm.isactive,
				winm.wholesalercode,
				winm.parentcustomercode,
				winm.isdirectacct,
				winm.abicode,
				winm.distributorsapid,
				winm.name,
				winm.code,
				winm.changetrackingmask,
				winm.validationstatus,
				winm.version_id,
				winm.versionflag,
				winm.versionname,
				winm.versionnumber,
				winm.lastchgdatetime,
				winm.lastchgusername,
				winm.lastchgversionnumber,
				winm.effective_from,
				NULL AS effective_to,
				'Y' AS active,
				NULL,
				winm.crtd_dttm,
				current_timestamp() AS updt_dttm
			FROM (
				SELECT sdl.*,
					itg.effective_from,
					itg.crtd_dttm,
					ROW_NUMBER() OVER (
						PARTITION BY sdl.wholesalercode,
						sdl.distributorsapid order by null
						) AS rn
				FROM sdl_mds_in_sv_winculum_master sdl,
					{{this}} itg
				WHERE sdl.lastchgdatetime = itg.lastchgdatetime
					AND sdl.wholesalercode = itg.wholesalercode
					AND sdl.distributorsapid = itg.distributorsapid
					AND itg.active = 'Y'
				) winm
			WHERE winm.rn = 1
			
			UNION ALL
			
			SELECT --- case 3: PK present in ITG and active = 'N', record is not updated in SDL, hence insert ITG record in ITG keeping active 'N'
				winm.customercode,
				winm.customername,
				winm.regioncode,
				winm.zonecode,
				winm.territorycode,
				winm.statecode,
				winm.towncode,
				winm.psnonps,
				winm.isactive,
				winm.wholesalercode,
				winm.parentcustomercode,
				winm.isdirectacct,
				winm.abicode,
				winm.distributorsapid,
				winm.name,
				winm.code,
				winm.changetrackingmask,
				winm.validationstatus,
				winm.version_id,
				winm.versionflag,
				winm.versionname,
				winm.versionnumber,
				winm.lastchgdatetime,
				winm.lastchgusername,
				winm.lastchgversionnumber,
				winm.effective_from,
				winm.effective_to,
				winm.active,
				NULL,
				winm.crtd_dttm,
				winm.updt_dttm
			FROM (
				SELECT itg.*,
					ROW_NUMBER() OVER (
						PARTITION BY sdl.wholesalercode,
						sdl.distributorsapid order by null
						) AS rn
				FROM sdl_mds_in_sv_winculum_master sdl,
					{{this}} itg
				WHERE sdl.lastchgdatetime = itg.lastchgdatetime
					AND sdl.wholesalercode = itg.wholesalercode
					AND sdl.distributorsapid = itg.distributorsapid
					AND itg.active = 'N'
				) winm
			WHERE winm.rn = 1
			
			UNION ALL
			
			SELECT --- case 4: PK not present in ITG, insert the whole new record in ITG from SDL keeping active 'Y'
				winm.customercode,
				winm.customername,
				winm.regioncode,
				winm.zonecode,
				winm.territorycode,
				winm.statecode,
				winm.towncode,
				winm.psnonps,
				winm.isactive,
				winm.wholesalercode,
				winm.parentcustomercode,
				winm.isdirectacct,
				winm.abicode,
				winm.distributorsapid,
				winm.name,
				winm.code,
				winm.changetrackingmask,
				winm.validationstatus,
				winm.version_id,
				winm.versionflag,
				winm.versionname,
				winm.versionnumber,
				winm.lastchgdatetime,
				winm.lastchgusername,
				winm.lastchgversionnumber,
				current_timestamp() AS effective_from,
				NULL AS effective_to,
				'Y' AS active,
				NULL,
				winm.enterdatetime,
				-- taking from SDL enterdatetime
				current_timestamp() AS updt_dttm
			FROM (
				SELECT sdl.*,
					ROW_NUMBER() OVER (
						PARTITION BY sdl.wholesalercode,
						sdl.distributorsapid order by null
						) AS rn
				FROM sdl_mds_in_sv_winculum_master sdl
				WHERE (
						sdl.wholesalercode,
						sdl.distributorsapid
						) NOT IN (
						SELECT wholesalercode,
							distributorsapid
						FROM {{this}}
						)
				) winm
			WHERE winm.rn = 1
),
transformed as(
    SELECT * FROM wks
	UNION ALL    
    SELECT * FROM {{this}} winm WHERE (
        winm.wholesalercode,
        winm.distributorsapid
        ) NOT IN (
        SELECT wholesalercode,
            distributorsapid
        FROM wks
			)
),
final as(
    select
        customercode::varchar(200) as customercode,
        customername::varchar(200) as customername,
        regioncode::varchar(200) as regioncode,
        zonecode::varchar(200) as zonecode,
        territorycode::varchar(200) as territorycode,
        statecode::varchar(200) as statecode,
        towncode::varchar(200) as towncode,
        psnonps::varchar(200) as psnonps,
        isactive::varchar(200) as isactive,
        wholesalercode::varchar(200) as wholesalercode,
        parentcustomercode::varchar(200) as parentcustomercode,
        isdirectacct::varchar(200) as isdirectacct,
        abicode::varchar(200) as abicode,
        distributorsapid::varchar(200) as distributorsapid,
        name::varchar(500) as name,
        code::varchar(500) as code,
        changetrackingmask::number(18,0) as changetrackingmask,
        validationstatus::varchar(500) as validationstatus,
        version_id::number(18,0) as version_id,
        versionflag::varchar(100) as versionflag,
        versionname::varchar(100) as versionname,
        versionnumber::number(18,0) as versionnumber,
        lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
        lastchgusername::varchar(200) as lastchgusername,
        lastchgversionnumber::number(18,0) as lastchgversionnumber,
        effective_from::timestamp_ntz(9) as effective_from,
        effective_to::timestamp_ntz(9) as effective_to,
        active::varchar(2) as active,
        run_id::number(18,0) as run_id,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final