with sdl_mds_vn_distributor_products as (
    select * from DEV_DNA_LOAD.SNAPOSESDL_RAW.SDL_MDS_VN_DISTRIBUTOR_PRODUCTS
),
 wks
	AS
		(
			select --- case 1.a: PK present in ITG, record is updated, insert the old record from ITG keeping active as 'N'
				dksh.id,
				dksh.muid,
				dksh.versionname,
				dksh.versionnumber,
				dksh.version_id,
				dksh.versionflag,
				dksh.name,
				dksh.code,
				dksh.changetrackingmask,
				dksh.barcode,
				dksh.jnj_sap_code,
				dksh.enterdatetime,
				dksh.enterusername,
				dksh.enterversionnumber,
				dksh.lastchgdatetime,
				dksh.lastchgusername,
				dksh.lastchgversionnumber,
				dksh.validationstatus,
				dksh.base_bundle,
				dksh.category,
				dksh.franchise,
				dksh.product_name,
				dksh.size,
				dksh.sub_brand,
				dksh.sub_category,
				dksh.effective_from,
				CASE WHEN dksh.effective_to IS NULL
							THEN dateadd(DAY,-1,current_timestamp())
					ELSE dksh.effective_to
				END AS effective_to,
			   'N' AS active,
				dksh.run_id,
				dksh.crtd_dttm,
				dksh.updt_dttm
				FROM (SELECT itg.*, row_number() over (partition by sdl.code order by null) as rn
							  FROM sdl_mds_vn_distributor_products sdl,
								   {{this}} itg
							  WHERE sdl.lastchgdatetime != itg.lastchgdatetime
							  AND   sdl.code = itg.code) dksh
			  where dksh.rn = 1
			UNION ALL
			select --- case 1.b: PK is present in ITG, record is updated, insert new record from SDL keeping active as 'Y'
				dksh.id,
				dksh.muid,
				dksh.versionname,
				dksh.versionnumber,
				dksh.version_id,
				dksh.versionflag,
				dksh.name,
				dksh.code,
				dksh.changetrackingmask,
				dksh.barcode,
				dksh.jnj_sap_code,
				dksh.enterdatetime,
				dksh.enterusername,
				dksh.enterversionnumber,
				dksh.lastchgdatetime,
				dksh.lastchgusername,
				dksh.lastchgversionnumber,
				dksh.validationstatus,
				dksh.base_bundle,
				dksh.category,
				dksh.franchise,
				dksh.product_name,
				dksh.size,
				dksh.sub_brand,
				dksh.sub_category,
				current_timestamp() AS effective_from,
				NULL AS effective_to,
				'Y' AS active,
				null as run_id,
				dksh.enterdatetime,  --- taking from SDL enterdatetime
				current_timestamp() AS updt_dttm
				FROM (SELECT sdl.*, row_number() over (partition by sdl.code order by null) as rn
					  FROM sdl_mds_vn_distributor_products sdl,
						   {{this}} itg
					  WHERE sdl.lastchgdatetime != itg.lastchgdatetime
					  AND   sdl.code = itg.code
					  AND	itg.active = 'Y') dksh
			  where dksh.rn = 1
			UNION ALL
			select  --- case 2: PK present in ITG and active = 'Y', record is not updated in SDL, hence insert SDL record in ITG keeping active 'Y'
				dksh.id,
				dksh.muid,
				dksh.versionname,
				dksh.versionnumber,
				dksh.version_id,
				dksh.versionflag,
				dksh.name,
				dksh.code,
				dksh.changetrackingmask,
				dksh.barcode,
				dksh.jnj_sap_code,
				dksh.enterdatetime,
				dksh.enterusername,
				dksh.enterversionnumber,
				dksh.lastchgdatetime,
				dksh.lastchgusername,
				dksh.lastchgversionnumber,
				dksh.validationstatus,
				dksh.base_bundle,
				dksh.category,
				dksh.franchise,
				dksh.product_name,
				dksh.size,
				dksh.sub_brand,
				dksh.sub_category,
				dksh.effective_from,
				NULL AS effective_to,
				'Y' AS active,
				null as run_id,
				dksh.crtd_dttm,
				current_timestamp() AS updt_dttm
				FROM (SELECT sdl.*,itg.effective_from ,itg.crtd_dttm, row_number() over (partition by sdl.code order by null) as rn
					  FROM sdl_mds_vn_distributor_products sdl,
						   {{this}} itg
					  WHERE sdl.lastchgdatetime = itg.lastchgdatetime
					  AND   sdl.code = itg.code
					  AND	itg.active = 'Y'
					  ) dksh
				where dksh.rn = 1	
			UNION ALL
			select  --- case 3: PK present in ITG and active = 'N', record is not updated in SDL, hence insert ITG record in ITG keeping active 'N'
				dksh.id,
				dksh.muid,
				dksh.versionname,
				dksh.versionnumber,
				dksh.version_id,
				dksh.versionflag,
				dksh.name,
				dksh.code,
				dksh.changetrackingmask,
				dksh.barcode,
				dksh.jnj_sap_code,
				dksh.enterdatetime,
				dksh.enterusername,
				dksh.enterversionnumber,
				dksh.lastchgdatetime,
				dksh.lastchgusername,
				dksh.lastchgversionnumber,
				dksh.validationstatus,
				dksh.base_bundle,
				dksh.category,
				dksh.franchise,
				dksh.product_name,
				dksh.size,
				dksh.sub_brand,
				dksh.sub_category,
				dksh.effective_from,
				dksh.effective_to,
				dksh.active,
				null as run_id,
				dksh.crtd_dttm,
				dksh.updt_dttm
				FROM (SELECT itg.*, row_number() over (partition by sdl.code order by null) as rn
					  FROM sdl_mds_vn_distributor_products sdl,
						   {{this}} itg
					  WHERE sdl.lastchgdatetime = itg.lastchgdatetime
					  AND   sdl.code = itg.code
					  AND	itg.active = 'N') dksh
				where dksh.rn = 1
			UNION ALL
			select   --- case 4: PK not present in ITG, insert the whole new record in ITG from SDL keeping active 'Y'
				dksh.id,
				dksh.muid,
				dksh.versionname,
				dksh.versionnumber,
				dksh.version_id,
				dksh.versionflag,
				dksh.name,
				dksh.code,
				dksh.changetrackingmask,
				dksh.barcode,
				dksh.jnj_sap_code,
				dksh.enterdatetime,
				dksh.enterusername,
				dksh.enterversionnumber,
				dksh.lastchgdatetime,
				dksh.lastchgusername,
				dksh.lastchgversionnumber,
				dksh.validationstatus,
				dksh.base_bundle,
				dksh.category,
				dksh.franchise,
				dksh.product_name,
				dksh.size,
				dksh.sub_brand,
				dksh.sub_category,
				current_timestamp() AS effective_from,
				NULL AS effective_to,
				'Y' AS active,
				null as run_id,
				dksh.enterdatetime, -- taking from SDL enterdatetime
				current_timestamp() AS updt_dttm
				FROM (SELECT sdl.*, row_number() over (partition by sdl.code order by null) as rn
					  FROM sdl_mds_vn_distributor_products sdl
					  WHERE sdl.code NOT IN (SELECT code FROM {{this}})) dksh	
				where dksh.rn = 1	  
					
					
		)		
	SELECT *
	FROM wks

	UNION ALL

	SELECT *
				
	FROM {{this}} dksh
	WHERE dksh.code NOT IN (SELECT code FROM wks)