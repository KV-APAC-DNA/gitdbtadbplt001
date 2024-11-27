{{
    config(
        pre_hook="{{build_itg_vn_mt_dksh_product_master()}}"
    )
}}


with sdl_mds_vn_distributor_products as (
    select * from {{ source('vnmsdl_raw', 'sdl_mds_vn_distributor_products') }}
),
 wks
	AS
		( 
                    --- case 1.a: PK present in ITG, record is updated, insert the old record from ITG keeping active as 'N'

			select 
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
				dksh.base_bundle as base_bundle,
                dksh.base_bundle_name,
                dksh.base_bundle_id,
				dksh.category as category,
                dksh.category_name,
                dksh.category_id,
				dksh.franchise as franchise,
                dksh.franchise_name,
                dksh.franchise_id,
				dksh.product_name,
				dksh.size as size,
                dksh.size_name,
                dksh.size_id,
				dksh.sub_brand as sub_brand,
                dksh.sub_brand_name,
                dksh.sub_brand_id,
				dksh.sub_category as sub_category,
                dksh.sub_category_name,
                dksh.sub_category_id,
                dksh.segmentation_code as segmentation_code,
                dksh.segmentation_name as segmentation_name,
                dksh.segmentation_id as segmentation_id,
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
							  WHERE sdl.lastchgdatetime::timestamp_ntz(9) != itg.lastchgdatetime
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
				dksh.base_bundle_code as base_bundle,
                dksh.base_bundle_name,
                dksh.base_bundle_id,
				dksh.category_code as category,
                dksh.category_name,
                dksh.category_id,
				dksh.franchise_code as franchise,
                dksh.franchise_name,
                dksh.franchise_id,
				dksh.product_name,
				dksh.size_code as size,
                dksh.size_name,
                dksh.size_id,
				dksh.sub_brand_code as sub_brand,
                dksh.sub_brand_name,
                dksh.sub_brand_id,
				dksh.sub_category_code as sub_category,
                dksh.sub_category_name,
                dksh.sub_category_id,
                dksh.segmentation_code as segmentation_code,
                dksh.segmentation_name as segmentation_name,
                dksh.segmentation_id as segmentation_id,
				current_timestamp() AS effective_from,
				NULL AS effective_to,
				'Y' AS active,
				null as run_id,
				dksh.enterdatetime,  --- taking from SDL enterdatetime
				current_timestamp() AS updt_dttm
				FROM (SELECT sdl.*, row_number() over (partition by sdl.code order by null) as rn
					  FROM sdl_mds_vn_distributor_products sdl,
						   {{this}} itg
					  WHERE sdl.lastchgdatetime::timestamp_ntz(9) != itg.lastchgdatetime
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
				dksh.base_bundle_code as base_bundle,
                dksh.base_bundle_name,
                dksh.base_bundle_id,
				dksh.category_code as category,
                dksh.category_name,
                dksh.category_id,
				dksh.franchise_code as franchise,
                dksh.franchise_name,
                dksh.franchise_id,
				dksh.product_name,
				dksh.size_code as size,
                dksh.size_name,
                dksh.size_id,
				dksh.sub_brand_code as sub_brand,
                dksh.sub_brand_name,
                dksh.sub_brand_id,
				dksh.sub_category_code as sub_category,
                dksh.sub_category_name,
                dksh.sub_category_id,
                dksh.segmentation_code as segmentation_code,
                dksh.segmentation_name as segmentation_name,
                dksh.segmentation_id as segmentation_id,
				dksh.effective_from,
				NULL AS effective_to,
				'Y' AS active,
				null as run_id,
				dksh.crtd_dttm,
				current_timestamp() AS updt_dttm
				FROM (SELECT sdl.*,itg.effective_from ,itg.crtd_dttm, row_number() over (partition by sdl.code order by null) as rn
					  FROM sdl_mds_vn_distributor_products sdl,
						   {{this}} itg
					  WHERE sdl.lastchgdatetime::timestamp_ntz(9) = itg.lastchgdatetime
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
				dksh.base_bundle as base_bundle,
                dksh.base_bundle_name,
                dksh.base_bundle_id,
				dksh.category as category,
                dksh.category_name,
                dksh.category_id,
				dksh.franchise as franchise,
                dksh.franchise_name,
                dksh.franchise_id,
				dksh.product_name,
				dksh.size as size,
                dksh.size_name,
                dksh.size_id,
				dksh.sub_brand as sub_brand,
                dksh.sub_brand_name,
                dksh.sub_brand_id,
				dksh.sub_category as sub_category,
                dksh.sub_category_name,
                dksh.sub_category_id,
                dksh.segmentation_code as segmentation_code,
                dksh.segmentation_name as segmentation_name,
                dksh.segmentation_id as segmentation_id,
				dksh.effective_from,
				dksh.effective_to,
				dksh.active,
				null as run_id,
				dksh.crtd_dttm,
				dksh.updt_dttm
				FROM (SELECT itg.*, row_number() over (partition by sdl.code order by null) as rn
					  FROM sdl_mds_vn_distributor_products sdl,
						   {{this}} itg
					  WHERE sdl.lastchgdatetime::timestamp_ntz(9) = itg.lastchgdatetime
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
				dksh.base_bundle_code as base_bundle,
                dksh.base_bundle_name,
                dksh.base_bundle_id,
				dksh.category_code as category,
                dksh.category_name,
                dksh.category_id,
				dksh.franchise_code as franchise,
                dksh.franchise_name,
                dksh.franchise_id,
				dksh.product_name,
				dksh.size_code as size,
                dksh.size_name,
                dksh.size_id,
				dksh.sub_brand_code as sub_brand,
                dksh.sub_brand_name,
                dksh.sub_brand_id,
				dksh.sub_category_code as sub_category,
                dksh.sub_category_name,
                dksh.sub_category_id,
                dksh.segmentation_code as segmentation_code,
                dksh.segmentation_name as segmentation_name,
                dksh.segmentation_id as segmentation_id,
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
					
					
		),
transformed as (
	SELECT *
	FROM wks

	UNION ALL

	SELECT *
				
	FROM {{this}} dksh
	WHERE dksh.code NOT IN (SELECT code FROM wks)
),
final as (
select
id::number(18,0) as id,
muid::varchar(36) as muid,
versionname::varchar(100) as versionname,
versionnumber::number(18,0) as versionnumber,
version_id::number(18,0) as version_id,
versionflag::varchar(100) as versionflag,
name::varchar(500) as name,
code::varchar(500) as code,
changetrackingmask::number(18,0) as changetrackingmask,
barcode::number(31,0) as barcode,
jnj_sap_code::number(31,0) as jnj_sap_code,
enterdatetime::timestamp_ntz(9) as enterdatetime,
enterusername::varchar(200) as enterusername,
enterversionnumber::number(18,0) as enterversionnumber,
lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
lastchgusername::varchar(200) as lastchgusername,
lastchgversionnumber::number(18,0) as lastchgversionnumber,
validationstatus::varchar(500) as validationstatus,
base_bundle::varchar(500) as base_bundle,
base_bundle_name::varchar(500) as base_bundle_name,
base_bundle_id::number(10,0) as base_bundle_id,
category::varchar(500) as category,
category_name::varchar(500) as category_name,
category_id::number(10,0) as category_id,
franchise::varchar(500) as franchise,
franchise_name::varchar(500) as franchise_name,
franchise_id::number(10,0) as franchise_id,
product_name::varchar(500) as product_name,
size::varchar(500) as size,
size_name::varchar(500) as size_name,
size_id::number(10,0) as size_id,
sub_brand::varchar(500) as sub_brand,
sub_brand_name::varchar(500) as sub_brand_name,
sub_brand_id::varchar(500) as sub_brand_id,
sub_category::varchar(500) as sub_category,
sub_category_name::varchar(500) as sub_category_name,
sub_category_id::number(10,0) as sub_category_id,
segmentation_code::varchar(500) as segmentation_code,
segmentation_name::varchar(500) as segmentation_name,
segmentation_id::number(10,0) as segmentation_id,
effective_from::timestamp_ntz(9) as effective_from,
effective_to::timestamp_ntz(9) as effective_to,
active::varchar(2) as active,
run_id::number(14,0) as run_id,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final