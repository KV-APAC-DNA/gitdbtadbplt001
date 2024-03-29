with sdl_mds_vn_distributor_customers as (
    select * from DEV_DNA_LOAD.SNAPOSESDL_RAW.SDL_MDS_VN_DISTRIBUTOR_CUSTOMERS
),
wks
	AS
		(
			select --- case 1.a: PK present in ITG, record is updated, insert the old record from ITG keeping active as 'N'
				dksh.account_code,
				dksh.account_id,
				dksh.account_name,
				dksh.address,
				dksh.changetrackingmask,
				dksh.code,
				dksh.enterdatetime,
				dksh.enterusername,
				dksh.enterversionnumber,
				dksh.group_account_code,
				dksh.group_account_id,
				dksh.group_account_name,
				dksh.id,
				dksh.lastchgdatetime,
				dksh.lastchgusername,
				dksh.lastchgversionnumber,
				dksh.muid,
				dksh.name,
				dksh.province_code,
				dksh.province_id,
				dksh.retail_environment,
				dksh.province_name,
				dksh.region_code,
				dksh.region_id,
				dksh.region_name,
				dksh.sub_channel_code,
				dksh.sub_channel_id,
				dksh.sub_channel_name,
				dksh.validationstatus,
				dksh.version_id,
				dksh.versionflag,
				dksh.versionname,
				dksh.versionnumber,
				dksh.ten_st_ddp,
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
							  FROM sdl_mds_vn_distributor_customers sdl,
								   DEV_DNA_CORE.nnaras01_workspace.vnmitg_integration__itg_vn_mt_dksh_cust_master itg
							  WHERE sdl.lastchgdatetime != itg.lastchgdatetime
							  AND   sdl.code = itg.code) dksh
			  where dksh.rn = 1
			UNION ALL
			select --- case 1.b: PK is present in ITG, record is updated, insert new record from SDL keeping active as 'Y'
				dksh.account_code,
				dksh.account_id,
				dksh.account_name,
				dksh.address,
				dksh.changetrackingmask,
				dksh.code,
				dksh.enterdatetime,
				dksh.enterusername,
				dksh.enterversionnumber,
				dksh.group_account_code,
				dksh.group_account_id,
				dksh.group_account_name,
				dksh.id,
				dksh.lastchgdatetime,
				dksh.lastchgusername,
				dksh.lastchgversionnumber,
				dksh.muid,
				dksh.name,
				dksh.province_code,
				dksh.province_id,
				dksh.retail_environment,
				dksh.province_name,
				dksh.region_code,
				dksh.region_id,
				dksh.region_name,
				dksh.sub_channel_code,
				dksh.sub_channel_id,
				dksh.sub_channel_name,
				dksh.validationstatus,
				dksh.version_id,
				dksh.versionflag,
				dksh.versionname,
				dksh.versionnumber,
				dksh.ten_st_ddp,
				current_timestamp() AS effective_from,
				NULL AS effective_to,
				'Y' AS active,
				null as run_id,
				dksh.enterdatetime,  --- taking from SDL enterdatetime
				current_timestamp() AS updt_dttm
				FROM (SELECT sdl.*, row_number() over (partition by sdl.code order by null) as rn
					  FROM sdl_mds_vn_distributor_customers sdl,
						   DEV_DNA_CORE.nnaras01_workspace.vnmitg_integration__itg_vn_mt_dksh_cust_master itg
					  WHERE sdl.lastchgdatetime != itg.lastchgdatetime
					  AND   sdl.code = itg.code
					  AND	itg.active = 'Y') dksh
			  where dksh.rn = 1
			UNION ALL
			select  --- case 2: PK present in ITG and active = 'Y', record is not updated in SDL, hence insert SDL record in ITG keeping active 'Y'
				dksh.account_code,
				dksh.account_id,
				dksh.account_name,
				dksh.address,
				dksh.changetrackingmask,
				dksh.code,
				dksh.enterdatetime,
				dksh.enterusername,
				dksh.enterversionnumber,
				dksh.group_account_code,
				dksh.group_account_id,
				dksh.group_account_name,
				dksh.id,
				dksh.lastchgdatetime,
				dksh.lastchgusername,
				dksh.lastchgversionnumber,
				dksh.muid,
				dksh.name,
				dksh.province_code,
				dksh.province_id,
				dksh.retail_environment,
				dksh.province_name,
				dksh.region_code,
				dksh.region_id,
				dksh.region_name,
				dksh.sub_channel_code,
				dksh.sub_channel_id,
				dksh.sub_channel_name,
				dksh.validationstatus,
				dksh.version_id,
				dksh.versionflag,
				dksh.versionname,
				dksh.versionnumber,
				dksh.ten_st_ddp,
				dksh.effective_from,
				NULL AS effective_to,
				'Y' AS active,
				null as run_id,
				dksh.crtd_dttm,
				current_timestamp() AS updt_dttm
				FROM (SELECT sdl.*,itg.effective_from ,itg.crtd_dttm, row_number() over (partition by sdl.code order by null) as rn
					  FROM sdl_mds_vn_distributor_customers sdl,
						   DEV_DNA_CORE.nnaras01_workspace.vnmitg_integration__itg_vn_mt_dksh_cust_master itg
					  WHERE sdl.lastchgdatetime = itg.lastchgdatetime
					  AND   sdl.code = itg.code
					  AND	itg.active = 'Y'
					  ) dksh
				where dksh.rn = 1	
			UNION ALL
			select  --- case 3: PK present in ITG and active = 'N', record is not updated in SDL, hence insert ITG record in ITG keeping active 'N'
				dksh.account_code,
				dksh.account_id,
				dksh.account_name,
				dksh.address,
				dksh.changetrackingmask,
				dksh.code,
				dksh.enterdatetime,
				dksh.enterusername,
				dksh.enterversionnumber,
				dksh.group_account_code,
				dksh.group_account_id,
				dksh.group_account_name,
				dksh.id,
				dksh.lastchgdatetime,
				dksh.lastchgusername,
				dksh.lastchgversionnumber,
				dksh.muid,
				dksh.name,
				dksh.province_code,
				dksh.province_id,
				dksh.retail_environment,
				dksh.province_name,
				dksh.region_code,
				dksh.region_id,
				dksh.region_name,
				dksh.sub_channel_code,
				dksh.sub_channel_id,
				dksh.sub_channel_name,
				dksh.validationstatus,
				dksh.version_id,
				dksh.versionflag,
				dksh.versionname,
				dksh.versionnumber,
				dksh.ten_st_ddp,
				dksh.effective_from,
				dksh.effective_to,
				dksh.active,
				null as run_id,
				dksh.crtd_dttm,
				dksh.updt_dttm
				FROM (SELECT itg.*, row_number() over (partition by sdl.code order by null) as rn
					  FROM sdl_mds_vn_distributor_customers sdl,
						   DEV_DNA_CORE.nnaras01_workspace.vnmitg_integration__itg_vn_mt_dksh_cust_master itg
					  WHERE sdl.lastchgdatetime = itg.lastchgdatetime
					  AND   sdl.code = itg.code
					  AND	itg.active = 'N') dksh
				where dksh.rn = 1
			UNION ALL
			select   --- case 4: PK not present in ITG, insert the whole new record in ITG from SDL keeping active 'Y'
				dksh.account_code,
				dksh.account_id,
				dksh.account_name,
				dksh.address,
				dksh.changetrackingmask,
				dksh.code,
				dksh.enterdatetime,
				dksh.enterusername,
				dksh.enterversionnumber,
				dksh.group_account_code,
				dksh.group_account_id,
				dksh.group_account_name,
				dksh.id,
				dksh.lastchgdatetime,
				dksh.lastchgusername,
				dksh.lastchgversionnumber,
				dksh.muid,
				dksh.name,
				dksh.province_code,
				dksh.province_id,
				dksh.retail_environment,
				dksh.province_name,
				dksh.region_code,
				dksh.region_id,
				dksh.region_name,
				dksh.sub_channel_code,
				dksh.sub_channel_id,
				dksh.sub_channel_name,
				dksh.validationstatus,
				dksh.version_id,
				dksh.versionflag,
				dksh.versionname,
				dksh.versionnumber,
				dksh.ten_st_ddp,
				current_timestamp() AS effective_from,
				NULL AS effective_to,
				'Y' AS active,
				null as run_id,
				dksh.enterdatetime, -- taking from SDL enterdatetime
				current_timestamp() AS updt_dttm
				FROM (SELECT sdl.*, row_number() over (partition by sdl.code order by null) as rn
					  FROM sdl_mds_vn_distributor_customers sdl
					  WHERE sdl.code NOT IN (SELECT code FROM DEV_DNA_CORE.nnaras01_workspace.vnmitg_integration__itg_vn_mt_dksh_cust_master)) dksh	
				where dksh.rn = 1	  
					
					
		),
transformed as (		
	SELECT *
	FROM wks

	UNION ALL

	SELECT *
				
	FROM DEV_DNA_CORE.nnaras01_workspace.vnmitg_integration__itg_vn_mt_dksh_cust_master dksh
	WHERE dksh.code NOT IN (SELECT code FROM wks)
),
final as (
select 
account_code::varchar(500) as account_code,
account_id::number(18,0) as account_id,
account_name::varchar(500) as account_name,
address::varchar(200) as address,
changetrackingmask::number(18,0) as changetrackingmask,
code::varchar(500) as code,
enterdatetime::timestamp_ntz(9) as enterdatetime,
enterusername::varchar(200) as enterusername,
enterversionnumber::number(18,0) as enterversionnumber,
group_account_code::varchar(500) as group_account_code,
group_account_id::number(18,0) as group_account_id,
group_account_name::varchar(500) as group_account_name,
id::number(18,0) as id,
lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
lastchgusername::varchar(200) as lastchgusername,
lastchgversionnumber::number(18,0) as lastchgversionnumber,
muid::varchar(36) as muid,
name::varchar(500) as name,
province_code::varchar(500) as province_code,
province_id::number(18,0) as province_id,
retail_environment::varchar(200) as retail_environment,
province_name::varchar(500) as province_name,
region_code::varchar(500) as region_code,
region_id::number(18,0) as region_id,
region_name::varchar(500) as region_name,
sub_channel_code::varchar(500) as sub_channel_code,
sub_channel_id::number(18,0) as sub_channel_id,
sub_channel_name::varchar(500) as sub_channel_name,
validationstatus::varchar(500) as validationstatus,
version_id::number(18,0) as version_id,
versionflag::varchar(100) as versionflag,
versionname::varchar(100) as versionname,
versionnumber::number(18,0) as versionnumber,
ten_st_ddp::varchar(200) as ten_st_ddp,
effective_from::timestamp_ntz(9) as effective_from,
effective_to::timestamp_ntz(9) as effective_to,
active::varchar(2) as active,
run_id::number(14,0) as run_id,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final