with source as 
(
    select * from {{source('phlsdl_raw','sdl_mds_ph_hce_customer_master')}}
),
transformed as 
(
    select 

   ID ,
	MUID ,
	VERSIONNAME ,
	VERSIONNUMBER ,
	VERSION_ID ,
	VERSIONFLAG ,
	NAME,
	CODE,
	CHANGETRACKINGMASK ,
	DIRECT_MANAGER_CODE ,
	DIRECT_MANAGER_NAME,
	DIRECT_MANAGER_ID ,
	TERRITORY_CODE_CODE ,
	TERRITORY_CODE_NAME ,
	TERRITORY_CODE_ID ,
	ENTERDATETIME ,
	ENTERUSERNAME ,
	ENTERVERSIONNUMBER ,
	LASTCHGDATETIME ,
	LASTCHGUSERNAME ,
	LASTCHGVERSIONNUMBER,
	VALIDATIONSTATUS ,
    current_timestamp as crtdtm
    from source 
),
final as 
(
    select 
    * from transformed
)
select * from final