with sdl_PROX_MD_PPGAllocation as(
    select * from {{ source('aspsdl_raw', 'sdl_prox_md_ppgallocation') }}
),
final as
(SELECT 
	ID 
	,PPGCode
	,MaterialCode
	,AllocationRate
	,Status
	,ApplicationID
	,CreateTime 
	,CreateUserID
	,LastModifyTime 
	,LastModifyUserID
	,BatchNo,
       filename,
       run_id,
       crt_dttm
FROM sdl_PROX_MD_PPGAllocation)
select * from final