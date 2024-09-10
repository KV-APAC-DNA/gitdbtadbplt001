with sdl_PROX_MD_PPGAllocation as(
    select * from {{ source('aspsdl_raw', 'sdl_prox_md_ppgallocation') }}
    where filename not in (
        select distinct file_name from {{source('aspwks_integration','TRATBL_sdl_prox_md_ppgallocation____null_test')}}
    )
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