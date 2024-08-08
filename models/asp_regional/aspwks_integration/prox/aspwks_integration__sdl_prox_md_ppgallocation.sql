with wks_prox_md_ppgallocation as(
    select * from {{ ref('aspwks_integration__wks_prox_md_ppgallocation') }}
),
final as(
    SELECT 
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
FROM wks_PROX_MD_PPGAllocation
)
select * from final

