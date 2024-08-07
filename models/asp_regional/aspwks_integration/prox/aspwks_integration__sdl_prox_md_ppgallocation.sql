with wks_PROX_MD_PPGAllocation as(
    select * from aspwks_integration.wks_PROX_MD_PPGAllocation
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

