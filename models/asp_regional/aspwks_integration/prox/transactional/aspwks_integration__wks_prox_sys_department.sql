with wks_prox_sys_department_temp as
(
    select * from rg_wks.wks_prox_sys_department_temp
),
final as
(
    Select
    DeptID,
	DeptCode,
	DeptName,
	DeptNameEN,
	ParentID,
	LevelID,
	SortID,
	STATUS,
	ApplicationID,
	Col1,
	Col2,
	Col3,
	Version,
	OrgPath,
	OrgType,
	CostCenter,
	CostCenterName,
	LocationCode,
	OrgCode,
	CreateUserID,
	CreateTime,
	LastModifyUserID,
	LastModifyTime,
	'"+(String)globalMap.get("File_Name")+"' AS filename,
	'"+context.run_id+"' AS run_id,
	convert_timezone('Asia/Kolkata', sysdate) AS crt_dttm
    from wks_prox_sys_department_temp
)
select * from final