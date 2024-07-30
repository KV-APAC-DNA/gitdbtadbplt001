with wks_prox_report_bi_budgetusage_temp as
(
    select * from rg_wks.wks_prox_report_bi_budgetusage_temp
),
final as
(
    Select
    RowID,
	BudgetYear,
	ExpenseSubCategory,
	BudgetType,
	AccountGroupCode,
	AccountGroupName,
	BrandCode,
	BrandName,
	BudgetAmount,
	UsedBudget,
	BudgetBalance,
	YTDCommitment,
	YTGCommitment,
	Commitment,
	PendingAmount,
	Paid,
	ApprovedPayment,
	PendingApprovalPayment,
	AvailableToPay,
	DraftAmount,
	BudgetOwner,
	Channel,
	SubChannel,
	ApplicationID,
	'"+(String)globalMap.get("File_Name")+"' AS filename,
	'"+context.run_id+"' AS run_id,
	convert_timezone('Asia/Kolkata', sysdate) AS crt_dttm
    from wks_prox_report_bi_budgetusage_temp
)
select * from final