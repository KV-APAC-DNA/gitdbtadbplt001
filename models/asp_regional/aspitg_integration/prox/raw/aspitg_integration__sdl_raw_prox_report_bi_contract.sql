{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}
with source as
(
    select * from {{source('aspsdl_raw', 'sdl_prox_report_bi_contract')}}
    where filename not in (
        select distinct file_name from {{source('aspwks_integration','TRATBL_sdl_prox_report_bi_contract__null_test')}}
    )
),
final as
 (
    select
    rowid::number(38,0) as rowid,
	budgetyear::varchar(50) as budgetyear,
	expensesubcategory::varchar(50) as expensesubcategory,
	accountgroupcode::varchar(50) as accountgroupcode,
	accountgroupname::varchar(1000) as accountgroupname,
	customertype::varchar(255) as customertype,
	hqsoldtocode::varchar(50) as hqsoldtocode,
	hqsoldtoname::varchar(1000) as hqsoldtoname,
	contractno::varchar(50) as contractno,
	contracttempno::varchar(50) as contracttempno,
	contracttitle::varchar(1000) as contracttitle,
	themecode::varchar(50) as themecode,
	themename::varchar(1000) as themename,
	objective::varchar(255) as objective,
	feespaid::varchar(255) as feespaid,
	contractrevenuerecognitionstartdate::timestamp_ntz(9) as contractrevenuerecognitionstartdate,
	contractrevenuerecognitionenddate::timestamp_ntz(9) as contractrevenuerecognitionenddate,
	contractpromoexecutionstartdate::timestamp_ntz(9) as contractpromoexecutionstartdate,
	contractpromoexecutionenddate::timestamp_ntz(9) as contractpromoexecutionenddate,
	contractcreatedate::timestamp_ntz(9) as contractcreatedate,
	contractapprovaldate::timestamp_ntz(9) as contractapprovaldate,
	contractstatus::varchar(255) as contractstatus,
	dateback::varchar(50) as dateback,
	datebackreason::varchar(1000) as datebackreason,
	expenseid::varchar(50) as expenseid,
	customercode::varchar(50) as customercode,
	customername::varchar(1000) as customername,
	rebateno::varchar(50) as rebateno,
	rebateactivityname::varchar(200) as rebateactivityname,
	customercontractno::varchar(500) as customercontractno,
	expenserevenuerecognitionstartdate::timestamp_ntz(9) as expenserevenuerecognitionstartdate,
	expenserevenuerecognitionenddate::timestamp_ntz(9) as expenserevenuerecognitionenddate,
	expensepromoexecutionstartdate::timestamp_ntz(9) as expensepromoexecutionstartdate,
	expensepromoexecutionenddate::timestamp_ntz(9) as expensepromoexecutionenddate,
	expenseclosed::varchar(50) as expenseclosed,
	closeuser::varchar(255) as closeuser,
	closetime::timestamp_ntz(9) as closetime,
	contracttype::varchar(255) as contracttype,
	conditiontype::varchar(50) as conditiontype,
	glaccount::varchar(50) as glaccount,
	expensetypename::varchar(1000) as expensetypename,
	profitcenter::varchar(1000) as profitcenter,
	ppgcode::varchar(50) as ppgcode,
	ppgname::varchar(1000) as ppgname,
	rate::number(18,5) as rate,
	materialcode::varchar(50) as materialcode,
	materialname::varchar(1000) as materialname,
	budgetowner::varchar(1000) as budgetowner,
	channel::varchar(50) as channel,
	rtmchannel::varchar(255) as rtmchannel,
	paymentmethod::varchar(255) as paymentmethod,
	expenseamount::number(18,2) as expenseamount,
	commitmentamount::number(18,2) as commitmentamount,
	accrualreleaseamountwithouttax::number(18,2) as accrualreleaseamountwithouttax,
	closeamount::number(18,2) as closeamount,
	paidamount::number(18,2) as paidamount,
	approvedamount::number(18,2) as approvedamount,
	unpaidamount::number(18,2) as unpaidamount,
	availabletopay::number(18,2) as availabletopay,
	agingdays::number(38,0) as agingdays,
	aging::varchar(255) as aging,
	applicant::varchar(255) as applicant,
	contractdescription::varchar(4000) as contractdescription,
	documentlink::varchar(4000) as documentlink,
	withattachment::varchar(50) as withattachment,
	dtsmarkuprate::number(18,5) as dtsmarkuprate,
	applicationid::varchar(50) as applicationid,
	filename::varchar(100) as filename,
	run_id::varchar(50) as run_id,
	crt_dttm::timestamp_ntz(9) as crt_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}     
)
select * from final