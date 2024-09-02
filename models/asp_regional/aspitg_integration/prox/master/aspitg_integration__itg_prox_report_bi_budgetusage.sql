with sdl_prox_report_bi_budgetusage as
(
    select * from {{source('aspsdl_raw', 'sdl_prox_report_bi_budgetusage')}}
),
final as
(
    select
    rowid::number(38,0) as rowid,
	budgetyear::number(38,0) as budgetyear,
	expensesubcategory::varchar(255) as expensesubcategory,
	budgettype::varchar(50) as budgettype,
	accountgroupcode::varchar(50) as accountgroupcode,
	accountgroupname::varchar(1000) as accountgroupname,
	brandcode::varchar(50) as brandcode,
	brandname::varchar(1000) as brandname,
	budgetamount::number(18,2) as budgetamount,
	usedbudget::number(18,2) as usedbudget,
	budgetbalance::number(18,2) as budgetbalance,
	ytdcommitment::number(18,2) as ytdcommitment,
	ytgcommitment::number(18,2) as ytgcommitment,
	commitment::number(18,2) as commitment,
	pendingamount::number(18,2) as pendingamount,
	paid::number(18,2) as paid,
	approvedpayment::number(18,2) as approvedpayment,
	pendingapprovalpayment::number(18,2) as pendingapprovalpayment,
	availabletopay::number(18,2) as availabletopay,
	draftamount::number(18,2) as draftamount,
	budgetowner::varchar(1000) as budgetowner,
	channel::varchar(100) as channel,
	subchannel::varchar(100) as subchannel,
	applicationid::varchar(50) as applicationid,
	filename::varchar(100) as filename,
	run_id::varchar(50) as run_id,
	crt_dttm::timestamp_ntz(9) as crt_dttm
    from sdl_prox_report_bi_budgetusage
)
select * from final