{{
    config(
        materialized="incremental",
        incremental_strategy= "append",    
        pre_hook= "{% if is_incremental() %}
        delete from {{this}}where salinvdate >= (select min(salinvdate) from {{ ref('inditg_integration__itg_keyaccountsales_wrk') }} where saleflag = 'DS') and  saleflag = 'DS';
        delete from {{this}}where salinvdate >= (select min(salinvdate) from {{ ref('inditg_integration__itg_keyaccountsales_wrk') }} where saleflag = 'IS') and  saleflag = 'IS';
        delete from {{this}}where salinvdate >= (select min(salinvdate) from {{ ref('inditg_integration__itg_keyaccountsales_wrk') }} where saleflag = 'IR') and  saleflag = 'IR';
        delete from {{this}}where salinvdate >= (select min(salinvdate) from {{ ref('inditg_integration__itg_keyaccountsales_wrk') }} where saleflag = 'WIS') and  saleflag = 'WIS';
        delete from {{this}}where salinvdate >= (select min(salinvdate) from {{ ref('inditg_integration__itg_keyaccountsales_wrk') }} where saleflag = 'WIR') and  saleflag = 'WIR';
        {% endif %}"
    )
}}
with source as 
(
    select * from {{ ref('inditg_integration__itg_keyaccountsales_wrk') }}
),
final as 
(
    select 
    keyaccountid::number(18,0) as keyaccountid,
	distributorcode::varchar(10) as distributorcode,
	salinvno::varchar(100) as salinvno,
	salinvdate::timestamp_ntz(9) as salinvdate,
	dlvsts::number(18,0) as dlvsts,
	rtrcode::varchar(50) as rtrcode,
	rtrnm::varchar(200) as rtrnm,
	productid::varchar(15) as productid,
	prdccode::varchar(50) as prdccode,
	productname::varchar(200) as productname,
	prdqty::number(18,0) as prdqty,
	prdsalerate::number(38,6) as prdsalerate,
	motherskuid::varchar(15) as motherskuid,
	motherskuname::varchar(50) as motherskuname,
	ctgtypid::varchar(15) as ctgtypid,
	ctgtypdsc::varchar(50) as ctgtypdsc,
	dlvsts2::number(18,0) as dlvsts2,
	prdtaxamt::number(38,6) as prdtaxamt,
	mnfid::number(18,0) as mnfid,
	prdschdiscamt::number(38,6) as prdschdiscamt,
	prddbdiscamt::number(38,6) as prddbdiscamt,
	salwdsamt::number(38,6) as salwdsamt,
	discount::number(38,6) as discount,
	schid::number(18,0) as schid,
	createddate::timestamp_ntz(9) as createddate,
	fromdate::timestamp_ntz(9) as fromdate,
	todate::timestamp_ntz(9) as todate,
	netrate::number(38,6) as netrate,
	saleflag::varchar(3) as saleflag,
	weekno::number(18,0) as weekno,
	confirmsales::varchar(1) as confirmsales,
	subtotal4::number(21,3) as subtotal4,
	current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final