{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as (
    select * from {{ source('indsdl_raw', 'sdl_csl_scheme_header')}}
),
final as
 (
    select
    schcode::varchar(20) as schcode,
	schdsc::varchar(100) as schdsc,
	claimable::number(38,0) as claimable,
	clmamton::number(38,0) as clmamton,
	cmpschcode::varchar(20) as cmpschcode,
	schlevel_id::number(18,0) as schlevel_id,
	schtype::varchar(10) as schtype,
	flexisch::number(38,0) as flexisch,
	flexischtype::number(38,0) as flexischtype,
	combisch::number(38,0) as combisch,
	range::number(38,0) as range,
	prorata::number(38,0) as prorata,
	qps::varchar(10) as qps,
	qpsreset::number(38,0) as qpsreset,
	schvalidfrom::timestamp_ntz(9) as schvalidfrom,
	schvalidtill::timestamp_ntz(9) as schvalidtill,
	schstatus::number(38,0) as schstatus,
	budget::number(18,2) as budget,
	adjwindisponlyonce::number(38,0) as adjwindisponlyonce,
	purofevery::number(38,0) as purofevery,
	apyqpssch::number(38,0) as apyqpssch,
	setwindowdisp::number(38,0) as setwindowdisp,
	editscheme::number(38,0) as editscheme,
	schlvlmode::number(38,0) as schlvlmode,
	createduserid::number(18,0) as createduserid,
	createddate::timestamp_ntz(9) as createddate,
	modifiedby::varchar(40) as modifiedby,
	modifieddate::timestamp_ntz(9) as modifieddate,
	versionno::number(18,0) as versionno,
	serialno::varchar(100) as serialno,
	claimgrpcode::varchar(40) as claimgrpcode,
	fbm::number(18,0) as fbm,
	combitype::number(18,0) as combitype,
	allowuncheck::number(18,0) as allowuncheck,
	settlementtype::number(18,0) as settlementtype,
    consumerpromo::number(18,0) as consumerpromo,
	wdsbillscount::number(18,0) as wdsbillscount,
	wdscapamount::number(38,6) as wdscapamount,
	wdsminpurqty::number(18,0) as wdsminpurqty,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as crt_dttm, 
	file_name::varchar(50) as file_name
from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
     
 )
select * from final