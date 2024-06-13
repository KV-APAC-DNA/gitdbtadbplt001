{{
    config(
        pre_hook="{{built_itg_rrl_retailermaster()}}"  
    )
}}
with source as
(
    select * from {{ source('inditg_raw', 'itg_rrl_retailermaster') }}
),
transformed as(
select
    src.retailercode::varchar(50) as retailercode,
	src.retailername src_retailername::varchar(100) as src_retailername,
	tgt.retailername tgt_retailername::varchar(100) as tgt_retailername,
	routecode::varchar(25) as routecode,
	retailerclasscode::varchar(50) as retailerclasscode,
	src.villagecode::varchar(50) as villagecode,
	src.rsdcode::varchar(50) as rsdcode,
	src.distributorcode::varchar(50) as distributorcode,
	foodlicenseno::varchar(50) as foodlicenseno,
	druglicenseno::varchar(50) as druglicenseno,
	address::varchar(100) as address,
	phone::varchar(15) as phone,
	mobile::varchar(15) as mobile,
	prcontact::varchar(50) as prcontact,
	seccontact::varchar(50) as seccontact,
	creditlimit::number(18,0) as creditlimit,
	creditperiod::number(18,0) as creditperiod,
	invoicelimit::varchar(30) as invoicelimit,
	isapproved::varchar(1) as isapproved,
	isactive::boolean as isactive,
	src.rsrcode::varchar(100) as rsrcode,
	drugvaliditydate::timestamp_ntz(9) as drugvaliditydate,
	fssaivaliditydate::timestamp_ntz(9) as fssaivaliditydate,
	src.displaystatus src_displaystatus::varchar(20) as src_displaystatus,
	tgt.displaystatus tgt_displaystatus::varchar(20) as tgt_displaystatus,
	createddate::timestamp_ntz(9) as createddate,
	ownername::varchar(100) as ownername,
	druglicenseno2::varchar(50) as druglicenseno2,
	r_statecode::number(18,0) as r_statecode,
	r_districtcode::number(18,0) as r_districtcode,
	r_tahsilcode::number(18,0) as r_tahsilcode,	
	address1::varchar(100) as address1,
	address2::varchar(100) as address2,
	retailerchannelcode::varchar(40) as retailerchannelcode,
	retailerclassid::number(18,0) as retailerclassid,
	filename::varchar(100) as filename
    --CASE 
    --WHEN tgt.crt_dttm IS NULL THEN tmpdt.currtime 
    --WHEN tgt.crt_dttm IS NOT NULL AND (src.retailername <> tgt.retailername OR src.displaystatus <> tgt.displaystatus) THEN tgt.crt_dttm
    --ELSE tgt.crt_dttm 
    --END::timestamp_ntz(9) as crt_dttm,
	--crt_dttm::timestamp_ntz(9) as crt_dttm,
    --CASE 
    --WHEN tgt.crt_dttm IS NULL THEN NULL 
    --WHEN tgt.crt_dttm IS NOT NULL AND (src.retailername <> tgt.retailername OR src.displaystatus <> tgt.displaystatus) THEN tmpdt.currtime
    --ELSE tmpdt.currtime 
    --END::timestamp_ntz(9) as updt_dttm,
	--updt_dttm::timestamp_ntz(9) as updt_dttm,
    --CASE 
    --WHEN tgt.crt_dttm IS NULL THEN 'I' 
    --WHEN tgt.crt_dttm IS NOT NULL AND (src.retailername <> tgt.retailername OR src.displaystatus <> tgt.displaystatus) THEN 'U2'
    --ELSE 'U' 
    --END::AS varchar(2) as chng_flg,
	--chng_flg::varchar(2) as chng_flg,
    --CASE 
    --WHEN tgt.crt_dttm IS NULL THEN 'Y' 
    --WHEN tgt.crt_dttm IS NOT NULL AND (src.retailername <> tgt.retailername OR src.displaystatus <> tgt.displaystatus) THEN 'N'
    --ELSE 'Y' 
    --END::varchar(1) as actv_flg,
	--actv_flg::varchar(1) as actv_flg
    
    from source
)
select * from final
