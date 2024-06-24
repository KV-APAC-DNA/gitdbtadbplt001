{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "{% if is_incremental() %}
        delete from {{this}} as itg_rrl_retailermaster using {{ ref('indwks_integration__wks_rrl_retailermaster')}} as s where itg_rrl_retailermaster.distributorcode = s.distributorcode and itg_rrl_retailermaster.retailercode=s.retailercode and itg_rrl_retailermaster.rsdcode=s.rsdcode and itg_rrl_retailermaster.villagecode=s.villagecode and itg_rrl_retailermaster.rsrcode=s.rsrcode and s.chng_flg in ('U', 'U2') and itg_rrl_retailermaster.actv_flg='Y';
        {% endif %}"
    )
}}
with source as
(
    select * from {{ ref('indwks_integration__wks_rrl_retailermaster')}}
),
transformed as
(
    select
    retailercode,
    CASE 
    WHEN chng_flg in ('I', 'U') 
        THEN src_retailername
    ELSE tgt_retailername
    END retailername,
    routecode,
    retailerclasscode,
    villagecode,
    rsdcode,
    distributorcode,
    foodlicenseno,
    druglicenseno,
    address,
    phone,
    mobile,
    prcontact,
    seccontact,
    creditlimit,
    creditperiod,
    invoicelimit,
    isapproved,
    isactive,
    rsrcode,
    drugvaliditydate,
    fssaivaliditydate,
    CASE 
    WHEN chng_flg in ('I', 'U') 
        THEN src_displaystatus
    ELSE tgt_displaystatus
    END displaystatus,
    createddate,
    ownername,
    druglicenseno2,
    r_statecode,
    r_districtcode,
    r_tahsilcode,
    address1,
    address2,
    retailerchannelcode,
    retailerclassid,
    actv_flg,
    filename,
    crt_dttm,
    updt_dttm
    from source
),
final as
(
    select
    retailercode::varchar(50) as retailercode,
	retailername::varchar(100) as retailername,
	routecode::varchar(25) as routecode,
	retailerclasscode::varchar(50) as retailerclasscode,
	villagecode::varchar(50) as villagecode,
	rsdcode::varchar(50) as rsdcode,
	distributorcode::varchar(50) as distributorcode,
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
	rsrcode::varchar(100) as rsrcode,
	drugvaliditydate::timestamp_ntz(9) as drugvaliditydate,
	fssaivaliditydate::timestamp_ntz(9) as fssaivaliditydate,
	displaystatus::varchar(20) as displaystatus,
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
	actv_flg::varchar(1) as actv_flg,
	filename::varchar(100) as filename,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm

    from transformed
)

select * from final