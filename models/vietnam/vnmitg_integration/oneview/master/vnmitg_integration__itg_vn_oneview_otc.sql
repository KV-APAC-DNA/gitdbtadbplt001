{{
config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=["billingdate","order_no"],
        pre_hook= "delete from {{this}} where  (billingdate,order_no) in (select distinct order_no,case when billingdate like '%-%' then to_char(to_date(billingdate,'dd-mm-yyyy'),'yyyymmdd') else billingdate end as billingdate from {{ source('vnmsdl_raw', 'sdl_vn_oneview_otc') }})"
)
}}

with sdl_vn_oneview_otc as 
(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_oneview_otc') }}
),
edw_vw_vn_mt_dist_products as 
(
    select * from snaposeedw_integration.edw_vw_vn_mt_dist_products
),
final as 
(
select 
    plant::varchar(10) as plant,
	principalcode::varchar(100) as principalcode,
	principal::varchar(100) as principal,
	product::varchar(100) as product,
	productname::varchar(100) as productname,
	kunnr::varchar(100) as kunnr,
	name1::varchar(100) as name1,
	name2::varchar(100) as name2,
	address::varchar(100) as address,
	province::varchar(100) as province,
	zterm::varchar(100) as zterm,
	kdgrp::varchar(100) as kdgrp,
	custgroup::varchar(100) as custgroup,
	region::varchar(100) as region,
	district::varchar(100) as district,
	vbeln::varchar(100) as vbeln,
    case
        when billingdate like '%-%' then to_char(to_date(billingdate, 'dd-mm-yyyy'), 'yyyymmdd')
        else billingdate
    end::varchar(100) as billingdate,
    reason::varchar(100) as reason,
	qty::varchar(100) as qty,
	dgle::varchar(100) as dgle,
	pernr::varchar(100) as pernr,
	vat::varchar(100) as vat,
	suom::varchar(100) as suom,
	custpayto::varchar(100) as custpayto,
	tt::number(21,5) as tt,
	nguyengia::varchar(100) as nguyengia,
	ttv::number(21,5) as ttv,
	discount::number(21,5) as discount,
	device_code::varchar(100) as device_code,
	device::varchar(100) as device,
	order_no::varchar(100) as order_no,
	orginv::varchar(100) as orginv,
	batch::varchar(100) as batch,
	charge::varchar(100) as charge,
	contact_name::varchar(100) as contact_name,
	userid::varchar(100) as userid,
	billinginst::varchar(100) as billinginst,
	distchannel::varchar(100) as distchannel,
	redinv::varchar(100) as redinv,
	serial::varchar(100) as serial,
	potext::varchar(100) as potext,
	expdate::varchar(100) as expdate,
	ret_so::varchar(100) as ret_so,
	vat_code::varchar(100) as vat_code,
	sodoc_date::varchar(100) as sodoc_date,
	itemnotes::varchar(100) as itemnotes,
	mg1::varchar(100) as mg1,
	year::varchar(100) as year,
	month::varchar(100) as month,
	channel::varchar(100) as channel,
	file_name::varchar(100) as file_name,
	null::varchar(100) as run_id,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm,
    product.sap_matl_num::varchar(100) as sap_matl_num,
from sdl_vn_oneview_otc ot
    left join (
        select jnj_sap_code as sap_matl_num,
            code
        from edw_vw_vn_mt_dist_products
    ) product on product.code = ot.product
)

select * from final