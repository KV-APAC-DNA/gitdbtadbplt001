with source as 
(
    select * from {{ ref('idnwks_integration__wks_mds_id_lav_inventory_intransit') }}
),
final as 
(
    select 
    month::varchar(10) as month,
	plant::varchar(100) as plant,
	saty::varchar(100) as saty,
	doc_num::varchar(100) as doc_num,
	po_num::varchar(100) as po_num,
	matl_num::varchar(100) as matl_num,
	matl_desc::varchar(255) as matl_desc,
	ship_to::varchar(100) as ship_to,
	rj_cd::varchar(100) as rj_cd,
	rj_reason::varchar(255) as rj_reason,
	billing::varchar(100) as billing,
	not_invoiced::varchar(100) as not_invoiced,
    to_date(doc_date) as doc_date,
    to_date(goods_issue) as goods_issue,
    to_date(bill_date) as bill_date,
    to_date(rdd) as rdd,
	order_qty::number(20,4) as order_qty,
	confirmed_qty::number(20,4) as confirmed_qty,
	net_value::number(20,4) as net_value,
	billing_check::number(20,4) as billing_check,
	order_value::number(20,4) as order_value,
	billing_value::number(20,4) as billing_value,
	unrecoverable_ord_val::number(20,4) as unrecoverable_ord_val,
	open_orders_val::number(20,4) as open_orders_val,
	return_value::number(20,4) as return_value,
	cust_type::varchar(100) as cust_type,
	cust_grp::varchar(100) as cust_grp,
	cust_name::varchar(255) as cust_name,
	bill_month::varchar(100) as bill_month,
	return_billing::number(20,4) as return_billing,
	unrecoverable_billing::number(20,4) as unrecoverable_billing,
	ship_to_2::varchar(100) as ship_to_2,
	to_date(gi_date) as gi_date,
	order_week::varchar(100) as order_week,
	pod::varchar(100) as pod,
	to_date(first_day) as first_day,
	remarks::varchar(255) as remarks,
	name1::varchar(255) as name1,
	to_date(created_on1) as created_on1,
	hashkey::varchar(255) as hashkey,
	convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crtd_dttm
    from source
)
select * from final