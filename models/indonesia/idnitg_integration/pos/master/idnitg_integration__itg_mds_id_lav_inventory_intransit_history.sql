{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}
with 
sdl_mds_id_lav_inventory_intransit as
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_lav_inventory_intransit') }}
),
itg_mds_id_lav_inventory_intransit as
(
    select * from {{ ref('idnitg_integration__itg_mds_id_lav_inventory_intransit') }}
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
	convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as crtd_dttm
    from itg_mds_id_lav_inventory_intransit
    where nvl(
        left (to_char(created_on1, 'yyyymmdd'), 6),
        '99991231'
    ) in (
        select distinct nvl(
                left (
                    to_char(cast("created on1" as date), 'yyyymmdd'),
                    6
                ),
                '99991231'
            )
        from sdl_mds_id_lav_inventory_intransit
    )
    and nvl(
        left (to_char(created_on1, 'yyyymmdd'), 6),
        '99991231'
    ) in (
        select distinct nvl(
                left (to_char(created_on1, 'yyyymmdd'), 6),
                '99991231'
            )
        from itg_mds_id_lav_inventory_intransit
        where hashkey not in (
                select distinct md5(
                      nvl (month, '') || nvl (plant, '') || nvl (saty, '') || nvl (document, '') || nvl ("po number", '') || nvl (material, '') || nvl (description, '') || nvl ("ship-to", '') || nvl (rj, '') || nvl (reason, '') || nvl (billing, '') || nvl ("not invoiced", '') || nvl ("DOC. DATE", '9999-12-31') || nvl ("goods issue", '9999-12-31') || nvl ("billing date", '9999-12-31') || nvl (rdd, '9999-12-31') || nvl ("order qty", 0) || nvl ("confirm qty", 0) || nvl ("net value", 0) || nvl ("billing check", 0) || nvl ("order value", 0) || nvl ("billing value", 0) || nvl (unrecoverable, 0) || nvl ("open orders", 0) || nvl ("return value", 0) || nvl ("customer type", '') || nvl ("customer group", '') || nvl (customer, '') || nvl ("bill month", '') || nvl ("return billing", 0) || nvl ("unrecoverable billing", 0) || nvl ("ship-to 2", '') || nvl ("gi date/rdd", '9999-12-31') || nvl ("order week", '') || nvl (pod, '9999-12-31') || nvl (cast("1st day" as date), '9999-12-31') || nvl (remarks, '') || nvl (name1, '') || nvl (cast("created on1" as date), '9999-12-31')
                    )
                FROM sdl_mds_id_lav_inventory_intransit
            )
    )
)
select * from final