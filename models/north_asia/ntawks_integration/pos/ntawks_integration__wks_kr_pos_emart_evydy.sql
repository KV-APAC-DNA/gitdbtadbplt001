
with sdl_kr_pos_emart_evydy_line as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_pos_emart_evydy_line') }}
),
sdl_kr_pos_emart_evydy_header as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_pos_emart_evydy_header') }}
),
final as (
select
 to_date(h.mesg_date, 'yyyymmdd') as mesg_date,
h.mesg_code,
h.mesg_func_code,
h.send_date,
h.send_gln,
h.send_id,
h.send_name,
h.recv_gln,
h.recv_id,
h.recv_name,
h.sendyn,
l.mesg_name,
l.mesg_id,
l.mesg_from,
l.mesg_to,
cast(l.sale_date as date) as sale_date,
l.mesg_no,
line_no,
sale_id,
sale_id_map,
sale_gln,
sale_name,
l.sale_date_type,
cate_code,
prod_code,
prod_code_map,
prod_name1,
prod_name2,
sale_amnt,
sale_mon_amnt,
unit_price,
sale_qty,
sale_unit,
sale_mon_qty,
sale_div,
sale_type,
online_sign,
online_sale_qty,
online_sale_amnt,
disc_sign,
disc_sale_amnt,
l.reg_date,
l.upd_date,
l.crt_dttm,
l.updt_dttm
   from sdl_kr_pos_emart_evydy_line l
left outer join sdl_kr_pos_emart_evydy_header h
 on h.mesg_id = l.mesg_id
)
select * from final