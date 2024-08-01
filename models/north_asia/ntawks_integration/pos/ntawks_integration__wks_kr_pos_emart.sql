with sdl_kr_pos_emart_line as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_pos_emart_line') }}
),
sdl_kr_pos_emart_header as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_pos_emart_header') }}
),
final as
(   
    select 
        h.mesg_no,
        h.mesg_code,
        h.mesg_func_code,
        to_date(h.mesg_date,  'yyyymmdd') as mesg_date,
        to_date(h.sale_date, 'yyyymmdd') as sale_date,
        h.sale_date_form,
        h.send_code,
        h.send_ean_code,
        h.send_name,
        h.recv_qual,
        h.recv_ean_code,
        h.recv_name,
        h.part_qual,
        h.part_ean_code,
        h.part_id,
        h.part_name,
        h.sender_id,
        h.recv_date,
        h.recv_time,
        h.file_size,
        h.file_path,
        h.lega_tran,
        h.regi_date,
        line_no,
        store_code,
        store_name,
        sku_code,
        instore_code,
        sku_name1,
        sku_name2,
        day_sale_amnt,
        mnth_sale_amnt,
        unit_prce,
        day_sale_qty,
        qty_unit,
        mnth_sale_qty,
        h.crt_dttm,
        h.updt_dttm
    from sdl_kr_pos_emart_line l
        left outer join sdl_kr_pos_emart_header h on h.mesg_no = l.mesg_no
)
select * from final