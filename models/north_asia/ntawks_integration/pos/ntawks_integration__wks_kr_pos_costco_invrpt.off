with sdl_kr_pos_costco_invrpt_header as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_pos_costco_invrpt_header') }}
),
sdl_kr_pos_costco_invrpt_line as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_pos_costco_invrpt_line') }}
),
final as 
(    
    select 
        ltrim(right(trim(doc_no), 3), 0)::varchar(10) as store_cd,
        ltrim(trim(ean_cd), 0)::varchar(40) as ean_cd,
        to_number(trim(sales_qty))::number(10,0) as sales_qty,
        trim(unit_of_pkg_sales)::varchar(5) as unit_of_pkg_sales,
        to_date(trim(doc_send_date), 'yyyymmdd')::date as doc_send_date,
        to_date(trim(inv_rpt_date), 'yyyymmdd')::date as pos_dt,
        to_number(trim(invt_qty))::number(10,0) as invt_qty,
        trim(unit_of_pkg_invt)::varchar(5) as unit_of_pkg_invt,
        trim(doc_fun)::varchar(6) as doc_fun,
        trim(doc_no)::varchar(40) as doc_no,
        trim(doc_fun_cd)::varchar(6) as doc_fun_cd,
        trim(buye_loc_cd)::varchar(40) as buye_loc_cd,
        trim(vend_loc_cd)::varchar(40) as vend_loc_cd,
        trim(provider_loc_cd)::varchar(40) as provider_loc_cd,
        trim(line_no)::varchar(10) as line_no,
        to_number(trim(comp_qty))::number(10,0) as comp_qty,
        trim(unit_of_pkg_comp)::varchar(5) as unit_of_pkg_comp,
        to_number(trim(order_qty))::number(10,0) as order_qty,
        trim(unit_of_pkg_order)::varchar(5) as unit_of_pkg_order,
        null::varchar(100) as filename,
        null::varchar(40) as run_id,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
    from sdl_kr_pos_costco_invrpt_header h,
        sdl_kr_pos_costco_invrpt_line l
    where left(h.seq_no, 6) = left(l.seq_no, 6)
)
select * from final


