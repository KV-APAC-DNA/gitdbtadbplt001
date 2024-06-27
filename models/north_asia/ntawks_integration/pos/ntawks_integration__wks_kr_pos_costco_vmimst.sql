with sdl_kr_pos_costco_vmimst_header as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_pos_costco_vmimst_header') }}
),
sdl_kr_pos_costco_vmimst_line as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_pos_costco_vmimst_line') }}
),
final as (
    select 
        trim(line_no) as line_no,
        ltrim(trim(product_cd), 0) product_cd,
        trim(product_nm) as product_nm,
        ltrim(store_cd, 0) as store_cd,
        trim(store_nm) as store_nm,
        ltrim(vendor, 0) as vendor,
        try_to_date(trim(prm_strt_dt), 'yyyymmdd') as prm_strt_dt,
        try_to_date(trim(prm_end_dt), 'yyyymmdd') as prm_end_dt,
        to_number(trim(sales_tgt), '99999999') as sales_tgt,
        to_number(trim(amt_order), '99999999') as amt_order,
        to_number(trim(warehouse_dt), '99999999') as warehouse_dt,
        item_type,
        unit_of_pkg_item,
        ltrim(pack_size, 0) as pack_size,
        delivery_method,
        style,
        occ_no,
        convert_timezone('UTC', current_timestamp()) as crt_dttm,
        convert_timezone('UTC', current_timestamp()) as updt_dttm,
        null as filename,
        null as run_id
    from sdl_kr_pos_costco_vmimst_header h,
        sdl_kr_pos_costco_vmimst_line l
    where left(h.seq_no, 6) = left(l.seq_no, 6)
)
select * from final