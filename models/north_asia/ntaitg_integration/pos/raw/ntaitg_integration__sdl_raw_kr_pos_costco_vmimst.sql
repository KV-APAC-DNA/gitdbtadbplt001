{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as (
    select * from {{ ref('ntawks_integration__wks_kr_pos_costco_vmimst') }}
),
final as (
    select
        line_no::varchar(10) as line_no,
        product_cd::varchar(20) as product_cd,
        product_nm::varchar(60) as product_nm,
        store_cd::varchar(10) as store_cd,
        store_nm::varchar(60) as store_nm,
        vendor::varchar(20) as vendor,
        prm_strt_dt::date as prm_strt_dt,
        prm_end_dt::date as prm_end_dt,
        sales_tgt::varchar(10) as sales_tgt,
        amt_order::varchar(10) as amt_order,
        warehouse_dt::varchar(10) as warehouse_dt,
        item_type::varchar(3) as item_type,
        unit_of_pkg_item::varchar(20) as unit_of_pkg_item,
        pack_size::varchar(10) as pack_size,
        delivery_method::varchar(1) as delivery_method,
        style::varchar(10) as style,
        occ_no::varchar(20) as occ_no,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm,
        filename::varchar(100) as filename,
        run_id::number(14,0) as run_id
    from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)
select * from final