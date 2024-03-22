with source as(
    select * from {{ ref('thaedw_integration__edw_vw_th_pos_analysis') }}
),
final as(
    select 
        customer_code::varchar(30) as customer_code,
        bar_code::varchar(20) as bar_code,
        invoice_date::timestamp_ntz(9) as invoice_date,
        customer_branch_code::varchar(20) as customer_branch_code,
        "year"::number(18,0) as year,
        quarter::varchar(14) as quarter,
        month_year::varchar(23) as month_year,
        month_number::number(18,0) as month_number,
        week::number(38,0) as week,
        month_week_number::number(38,0) as month_week_number,
        region_name::varchar(200) as region_name,
        province_code::varchar(200) as province_code,
        province_name::varchar(200) as province_name,
        branch_name::varchar(251) as branch_name,
        branch_type::varchar(200) as branch_type,
        item_code::varchar(20) as item_code,
        material_description::varchar(100) as material_description,
        franchise::varchar(30) as franchise,
        brand::varchar(30) as brand,
        variant::varchar(100) as variant,
        segment::varchar(50) as segment,
        put_up::varchar(100) as put_up,
        inventory_quantity_piece::number(38,4) as inventory_quantity_piece,
        inventory_quantity_dozen::number(38,4) as inventory_quantity_dozen,
        inventory_baht::number(38,4) as inventory_baht,
        inventory_gts::number(38,4) as inventory_gts,
        sale_quantity::number(38,4) as sale_quantity,
        salesbaht::number(38,4) as salesbaht,
        sales_gts::number(38,4) as sales_gts,
        retailer_unit_conversion::number(31,0) as retailer_unit_conversion,
        list_price::number(20,4) as list_price,
        customer_rsp::number(20,4) as customer_rsp
    from source
)
select * from final