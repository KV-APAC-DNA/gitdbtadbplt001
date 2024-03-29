with source as(
    select * from {{ ref('thaedw_integration__edw_vw_th_jbp_analysis') }}
),
final as
(
    select 
        data_type::varchar(21) as data_type,
        "year"::number(18,0) as year,
        year_quarter::varchar(14) as year_quarter,
        month_year::varchar(23) as month_year,
        order_date::varchar(52) as order_date,
        month_number::number(18,0) as month_number,
        distributor_id::varchar(20) as distributor_id,
        region_desc::varchar(100) as region_desc,
        city::varchar(100) as city,
        whcode::varchar(50) as whcode,
        whgroup::varchar(12) as whgroup,
        district::varchar(100) as district,
        ar_code::varchar(50) as ar_code,
        ar_name::varchar(500) as ar_name,
        channel_code::varchar(30) as channel_code,
        channel::varchar(100) as channel,
        sales_office_code::varchar(8) as sales_office_code,
        sales_office_name::varchar(27) as sales_office_name,
        sales_group::varchar(10) as sales_group,
        "cluster"::varchar(8) as cluster,
        ar_type_code::varchar(30) as ar_type_code,
        ar_type_name::varchar(400) as ar_type_name,
        distributor_name::varchar(100) as distributor_name,
        sap_cust_id::varchar(10) as sap_cust_id,
        sku_code::varchar(50) as sku_code,
        sku_description::varchar(100) as sku_description,
        bar_code::varchar(50) as bar_code,
        mcl_barcd::varchar(50) as mcl_barcd,
        franchise::varchar(30) as franchise,
        brand::varchar(30) as brand,
        variant::varchar(100) as variant,
        segment::varchar(50) as segment,
        put_up_description::varchar(100) as put_up_description,
        salesman_name::varchar(250) as salesman_name,
        salesman_code::varchar(150) as salesman_code,
        cn_reason_code::varchar(255) as cn_reason_code,
        cn_reason_description::varchar(250) as cn_reason_description,
        gross_trade_sales::float as gross_trade_sales,
        cn_damaged_goods::float as cn_damaged_goods,
        credit_note_amount::float as credit_note_amount,
        line_discount::number(38,6) as line_discount,
        bottom_line_discount::number(38,6) as bottom_line_discount,
        sales_quantity::number(38,6) as sales_quantity,
        return_quantity::number(38,6) as return_quantity,
        quantity_dz::float as quantity_dz,
        net_invoice::float as net_invoice,
        target_calls::number(38,0) as target_calls,
        target_sales::number(38,0) as target_sales,
        str_count::number(18,0) as str_count,
        inventory_quantity::float as inventory_quantity,
        inventory::float as inventory,
        si_sellin_target::number(38,5) as si_sellin_target,
        si_gross_trade_sales_value::number(38,5) as si_gross_trade_sales_value
    from source
)
select * from final