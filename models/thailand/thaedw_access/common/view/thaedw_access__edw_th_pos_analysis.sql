with source as (
    select * from {{ ref('thaedw_integration__edw_th_pos_analysis') }}
),
final as (
    select 
        province_code as "province_code",
        month_number as "month_number",
        province_name as "province_name",
        retailer_unit_conversion as "retailer_unit_conversion",
        inventory_baht as "inventory_baht",
        customer_branch_code as "customer_branch_code",
        franchise as "franchise",
        inventory_quantity_piece as "inventory_quantity_piece",
        bar_code as "bar_code",
        quarter as "quarter",
        salesbaht as "salesbaht",
        inventory_quantity_dozen as "inventory_quantity_dozen",
        branch_name as "branch_name",
        sales_gts as "sales_gts",
        inventory_gts as "inventory_gts",
        region_name as "region_name",
        customer_rsp as "customer_rsp",
        month_week_number as "month_week_number",
        month_year as "month_year",
        variant as "variant",
        put_up as "put_up",
        week as "week",
        brand as "brand",
        invoice_date as "invoice_date",
        customer_code as "customer_code",
        sale_quantity as "sale_quantity",
        segment as "segment",
        year as "year",
        material_description as "material_description",
        branch_type as "branch_type",
        item_code as "item_code",
        list_price as "list_price"
    from source
)
select * from final