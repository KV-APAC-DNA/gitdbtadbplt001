 {{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
    )
 }}
 
 with wks_so_sales_133986 as(
    select * from {{ ref('myswks_integration__wks_so_sales_133986') }}
 ),
 wks_so_sales_133980 as(
    select * from {{ ref('myswks_integration__wks_so_sales_133980') }}
 ),
 wks_so_sales_133981 as(
    select * from {{ ref('myswks_integration__wks_so_sales_133981') }}
 ),
 wks_so_sales_133982 as(
    select * from {{ ref('myswks_integration__wks_so_sales_133982') }}
 ),
 wks_so_sales_133983 as(
    select * from {{ ref('myswks_integration__wks_so_sales_133983') }}
 ),
 wks_so_sales_133984 as(
    select * from {{ ref('myswks_integration__wks_so_sales_133984') }}
 ),
 wks_so_sales_133985 as(
    select * from {{ ref('myswks_integration__wks_so_sales_133985') }}
 ),
wks_so_sales_131164 as(
    select * from {{ ref('myswks_integration__wks_so_sales_131164') }}
 ),
wks_so_sales_131165 as(
    select * from {{ ref('myswks_integration__wks_so_sales_131165') }}
 ),
wks_so_sales_131166 as(
    select * from {{ ref('myswks_integration__wks_so_sales_131166') }}
 ),
wks_so_sales_131167 as(
    select * from {{ ref('myswks_integration__wks_so_sales_131167') }}
 ),
wks_so_sales_130516 as(
    select * from {{ ref('myswks_integration__wks_so_sales_130516') }}
 ),
wks_so_sales_130517 as(
    select * from {{ ref('myswks_integration__wks_so_sales_130517') }}
 ),
wks_so_sales_130518 as(
    select * from {{ ref('myswks_integration__wks_so_sales_130518') }}
 ),
wks_so_sales_130519 as(
    select * from {{ ref('myswks_integration__wks_so_sales_130519') }}
 ),
wks_so_sales_130520 as(
    select * from {{ ref('myswks_integration__wks_so_sales_130520') }}
 ),
wks_so_sales_130521 as(
    select * from {{ ref('myswks_integration__wks_so_sales_130521') }}
 ),
wks_so_sales_119024 as(
    select * from {{ ref('myswks_integration__wks_so_sales_119024') }}
 ),
wks_so_sales_119025 as(
    select * from {{ ref('myswks_integration__wks_so_sales_119025') }}
 ),
wks_so_sales_108129 as(
    select * from {{ ref('myswks_integration__wks_so_sales_108129') }}
 ),
wks_so_sales_108273 as(
    select * from {{ ref('myswks_integration__wks_so_sales_108273') }}
 ),
wks_so_sales_108565 as(
    select * from {{ ref('myswks_integration__wks_so_sales_108565') }}
 ),
wks_so_sales_118477 as(
    select * from {{ ref('myswks_integration__wks_so_sales_118477') }}
 ),
wks_so_sales_109772 as(
    select * from {{ ref('myswks_integration__wks_so_sales_109772') }}
 ),
wks_so_sales_135976 as(
    select * from {{ ref('myswks_integration__wks_so_sales_135976') }}
 ),
wks_so_sales_137021 as(
    select * from {{ ref('myswks_integration__wks_so_sales_137021') }}
 ),
transformed as(
    select * from wks_so_sales_108129
    union all
    select * from wks_so_sales_108273
    union all
    select * from wks_so_sales_108565
    union all
    select * from wks_so_sales_109772
    union all
    select * from wks_so_sales_118477
    union all
    select * from wks_so_sales_119024
    union all
    select * from wks_so_sales_119025
    union all
    select * from wks_so_sales_130516
    union all
    select * from wks_so_sales_130517
    union all
    select * from wks_so_sales_130518
    union all
    select * from wks_so_sales_130519
    union all
    select * from wks_so_sales_130520
    union all
    select * from wks_so_sales_130521
    union all
    select * from wks_so_sales_131164
    union all
    select * from wks_so_sales_131165
    union all
    select * from wks_so_sales_131166
    union all
    select * from wks_so_sales_131167
    union all
    select * from wks_so_sales_133980
    union all
    select * from wks_so_sales_133981
    union all
    select * from wks_so_sales_133982
    union all
    select * from wks_so_sales_133983
    union all
    select * from wks_so_sales_133984
    union all
    select * from wks_so_sales_133985
    union all
    select * from wks_so_sales_133986
    union all
    select * from wks_so_sales_135976
    union all
    select * from wks_so_sales_137021
),
final as(
    select 
    distributor_id::varchar(255) as dstrbtr_id,
    sales_order_number::varchar(255) as sls_ord_num,
    sales_order_date::varchar(255) as sls_ord_dt,
    type::varchar(255) as type,
    customer_code::varchar(255) as cust_cd,
    distributor_wh_id::varchar(255) as dstrbtr_wh_id,
    sap_material_id::varchar(255) as item_cd,
    product_code::varchar(255) as dstrbtr_prod_cd,
    product_ean_code::varchar(255) as ean_num,
    product_description::varchar(255) as dstrbtr_prod_desc,
    gross_item_price::varchar(255) as grs_prc,
    quantity::varchar(255) as qty,
    uom::varchar(255) as uom,
    quantity_in_pieces::varchar(255) as qty_pc,
    quantity_after_conversion::varchar(255) as qty_aft_conv,
    sub_total_1::varchar(255) as subtotal_1,
    discount::varchar(255) as discount,
    sub_total_2::varchar(255) as subtotal_2,
    bottom_line_discount::varchar(255) as bottom_line_dscnt,
    total_amt_after_tax::varchar(255) as total_amt_aft_tax,
    total_amt_before_tax::varchar(255) as total_amt_bfr_tax,
    sales_employee::varchar(255) as sls_emp,
    custom_field_1::varchar(255) as custom_field1,
    custom_field_2::varchar(255) as custom_field2,
    custom_field_3::varchar(255) as custom_field3,
    file_name::varchar(255) as filename,            
    current_timestamp()::timestamp_ntz(9) as curr_dt,
    NULL as cdl_dttm from transformed
)
select * from final
