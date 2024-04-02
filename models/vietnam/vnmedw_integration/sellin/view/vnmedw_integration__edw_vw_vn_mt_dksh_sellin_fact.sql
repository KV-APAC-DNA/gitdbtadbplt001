with source as(
    select * from {{ ref('vnmitg_integration__itg_vn_mt_sellin_dksh') }} 
),
source2 as(
    select * from {{ ref('vnmitg_integration__itg_vn_mt_sellin_dksh_history') }}
),
itg_vn_mt_sellin_dksh as (
        SELECT dense_rank() OVER (
                PARTITION BY itg_vn_mt_sellin_dksh.productid
                ,itg_vn_mt_sellin_dksh.custcode
                ,itg_vn_mt_sellin_dksh.billing_no
                ,itg_vn_mt_sellin_dksh.invoice_date
                ,itg_vn_mt_sellin_dksh.order_no ORDER BY itg_vn_mt_sellin_dksh.filename DESC
                ) AS rnk
            ,itg_vn_mt_sellin_dksh.supplier_code
            ,itg_vn_mt_sellin_dksh.supplier_name
            ,itg_vn_mt_sellin_dksh.plant
            ,itg_vn_mt_sellin_dksh.productid
            ,itg_vn_mt_sellin_dksh.product
            ,itg_vn_mt_sellin_dksh.brand
            ,itg_vn_mt_sellin_dksh.sellin_category
            ,itg_vn_mt_sellin_dksh.product_group
            ,itg_vn_mt_sellin_dksh.product_sub_group
            ,itg_vn_mt_sellin_dksh.unit_of_measurement
            ,itg_vn_mt_sellin_dksh.custcode
            ,itg_vn_mt_sellin_dksh.customer
            ,itg_vn_mt_sellin_dksh.address
            ,itg_vn_mt_sellin_dksh.district
            ,itg_vn_mt_sellin_dksh.province
            ,itg_vn_mt_sellin_dksh.region
            ,itg_vn_mt_sellin_dksh.zone
            ,itg_vn_mt_sellin_dksh.channel
            ,itg_vn_mt_sellin_dksh.sellin_sub_channel
            ,itg_vn_mt_sellin_dksh.cust_group
            ,itg_vn_mt_sellin_dksh.billing_no
            ,itg_vn_mt_sellin_dksh.invoice_date
            ,itg_vn_mt_sellin_dksh.qty_include_foc
            ,itg_vn_mt_sellin_dksh.qty_exclude_foc
            ,itg_vn_mt_sellin_dksh.foc
            ,itg_vn_mt_sellin_dksh.net_price_wo_vat
            ,itg_vn_mt_sellin_dksh.tax
            ,itg_vn_mt_sellin_dksh.net_amount_wo_vat
            ,itg_vn_mt_sellin_dksh.net_amount_w_vat
            ,itg_vn_mt_sellin_dksh.gross_amount_wo_vat
            ,itg_vn_mt_sellin_dksh.gross_amount_w_vat
            ,itg_vn_mt_sellin_dksh.list_price_wo_vat
            ,itg_vn_mt_sellin_dksh.vendor_lot
            ,itg_vn_mt_sellin_dksh.order_type
            ,itg_vn_mt_sellin_dksh.red_invoice_no
            ,itg_vn_mt_sellin_dksh.expiry_date
            ,itg_vn_mt_sellin_dksh.order_no
            ,itg_vn_mt_sellin_dksh.order_date
            ,itg_vn_mt_sellin_dksh.period
            ,itg_vn_mt_sellin_dksh.sellout_sub_channel
            ,itg_vn_mt_sellin_dksh.group_account
            ,itg_vn_mt_sellin_dksh.account
            ,itg_vn_mt_sellin_dksh.name_st_or_ddp
            ,itg_vn_mt_sellin_dksh.zone_or_area
            ,itg_vn_mt_sellin_dksh.franchise
            ,itg_vn_mt_sellin_dksh.sellout_category
            ,itg_vn_mt_sellin_dksh.sub_cat
            ,itg_vn_mt_sellin_dksh.sub_brand
            ,itg_vn_mt_sellin_dksh.barcode
            ,itg_vn_mt_sellin_dksh.base_or_bundle
            ,itg_vn_mt_sellin_dksh.size
            ,itg_vn_mt_sellin_dksh.key_chain
            ,itg_vn_mt_sellin_dksh.STATUS
            ,itg_vn_mt_sellin_dksh.filename
            ,itg_vn_mt_sellin_dksh.run_id
            ,itg_vn_mt_sellin_dksh.crtd_dttm
            ,itg_vn_mt_sellin_dksh.updt_dttm
        FROM source itg_vn_mt_sellin_dksh
        WHERE (
                ((itg_vn_mt_sellin_dksh.channel)::TEXT <> 'ECOM'::TEXT)
                AND ((itg_vn_mt_sellin_dksh.channel)::TEXT <> 'GT'::TEXT)
                )
),
itg_vn_mt_sellin_dksh_history as (
       SELECT itg_vn_mt_sellin_dksh_history.supplier_code
            ,itg_vn_mt_sellin_dksh_history.supplier_name
            ,itg_vn_mt_sellin_dksh_history.plant
            ,itg_vn_mt_sellin_dksh_history.productid
            ,itg_vn_mt_sellin_dksh_history.product
            ,itg_vn_mt_sellin_dksh_history.brand
            ,itg_vn_mt_sellin_dksh_history.sellin_category
            ,itg_vn_mt_sellin_dksh_history.product_group
            ,itg_vn_mt_sellin_dksh_history.product_sub_group
            ,itg_vn_mt_sellin_dksh_history.unit_of_measurement
            ,itg_vn_mt_sellin_dksh_history.custcode
            ,itg_vn_mt_sellin_dksh_history.customer
            ,itg_vn_mt_sellin_dksh_history.address
            ,itg_vn_mt_sellin_dksh_history.district
            ,itg_vn_mt_sellin_dksh_history.province
            ,itg_vn_mt_sellin_dksh_history.region
            ,itg_vn_mt_sellin_dksh_history.zone
            ,itg_vn_mt_sellin_dksh_history.channel
            ,itg_vn_mt_sellin_dksh_history.sellin_sub_channel
            ,itg_vn_mt_sellin_dksh_history.cust_group
            ,itg_vn_mt_sellin_dksh_history.billing_no
            ,itg_vn_mt_sellin_dksh_history.invoice_date
            ,itg_vn_mt_sellin_dksh_history.qty_include_foc
            ,itg_vn_mt_sellin_dksh_history.qty_exclude_foc
            ,itg_vn_mt_sellin_dksh_history.foc
            ,itg_vn_mt_sellin_dksh_history.net_price_wo_vat
            ,itg_vn_mt_sellin_dksh_history.tax
            ,itg_vn_mt_sellin_dksh_history.net_amount_wo_vat
            ,itg_vn_mt_sellin_dksh_history.net_amount_w_vat
            ,itg_vn_mt_sellin_dksh_history.gross_amount_wo_vat
            ,itg_vn_mt_sellin_dksh_history.gross_amount_w_vat
            ,itg_vn_mt_sellin_dksh_history.list_price_wo_vat
            ,itg_vn_mt_sellin_dksh_history.vendor_lot
            ,itg_vn_mt_sellin_dksh_history.order_type
            ,itg_vn_mt_sellin_dksh_history.red_invoice_no
            ,itg_vn_mt_sellin_dksh_history.expiry_date
            ,itg_vn_mt_sellin_dksh_history.order_no
            ,itg_vn_mt_sellin_dksh_history.order_date
            ,itg_vn_mt_sellin_dksh_history.period
            ,itg_vn_mt_sellin_dksh_history.sellout_sub_channel
            ,itg_vn_mt_sellin_dksh_history.group_account
            ,itg_vn_mt_sellin_dksh_history.account
            ,itg_vn_mt_sellin_dksh_history.name_st_or_ddp
            ,itg_vn_mt_sellin_dksh_history.zone_or_area
            ,itg_vn_mt_sellin_dksh_history.franchise
            ,itg_vn_mt_sellin_dksh_history.sellout_category
            ,itg_vn_mt_sellin_dksh_history.sub_cat
            ,itg_vn_mt_sellin_dksh_history.sub_brand
            ,itg_vn_mt_sellin_dksh_history.barcode
            ,itg_vn_mt_sellin_dksh_history.base_or_bundle
            ,itg_vn_mt_sellin_dksh_history.size
            ,itg_vn_mt_sellin_dksh_history.key_chain
            ,itg_vn_mt_sellin_dksh_history.STATUS
        FROM source2 itg_vn_mt_sellin_dksh_history
        WHERE (
                ((itg_vn_mt_sellin_dksh_history.channel)::TEXT <> 'ECOM'::TEXT)
                AND ((itg_vn_mt_sellin_dksh_history.channel)::TEXT <> 'GT'::TEXT)
                )
),
transformed as(
    SELECT itg_vn_mt_sellin_dksh.supplier_code
        ,itg_vn_mt_sellin_dksh.supplier_name
        ,itg_vn_mt_sellin_dksh.plant
        ,itg_vn_mt_sellin_dksh.productid
        ,itg_vn_mt_sellin_dksh.product
        ,itg_vn_mt_sellin_dksh.brand
        ,itg_vn_mt_sellin_dksh.sellin_category
        ,itg_vn_mt_sellin_dksh.product_group
        ,itg_vn_mt_sellin_dksh.product_sub_group
        ,itg_vn_mt_sellin_dksh.unit_of_measurement
        ,itg_vn_mt_sellin_dksh.custcode
        ,itg_vn_mt_sellin_dksh.customer
        ,itg_vn_mt_sellin_dksh.address
        ,itg_vn_mt_sellin_dksh.district
        ,itg_vn_mt_sellin_dksh.province
        ,itg_vn_mt_sellin_dksh.region
        ,itg_vn_mt_sellin_dksh.zone
        ,itg_vn_mt_sellin_dksh.channel
        ,itg_vn_mt_sellin_dksh.sellin_sub_channel
        ,itg_vn_mt_sellin_dksh.cust_group
        ,itg_vn_mt_sellin_dksh.billing_no
        ,itg_vn_mt_sellin_dksh.invoice_date
        ,itg_vn_mt_sellin_dksh.qty_include_foc
        ,itg_vn_mt_sellin_dksh.qty_exclude_foc
        ,itg_vn_mt_sellin_dksh.foc
        ,itg_vn_mt_sellin_dksh.net_price_wo_vat
        ,itg_vn_mt_sellin_dksh.tax
        ,itg_vn_mt_sellin_dksh.net_amount_wo_vat
        ,itg_vn_mt_sellin_dksh.net_amount_w_vat
        ,itg_vn_mt_sellin_dksh.gross_amount_wo_vat
        ,itg_vn_mt_sellin_dksh.gross_amount_w_vat
        ,itg_vn_mt_sellin_dksh.list_price_wo_vat
        ,itg_vn_mt_sellin_dksh.vendor_lot
        ,itg_vn_mt_sellin_dksh.order_type
        ,itg_vn_mt_sellin_dksh.red_invoice_no
        ,itg_vn_mt_sellin_dksh.expiry_date
        ,itg_vn_mt_sellin_dksh.order_no
        ,itg_vn_mt_sellin_dksh.order_date
        ,itg_vn_mt_sellin_dksh.period
        ,itg_vn_mt_sellin_dksh.sellout_sub_channel
        ,itg_vn_mt_sellin_dksh.group_account
        ,itg_vn_mt_sellin_dksh.account
        ,itg_vn_mt_sellin_dksh.name_st_or_ddp
        ,itg_vn_mt_sellin_dksh.zone_or_area
        ,itg_vn_mt_sellin_dksh.franchise
        ,itg_vn_mt_sellin_dksh.sellout_category
        ,itg_vn_mt_sellin_dksh.sub_cat
        ,itg_vn_mt_sellin_dksh.sub_brand
        ,itg_vn_mt_sellin_dksh.barcode
        ,itg_vn_mt_sellin_dksh.base_or_bundle
        ,itg_vn_mt_sellin_dksh.size
        ,itg_vn_mt_sellin_dksh.key_chain
        ,itg_vn_mt_sellin_dksh.STATUS
    FROM itg_vn_mt_sellin_dksh
    WHERE (itg_vn_mt_sellin_dksh.rnk = 1)

    UNION ALL
    select * from itg_vn_mt_sellin_dksh_history

)
select * from transformed
