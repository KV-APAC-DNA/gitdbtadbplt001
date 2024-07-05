with wks_ka_sales_fact as 
(
    select * from DEV_DNA_CORE.SNAPINDWKS_INTEGRATION.WKS_KA_SALES_FACT
),
final as 
(
    select
        customer_code::varchar(50) as customer_code,
        invoice_date::number(18,0) as invoice_date,
        retailer_code::varchar(50) as retailer_code,
        retailer_name::varchar(200) as retailer_name,
        product_code::varchar(50) as product_code,
        invoice_no::varchar(100) as invoice_no,
        prdqty::number(18,0) as prdqty,
        prdtaxamt::number(38,6) as prdtaxamt,
        prdschdiscamt::number(38,6) as prdschdiscamt,
        prddbdiscamt::number(38,6) as prddbdiscamt,
        salwdsamt::number(38,6) as salwdsamt,
        saleflag::varchar(10) as saleflag,
        confirmsales::varchar(10) as confirmsales,
        subtotal4::number(21,3) as subtotal4,
        totalgrosssalesincltax::number(38,6) as totalgrosssalesincltax,
        totalsalesnr::number(38,6) as totalsalesnr,
        totalsalesconfirmed::number(38,6) as totalsalesconfirmed,
        totalsalesnrconfirmed::number(38,6) as totalsalesnrconfirmed,
        totalsalesunconfirmed::number(38,6) as totalsalesunconfirmed,
        totalsalesnrunconfirmed::number(38,6) as totalsalesnrunconfirmed,
        totalqtyconfirmed::number(18,0) as totalqtyconfirmed,
        totalqtyunconfirmed::number(18,0) as totalqtyunconfirmed,
        buyingoutlets::varchar(100) as buyingoutlets,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from wks_ka_sales_fact
)
select * from final
