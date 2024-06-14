with itg_orderbooking as 
(
    select * from inditg_integration.itg_orderbooking
),
itg_salesinvoiceorders as
(
    select * from inditg_integration.itg_salesinvoiceorders
),
itg_retailermaster as
(
    select * from inditg_integration.itg_retailermaster
),
final as
(
    select 
    src.Customer_Code,
    src.Salesman_Code,
    src.Route_Code,
    src.Retailer_Code,
    src.Retailer_Name,
    src.Product_Code,
    src.Order_Date,
    src.Order_No,
    src.Ord_Qty,
    src.Ord_Amt,
    src.Invoice_No,
    src.crt_dttm ,
    src.updt_dttm
    from 
    (SELECT
    OrderBooking.DistCode as Customer_Code,
    OrderBooking.SalesmanCode as Salesman_Code,
    OrderBooking.SalesRouteCode as Route_Code,
    retailermaster.RtrCode as Retailer_Code,
    OrderBooking.RtrName as Retailer_Name,
    OrderBooking.PrdCode as Product_Code,
    OrderBooking.OrderDate as Order_Date,
    OrderBooking.OrderNo as Order_No,
    Sum(OrderBooking.PrdQty) as Ord_Qty,
    Sum(OrderBooking.PrdGrossAmt) as Ord_Amt,
    SalesInvoiceOrders.SalInvNo as Invoice_No,
    max(OrderBooking.crt_dttm) as crt_dttm,
    max(OrderBooking.updt_dttm) as updt_dttm
    from itg_orderbooking OrderBooking 
    left join 
    (Select * from
    (Select 
    row_number() over ( partition by distcode, orderno, orderdate order by createddate, SalInvNo desc) rn,
    sio.* from itg_salesinvoiceorders sio) si1
    where si1.rn=1) SalesInvoiceOrders
    on OrderBooking.distcode=SalesInvoiceOrders.distcode
    and OrderBooking.orderno=SalesInvoiceOrders.orderno
    and OrderBooking.orderdate=SalesInvoiceOrders.orderdate
    left join 
    (Select * from
    (Select 
    row_number() over (partition by  distcode, csrtrcode, trim(upper(rtrname)) order by createddate desc) rn,
    distcode, csrtrcode, trim(upper(rtrname)) rtrname, rtrcode from itg_retailermaster m where m.actv_flg='Y') m1
    where m1.rn=1) retailermaster 
    on OrderBooking.distcode=retailermaster.distcode
    and OrderBooking.RtrCode=retailermaster.csrtrcode
    and trim(upper(OrderBooking.rtrname))=trim(upper(retailermaster.rtrname))
    group by 
    OrderBooking.DistCode,OrderBooking.SalesmanCode,OrderBooking.SalesRouteCode,retailermaster.RtrCode,
    OrderBooking.RtrName,OrderBooking.PrdCode,OrderBooking.OrderDate,OrderBooking.OrderNo,SalesInvoiceOrders.SalInvNo
    )src
)
select customer_code::varchar(50) as customer_code,
    salesman_code::varchar(100) as salesman_code,
    route_code::varchar(100) as route_code,
    retailer_code::varchar(100) as retailer_code,
    retailer_name::varchar(100) as retailer_name,
    product_code::varchar(50) as product_code,
    order_date::timestamp_ntz(9) as order_date,
    order_no::varchar(50) as order_no,
    ord_qty::number(38,0) as ord_qty,
    ord_amt::number(38,6) as ord_amt,
    invoice_no::varchar(50) as invoice_no,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
 from final