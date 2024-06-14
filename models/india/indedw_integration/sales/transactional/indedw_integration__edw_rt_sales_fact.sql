with itg_ruralstoreorderdetail as
(
    select * from inditg_integration.itg_ruralstoreorderdetail
),
itg_ruralstoreorderheader as
(
    select * from inditg_integration.itg_ruralstoreorderheader
),
final as 
(
    SELECT 
	case when a.retailerid is null or trim(a.retailerid)='' then 'Unknown' else a.retailerid end as SubD_Ret_Code, 
	case when a.RSD_Code is null or trim(a.RSD_Code)='' then 'Unknown' else a.RSD_Code end as SubD_Code, 
	b.ordd_distributorcode as Customer_Code,
	b.UserCode  as User_Code,
	b.ProductId as Product_Code,
	to_date(b.OrderDate) as Order_Date,
	b.OrderID   as Order_ID,
	b.Qty       as Qty,
	b.Price     as Price,
	b.NetPrice  as Achievement_Amt,
	'1' as Lines,
	b.UserCode||'_'||a.RetailerID as Buying_Retailers,
    b.UserCode||'_'||b.OrderID as Bills,
	b.crt_dttm AS CRT_DTTM,
	convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM
    FROM itg_ruralstoreorderdetail b 
    left join 
	(Select * from
	(Select 
	row_number() over (partition by orderid order by orderdate desc) rn,
	* from itg_ruralstoreorderheader)
	where rn=1) a
    on a.orderid=b.orderid
)
select subd_ret_code::varchar(100) as subd_ret_code,
    subd_code::varchar(50) as subd_code,
    customer_code::varchar(100) as customer_code,
    user_code::varchar(100) as user_code,
    product_code::varchar(100) as product_code,
    order_date::timestamp_ntz(9) as order_date,
    order_id::varchar(100) as order_id,
    qty::number(38,0) as qty,
    price::number(18,2) as price,
    achievement_amt::number(18,2) as achievement_amt,
    lines::varchar(1) as lines,
    buying_retailers::varchar(151) as buying_retailers,
    bills::varchar(151) as bills,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
from final