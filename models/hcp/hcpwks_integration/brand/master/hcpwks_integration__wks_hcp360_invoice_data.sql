with 
edw_billing_fact as 
(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),
edw_customer_dim as 
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
edw_product_dim as 
(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
) ,
edw_retailer_calendar_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
trans as 
(
    select CASE
         WHEN brand_name = 'ORSL' THEN 'ORSL'
         WHEN brand_name = 'Johnson''s Baby' THEN 'JBABY'
         WHEN brand_name = 'AVEENO Baby' THEN 'DERMA'
         WHEN brand_name = 'AVEENO BODY' THEN 'DERMA'
       END AS Brand,
       mth_mm,
       customer_code,
       customer_name,  
       prd.franchise_name,
       product_category_name,
       variant_name,
       UPPER(region_name) as region,
       UPPER(zone_name) as zone,
       UPPER(territory_name) as territory,     
       SUM(CASE WHEN doc_currcy != 'INR' THEN subtotal_4*exrate_acc ELSE subtotal_4 END) as invoice_val
 from edw_billing_fact bill,
      edw_customer_dim cust,
      edw_product_dim prd,
      edw_retailer_calendar_dim cal
     
WHERE sls_org = '5100'
AND   BILL_TYPE IN ('S1','S2','ZC22','ZC2D','ZF2D','ZG22','ZG2D','ZL2D','ZL22','ZC3D','ZF2E','ZL3D','ZG3D','ZRSM','ZSMD')
AND   CAST(TO_CHAR(to_date(created_on),'YYYY') AS INT) >= (DATE_PART(YEAR,convert_timezone('UTC',current_timestamp())) -2)
AND   LTRIM(bill.sold_to,'0') = cust.customer_code (+)
AND   LTRIM(bill.material,'0') = prd.product_code (+)
AND   created_on = to_date(caldate)
AND brand_name in ('AVEENO Baby' ,'AVEENO BODY' , 'Johnson''s Baby' , 'ORSL' ) 
AND prd.product_code not in
(
'48001812'
,'48001816'
,'48001819'
,'79600715'
,'79600739'
,'48001813'
,'79614709'
,'79614711'
,'79614721'
,'79614730'
,'48304810'
,'79607607'
,'79607629'
,'79607606'
)
and cust.customer_code  not in (
 '135456'
,'135957'
,'136999'
)
group by 1,2,3,4,5,6,7,8,9,10 
),
final as 
(
    select 
    brand::varchar(5) as brand,
	mth_mm::number(18,0) as mth_mm,
	customer_code::varchar(20) as customer_code,
	customer_name::varchar(150) as customer_name,
	franchise_name::varchar(50) as franchise_name,
	product_category_name::varchar(150) as product_category_name,
	variant_name::varchar(150) as variant_name,
	region::varchar(75) as region,
	zone::varchar(75) as zone,
	territory::varchar(75) as territory,
	invoice_val::number(38,8) as invoice_val
    from trans
)
select * from final