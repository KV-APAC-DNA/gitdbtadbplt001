with itg_pf_clstk_mth_ds as
(
    select * from {{ ref('inditg_integration__itg_pf_clstk_mth_ds') }}
),
edw_customer_dim as 
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
edw_product_dim as
(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
edw_billing_fact as 
(
    select * from {{ ref('aspedw_access__edw_billing_fact') }}
),
edw_retailer_calendar_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
transformed as
(
SELECT 'Stock' AS Dataset,
       CAST(itg_pf_clstk_mth_ds.year ||lpad (itg_pf_clstk_mth_ds.month,2,'0') AS INTEGER) mnth_mm,
       NULL AS week,
       UPPER(region_name) AS region_name,
       UPPER(zone_name) AS zone_name,
       UPPER(territory_name) AS territory_name,
       itg_pf_clstk_mth_ds.customer_code,
       NULL AS retailer_code,
       customer_name,
       type_name AS customer_type,
       franchise_name,
       brand_name,
       mothersku_name,
       variant_name,
       NULL AS retailer_name,
       NULL AS channel_name,
       SUM(itg_pf_clstk_mth_ds.cl_stck_value) AS cl_stck_value,
       NULL AS billing_value
FROM itg_pf_clstk_mth_ds,
     edw_customer_dim cust,
     edw_product_dim prd
WHERE itg_pf_clstk_mth_ds.year >= (DATE_PART(YEAR,current_timestamp()) -2)

AND   itg_pf_clstk_mth_ds.customer_code = cust.customer_code (+)
AND   itg_pf_clstk_mth_ds.product_code = prd.product_code (+)
GROUP BY itg_pf_clstk_mth_ds.month,
         itg_pf_clstk_mth_ds.year,
         week,
         region_name,
         zone_name,
         territory_name,
         itg_pf_clstk_mth_ds.customer_code,
         customer_name,
         type_name,
         franchise_name,
         brand_name,
         mothersku_name,
         variant_name
UNION ALL
SELECT 'Invoice' AS dataset,
       mth_mm,
       CAST(week AS VARCHAR),
       UPPER(region_name),
       UPPER(zone_name),
       UPPER(territory_name),
       customer_code,
       NULL AS retailer_code,
       customer_name,
       type_name,
       franchise_name,
       brand_name,
       mothersku_name,
       variant_name,
       NULL AS retailer_name,
       NULL AS channel_name,
       NULL AS closing_stock,
       SUM(CASE WHEN doc_currcy != 'INR' THEN subtotal_4*exrate_acc ELSE subtotal_4 END) billing_value
FROM edw_billing_fact bill,
     edw_customer_dim cust,
     edw_product_dim prd,
     edw_retailer_calendar_dim cal
WHERE sls_org = '5100'
AND   BILL_TYPE IN ('S1','S2','ZC22','ZC2D','ZF2D','ZG22','ZG2D','ZL2D','ZL22','ZC3D','ZF2E','ZL3D','ZG3D','ZRSM','ZSMD')
AND   CAST(TO_CHAR(to_date(created_on),'YYYY') AS INT) >= (DATE_PART(YEAR,current_timestamp()) -2)
AND   LTRIM(bill.sold_to,'0') = cust.customer_code (+)
AND   LTRIM(bill.material,'0') = prd.product_code (+)
AND   created_on = to_date(caldate)

GROUP BY mth_mm,
         week,
         customer_code,
         customer_name,
         type_name,
         region_name,
         zone_name,
         territory_name,
         franchise_name,
         brand_name,
         mothersku_name,
         variant_name
),
final as
(
    select
        dataset::varchar(7) as dataset,
        mnth_mm::number(18,0) as mnth_mm,
        week::varchar(11) as week,
        region_name::varchar(75) as region_name,
        zone_name::varchar(75) as zone_name,
        territory_name::varchar(75) as territory_name,
        customer_code::varchar(50) as customer_code,
        retailer_code::varchar(1) as retailer_code,
        customer_name::varchar(150) as customer_name,
        customer_type::varchar(50) as customer_type,
        franchise_name::varchar(50) as franchise_name,
        brand_name::varchar(50) as brand_name,
        mothersku_name::varchar(150) as mothersku_name,
        variant_name::varchar(150) as variant_name,
        retailer_name::varchar(1) as retailer_name,
        channel_name::varchar(1) as channel_name,
        cl_stck_value::number(38,2) as cl_stck_value,
        billing_value::number(38,8) as billing_value
    from transformed
)
select * from final
