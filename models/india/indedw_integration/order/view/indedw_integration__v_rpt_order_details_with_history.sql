{{
    config(
        materialized='view'
    )
}}

with edw_order_fact as
(
    select * from {{ ref('indedw_integration__edw_order_fact') }}
),
edw_dailysales_fact as
(
    select * from {{ ref('indedw_integration__edw_dailysales_fact') }}
),
edw_customer_dim as
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
edw_product_dim as
(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
edw_retailer_calendar_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
edw_route_dim as
(
    select * from {{ ref('indedw_integration__edw_route_dim') }}
),
edw_retailer_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_dim') }}
),
sales_invoice_orders as (
    select * from {{ ref('inditg_integration__itg_salesinvoiceorders') }}
),
final as
(
SELECT 
    NVL(oti.customer_code,sio.distcode) AS customer_code, 
    c.customer_name, 
    c.region_name, 
    c.zone_name, 
    c.territory_name, 
    oti.salesman_code, 
    oti.route_code, 
    oti.retailer_code, 
    rtdim.smname AS salesman_name, 
    rtdim.rmname AS route_name, 
    oti.retailer_name, 
    oti.product_code, 
    d.product_name, 
    d.franchise_name, 
    d.brand_name, 
    d.variant_name, 
    d.product_category_name, 
    d.mothersku_name, 
	oti.invoice_date,
    COALESCE(oti.invoice_no,oti.invoice_no_invoiced,sio.SalInvNo) AS invoice_no,
    NVL(oti.order_date,sio.orderdate) AS order_date,
    NVL(oti.order_no,sio.orderno) AS order_no,
    f.week AS ord_dt_week, 
    f.mth_mm AS ord_dt_month, 
    f.fisc_yr AS ord_dt_year, 
    oti.quantity AS invoice_product_quantity, 
    oti.gross_amt AS invoice_product_nr_amount, 
    oti.tax_amt AS invoice_tax_amount, 
    oti.ord_qty AS order_product_quantity, 
    oti.ord_amt AS order_product_nr_amount, 
    oti.product_code_invoiced,
    oti.remarks as order_source,
    rtld.csrtrcode, 
    NULL  AS abi_ntid, 
    NULL  AS flm_ntid, 
    NULL  AS bdm_ntid, 
    NULL  AS rsm_ntid, 
    rtld.class_desc, 
    rtld.retailer_category_name, 
    rtld.channel_name, 
    c.cfa, 
    c.cfa_name,
    CASE WHEN oti.order_no IS NULL THEN 'Invoice'  ELSE 'Order' END AS datasource,
	   CASE WHEN oti.order_no IS NOT NULL AND oti.quantity IS NOT NULL THEN 'Ordered and Invoiced at SKU level'
	        WHEN oti.order_no IS NOT NULL AND oti.invoice_no IS NULL AND oti.quantity IS NULL THEN 'Ordered and Not Invoiced'
	        WHEN oti.order_no IS NOT NULL AND oti.invoice_no IS NOT NULL AND oti.quantity IS NULL THEN 'Ordered and Invoiced at different SKU level'			
            WHEN oti.order_no IS NULL AND sio.orderno IS NOT NULL THEN 'Ordered and Invoiced at different SKU level'
            WHEN oti.order_no IS NULL AND sio.orderno IS NULL THEN 'Not Ordered but Invoiced'
			ELSE NULL END AS desc_flag
    FROM 
    (
        (SELECT NVL(b.customer_code,a.customer_code) AS customer_code,
             b.salesman_code,
             b.route_code,
             b.retailer_code,
             b.retailer_name,
           --  NVL(b.product_code,a.product_code) AS product_code,
		     b.product_code,
           --  NVL(b.invoice_no,a.invoice_no) AS invoice_no,
		     b.invoice_no,
             b.order_date,
             b.order_no,
             b.ord_qty,
             b.ord_amt,
             b.remarks,
             a.customer_code AS customer_code_invoiced,
             a.invoice_no AS invoice_no_invoiced,
			 a.invoice_date,
             a.product_code AS product_code_invoiced,
             a.quantity,
             a.gross_amt,
             a.tax_amt
             FROM(
        SELECT TRIM(edw_order_fact.customer_code) AS customer_code,
                   TRIM(edw_order_fact.retailer_code) AS retailer_code,
                   TRIM(edw_order_fact.retailer_name) AS retailer_name,
                   TRIM(edw_order_fact.product_code) AS product_code,
                   TRIM(edw_order_fact.invoice_no) AS invoice_no,
                   TRIM(edw_order_fact.salesman_code) AS salesman_code,
                   edw_order_fact.order_date AS order_date,
                   TRIM(edw_order_fact.route_code) AS route_code,
                   TRIM(edw_order_fact.order_no) AS order_no,
                   TRIM(edw_order_fact.remarks) AS remarks,
                    sum(edw_order_fact.ord_qty) AS ord_qty, 
                    sum(edw_order_fact.ord_amt) AS ord_amt 
                    FROM edw_order_fact edw_order_fact 
                    WHERE EXTRACT(YEAR FROM order_date) >= 2024 
                    GROUP BY 
                    edw_order_fact.customer_code, 
                    edw_order_fact.retailer_code, 
                    edw_order_fact.retailer_name, 
                    edw_order_fact.product_code, 
                    edw_order_fact.invoice_no, 
                    edw_order_fact.salesman_code, 
                    edw_order_fact.route_code, 
                    edw_order_fact.order_date, 
                    edw_order_fact.order_no,
                    edw_order_fact.remarks
                ) b 
            FULL OUTER JOIN (
                    SELECT 
                    edw_dailysales_fact.customer_code, 
                    edw_dailysales_fact.product_code, 
                    edw_dailysales_fact.invoice_no,
                    edw_dailysales_fact.invoice_date, 
                    sum(edw_dailysales_fact.quantity) AS quantity, 
                    sum(edw_dailysales_fact.gross_amt) AS gross_amt, 
                    sum(edw_dailysales_fact.tax_amt) AS tax_amt 
                    FROM 
                    edw_dailysales_fact edw_dailysales_fact 
                    WHERE saleflag = 'D' AND LEFT(edw_dailysales_fact.invoice_date,4) >= '2024'                     
                    GROUP BY 
                    edw_dailysales_fact.customer_code, 
                    edw_dailysales_fact.product_code, 
                    edw_dailysales_fact.invoice_no,
                    edw_dailysales_fact.invoice_date
                ) a 
                ON ((((TRIM(b.customer_code):: text = (a.customer_code):: text) 
                    AND (TRIM(b.product_code):: text = (a.product_code):: text)) 
                    AND (TRIM(b.invoice_no):: text = (a.invoice_no):: text)
                    ))) oti
                LEFT JOIN (SELECT TRIM(si.distcode) AS distcode, TRIM(si.salinvno) AS salinvno, TRIM(si.orderno) AS orderno, si.orderdate, 
                    ROW_NUMBER() OVER (PARTITION BY TRIM(distcode), TRIM(SalInvNo) ORDER BY createddate,TRIM(orderno) DESC) rn
                    FROM sales_invoice_orders si) sio
                     ON oti.customer_code_invoiced = TRIM(sio.distcode)
                     AND oti.invoice_no_invoiced = TRIM(sio.SalInvNo)
                    AND sio.rn = 1
                LEFT JOIN edw_customer_dim c ON (((c.customer_code):: text = (NVL(oti.customer_code::TEXT, oti.customer_code_invoiced::TEXT)))) 
                LEFT JOIN edw_product_dim d ON (((d.product_code):: text = NVL(oti.product_code::TEXT,oti.product_code_invoiced::TEXT))) 
                LEFT JOIN edw_retailer_calendar_dim f ON ((f.day = (replace(((to_date(NVL(oti.order_date,sio.orderdate))):: character varying):: text,('-' :: character varying):: text,('' :: character varying):: text)):: integer))
            LEFT JOIN (
                SELECT 
                derived_table1.distcode, 
                derived_table1.smcode, 
                derived_table1.rmcode, 
                derived_table1.smname, 
                derived_table1.rmname, 
                derived_table1.crt_dttm, 
                derived_table1.rn 
                FROM 
                (
                    SELECT 
                    edw_route_dim.distcode, 
                    edw_route_dim.smcode, 
                    edw_route_dim.rmcode, 
                    edw_route_dim.smname, 
                    edw_route_dim.rmname, 
                    edw_route_dim.crt_dttm, 
                    row_number() OVER(PARTITION BY edw_route_dim.distcode,edw_route_dim.smcode,edw_route_dim.rmcode ORDER BY edw_route_dim.crt_dttm) AS rn 
                    FROM edw_route_dim edw_route_dim) derived_table1 
                WHERE (derived_table1.rn = 1)) rtdim 
                ON (((((oti.customer_code):: text = (rtdim.distcode):: text) AND ((oti.route_code):: text = (rtdim.rmcode):: text)) 
                AND ((oti.salesman_code):: text = (rtdim.smcode):: text))) 
            LEFT JOIN (
            SELECT 
                edw_retailer_dim.retailer_code, 
                edw_retailer_dim.start_date, 
                edw_retailer_dim.end_date, 
                edw_retailer_dim.customer_code, 
                edw_retailer_dim.customer_name, 
                edw_retailer_dim.retailer_name, 
                edw_retailer_dim.retailer_address1, 
                edw_retailer_dim.retailer_address2, 
                edw_retailer_dim.retailer_address3, 
                edw_retailer_dim.region_code, 
                edw_retailer_dim.region_name, 
                edw_retailer_dim.zone_code, 
                edw_retailer_dim.zone_name, 
                edw_retailer_dim.zone_classification, 
                edw_retailer_dim.territory_code, 
                edw_retailer_dim.territory_name, 
                edw_retailer_dim.territory_classification, 
                edw_retailer_dim.state_code, 
                edw_retailer_dim.state_name, 
                edw_retailer_dim.town_code, 
                edw_retailer_dim.town_name, 
                edw_retailer_dim.town_classification, 
                edw_retailer_dim.class_code, 
                edw_retailer_dim.class_desc, 
                edw_retailer_dim.outlet_type, 
                edw_retailer_dim.channel_code, 
                edw_retailer_dim.channel_name, 
                edw_retailer_dim.business_channel, 
                edw_retailer_dim.loyalty_desc, 
                edw_retailer_dim.registration_date, 
                edw_retailer_dim.status_cd, 
                edw_retailer_dim.status_desc, 
                edw_retailer_dim.csrtrcode, 
                edw_retailer_dim.crt_dttm, 
                edw_retailer_dim.updt_dttm, 
                edw_retailer_dim.actv_flg, 
                edw_retailer_dim.retailer_category_cd, 
                edw_retailer_dim.retailer_category_name 
            FROM 
                edw_retailer_dim 
            WHERE (edw_retailer_dim.actv_flg = 'Y')) rtld 
            ON (((oti.customer_code):: text = (rtld.customer_code):: text) 
            AND ((oti.retailer_code):: text = (rtld.retailer_code):: text))
        
    )
)
select * from final