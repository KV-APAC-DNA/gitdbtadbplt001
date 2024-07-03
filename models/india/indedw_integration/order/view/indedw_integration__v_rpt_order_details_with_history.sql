{{
    config(
        materialized='view'
    )
}}

with edw_order_fact as
(
    select * from snapindedw_integration.edw_order_fact
    --{{ ref('indedw_integration__edw_order_fact') }}
),
edw_dailysales_fact as
(
    select * from snapindedw_integration.edw_dailysales_fact
    --{{ ref('indedw_integration__edw_dailysales_fact') }}
),
edw_customer_dim as
(
    select * from snapindedw_integration.edw_customer_dim
    --{{ ref('indedw_integration__edw_customer_dim') }}
),
edw_product_dim as
(
    select * from snapindedw_integration.edw_product_dim
    --{{ ref('indedw_integration__edw_product_dim') }}
),
edw_retailer_calendar_dim as
(
    select * from snapindedw_integration.edw_retailer_calendar_dim
    --{{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
edw_route_dim as
(
    select * from snapindedw_integration.edw_route_dim
),
edw_retailer_dim as
(
    select * from snapindedw_integration.edw_retailer_dim
    --{{ ref('indedw_integration__edw_retailer_dim') }}
),
final as
(
    SELECT 
    b.customer_code, 
    c.customer_name, 
    c.region_name, 
    c.zone_name, 
    c.territory_name, 
    b.salesman_code, 
    b.route_code, 
    b.retailer_code, 
    rtdim.smname AS salesman_name, 
    rtdim.rmname AS route_name, 
    b.retailer_name, 
    b.product_code, 
    d.product_name, 
    d.franchise_name, 
    d.brand_name, 
    d.variant_name, 
    d.product_category_name, 
    d.mothersku_name, 
    b.invoice_no, 
    b.order_date, 
    b.order_no, 
    f.week AS ord_dt_week, 
    f.mth_mm AS ord_dt_month, 
    f.fisc_yr AS ord_dt_year, 
    a.quantity AS invoice_product_quantity, 
    a.gross_amt AS invoice_product_nr_amount, 
    a.tax_amt AS invoice_tax_amount, 
    b.ord_qty AS order_product_quantity, 
    b.ord_amt AS order_product_nr_amount, 
    rtld.csrtrcode, 
    NULL  AS abi_ntid, 
    NULL  AS flm_ntid, 
    NULL  AS bdm_ntid, 
    NULL  AS rsm_ntid, 
    rtld.class_desc, 
    rtld.retailer_category_name, 
    rtld.channel_name, 
    c.cfa, 
    c.cfa_name 
    FROM 
    (((((((SELECT edw_order_fact.customer_code, 
                    edw_order_fact.retailer_code, 
                    edw_order_fact.retailer_name, 
                    edw_order_fact.product_code, 
                    edw_order_fact.invoice_no, 
                    edw_order_fact.salesman_code, 
                    edw_order_fact.order_date, 
                    edw_order_fact.route_code, 
                    edw_order_fact.order_no, 
                    sum(edw_order_fact.ord_qty) AS ord_qty, 
                    sum(edw_order_fact.ord_amt) AS ord_amt 
                    FROM edw_order_fact edw_order_fact 
                    GROUP BY 
                    edw_order_fact.customer_code, 
                    edw_order_fact.retailer_code, 
                    edw_order_fact.retailer_name, 
                    edw_order_fact.product_code, 
                    edw_order_fact.invoice_no, 
                    edw_order_fact.salesman_code, 
                    edw_order_fact.route_code, 
                    edw_order_fact.order_date, 
                    edw_order_fact.order_no
                ) b 
                LEFT JOIN (
                    SELECT 
                    edw_dailysales_fact.customer_code, 
                    edw_dailysales_fact.product_code, 
                    edw_dailysales_fact.invoice_no, 
                    sum(edw_dailysales_fact.quantity) AS quantity, 
                    sum(edw_dailysales_fact.gross_amt) AS gross_amt, 
                    sum(edw_dailysales_fact.tax_amt) AS tax_amt 
                    FROM 
                    edw_dailysales_fact edw_dailysales_fact 
                    GROUP BY 
                    edw_dailysales_fact.customer_code, 
                    edw_dailysales_fact.product_code, 
                    edw_dailysales_fact.invoice_no
                ) a ON (((((b.customer_code):: text = (a.customer_code):: text) 
                    AND ((b.product_code):: text = (a.product_code):: text)) 
                    AND ((b.invoice_no):: text = (a.invoice_no):: text)
                    ))) 
                LEFT JOIN edw_customer_dim c ON (((c.customer_code):: text = (b.customer_code):: text))) 
                LEFT JOIN edw_product_dim d ON (((d.product_code):: text = (b.product_code):: text))) 
                LEFT JOIN edw_retailer_calendar_dim f ON ((f.day = (replace(((to_date(b.order_date)):: character varying):: text,('-' :: character varying):: text,('' :: character varying):: text)):: integer))) 
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
                ON (((((b.customer_code):: text = (rtdim.distcode):: text) AND ((b.route_code):: text = (rtdim.rmcode):: text)) 
                AND ((b.salesman_code):: text = (rtdim.smcode):: text)))) 
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
            ON ((((b.customer_code):: text = (rtld.customer_code):: text) 
            AND ((b.retailer_code):: text = (rtld.retailer_code):: text))))
)

select * from final