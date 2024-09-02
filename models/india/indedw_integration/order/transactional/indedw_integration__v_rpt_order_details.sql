{{ config(
  sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
) }}

with v_rpt_order_details_with_history as
(
    select * from {{ ref('indedw_integration__v_rpt_order_details_with_history') }}
),
final as
(
    SELECT 
    v_rpt_order_details_with_history.customer_code, 
    v_rpt_order_details_with_history.customer_name, 
    v_rpt_order_details_with_history.region_name, 
    v_rpt_order_details_with_history.zone_name, 
    v_rpt_order_details_with_history.territory_name, 
    v_rpt_order_details_with_history.salesman_code, 
    v_rpt_order_details_with_history.route_code, 
    v_rpt_order_details_with_history.retailer_code, 
    v_rpt_order_details_with_history.salesman_name, 
    v_rpt_order_details_with_history.route_name, 
    v_rpt_order_details_with_history.retailer_name, 
    v_rpt_order_details_with_history.product_code, 
    v_rpt_order_details_with_history.product_name, 
    v_rpt_order_details_with_history.franchise_name, 
    v_rpt_order_details_with_history.brand_name, 
    v_rpt_order_details_with_history.variant_name, 
    v_rpt_order_details_with_history.product_category_name, 
    v_rpt_order_details_with_history.mothersku_name, 
    v_rpt_order_details_with_history.invoice_no, 
    v_rpt_order_details_with_history.order_date, 
    v_rpt_order_details_with_history.order_no, 
    v_rpt_order_details_with_history.ord_dt_week, 
    v_rpt_order_details_with_history.ord_dt_month, 
    v_rpt_order_details_with_history.ord_dt_year, 
    v_rpt_order_details_with_history.invoice_product_quantity, 
    v_rpt_order_details_with_history.invoice_product_nr_amount, 
    v_rpt_order_details_with_history.invoice_tax_amount, 
    v_rpt_order_details_with_history.order_product_quantity, 
    v_rpt_order_details_with_history.order_product_nr_amount, 
    v_rpt_order_details_with_history.csrtrcode, 
    v_rpt_order_details_with_history.abi_ntid, 
    v_rpt_order_details_with_history.flm_ntid, 
    v_rpt_order_details_with_history.bdm_ntid, 
    v_rpt_order_details_with_history.rsm_ntid, 
    v_rpt_order_details_with_history.class_desc, 
    v_rpt_order_details_with_history.retailer_category_name, 
    v_rpt_order_details_with_history.channel_name, 
    v_rpt_order_details_with_history.cfa, 
    v_rpt_order_details_with_history.cfa_name 
    FROM v_rpt_order_details_with_history 
    WHERE ((v_rpt_order_details_with_history.ord_dt_year):: double precision >= (date_part(year,convert_timezone('UTC',current_timestamp())::timestamp_ntz) - (1):: double precision))
)
select * from final