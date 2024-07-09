{{
    config(
        materialized='view'
    )
}}

with edw_customer_dim as
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
edw_calendar_dim as 
(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
itg_query_parameters as
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
edw_billing_fact as
(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),
edw_product_dim as
(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
final as
(
    SELECT calendar_dim.DATE,
	calendar_dim.week,
	calendar_dim.qtr,
	calendar_dim.month_nm_shrt,
	calendar_dim.fisc_yr,
	t0.bill_type,
	(t0.customer_code)::CHARACTER VARYING AS customer_code_1,
	t0.inv_qty,
	t0.base_uom AS base_uom_1,
	t0.invoice_volume_pc,
	t0.subtotal_4,
	t0.exratexacc,
	t0.loc_currcy,
	t0.exrate_acc,
	t0.distr_chnl,
	t0.doc_currcy,
	t0.material,
	t0.created_on,
	t0.sls_off,
	t0.invoice_val,
	customer_dim.customer_code,
	customer_dim.region_name,
	customer_dim.zone_name,
	customer_dim.territory_name,
	customer_dim.state_name,
	customer_dim.town_name,
	customer_dim.town_classification,
	customer_dim.type_code,
	customer_dim.type_name,
	customer_dim.customer_name,
	customer_dim.active_flag,
	customer_dim.active_start_date,
	customer_dim.super_stockiest,
	customer_dim.direct_account_flag,
	customer_dim.abi_code,
	customer_dim.abi_name,
	customer_dim.num_of_retailers,
	customer_dim.customer_type,
	customer_dim.psnonps,
	customer_dim.suppliedby,
	t1.product_code,
	t1.product_name,
	t1.product_desc,
	t1.franchise_name,
	t1.brand_name,
	t1.product_category_name,
	t1.variant_name,
	t1.mothersku_name,
	t1.uom,
	t1.std_nr,
	t1.case_lot,
	t1.sale_uom,
	t1.sale_conversion_factor,
	t1.base_uom,
	t1.int_uom,
	t1.gross_wt,
	t1.net_wt,
	t1.active_flag AS active_flag_edw_product_dim
FROM ((((SELECT edw_calendar_dim.cal_day AS DATE,
					ltrim(right(((edw_calendar_dim.cal_wk)::CHARACTER VARYING)::TEXT, 2), ('0'::CHARACTER VARYING)::TEXT) AS week,
					edw_calendar_dim.cal_qtr_1 AS qtr,
					edw_calendar_dim.pstng_per AS month_num,
					ltrim(left(to_char((edw_calendar_dim.cal_day)::DATE, ('month'::CHARACTER VARYING)::TEXT), 3), ('0'::CHARACTER VARYING)::TEXT) AS month_nm_shrt,
					edw_calendar_dim.fisc_yr
				FROM edw_calendar_dim
				WHERE (((edw_calendar_dim.fisc_yr)::CHARACTER VARYING)::TEXT > substring(((TO_DATE(add_months(current_timestamp()::DATE, ((
													- (SELECT (itg_query_parameters.parameter_value)::INTEGER AS parameter_value
														FROM itg_query_parameters
														WHERE (((upper((itg_query_parameters.country_code)::TEXT) = ('IN'::CHARACTER VARYING)::TEXT) AND (upper((itg_query_parameters.parameter_type)::TEXT) = ('ORSL_REPORTING'::CHARACTER VARYING)::TEXT)) AND (upper((itg_query_parameters.parameter_name)::TEXT) = ('ORSL_REPORTING_DATE_RANGE'::CHARACTER VARYING)::TEXT)))))::BIGINT)))::CHARACTER VARYING)::TEXT,1,4))
				GROUP BY edw_calendar_dim.cal_day,
					ltrim(right(((edw_calendar_dim.cal_wk)::CHARACTER VARYING)::TEXT, 2), ('0'::CHARACTER VARYING)::TEXT),
					edw_calendar_dim.cal_qtr_1,
					edw_calendar_dim.pstng_per,
					ltrim(left(to_char((edw_calendar_dim.cal_day)::DATE, ('month'::CHARACTER VARYING)::TEXT), 3), ('0'::CHARACTER VARYING)::TEXT),
					edw_calendar_dim.fisc_yr
				) calendar_dim JOIN (
				SELECT edw_billing.bill_type,
					edw_billing.subtotal_4,
					edw_billing.customer_code,
					edw_billing.exrate_acc,
					edw_billing.invoice_volume_pc,
					edw_billing.loc_currcy,
					edw_billing.inv_qty,
					edw_billing.base_uom,
					edw_billing.exratexacc,
					edw_billing.distr_chnl,
					edw_billing.doc_currcy,
					edw_billing.material,
					edw_billing.created_on,
					edw_billing.sls_off,
					edw_billing.invoice_val,
					CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)|| ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)|| ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)|| ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)|| ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT)|| ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT)AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT))
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT)|| ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT))
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT)|| ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT))
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT)|| ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT))
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT)) 
						THEN ((((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT)|| ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT))
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT))
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)|| ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT)AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT)AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('13'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT)AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('14'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('15'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('16'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT)AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('17'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('18'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('19'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT)AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('20'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('21'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('22'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT)AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('23'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('24'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('25'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('26'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('27'::CHARACTER VARYING)::TEXT))
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('28'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('29'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('30'::CHARACTER VARYING)::TEXT))
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('31'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT))
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT))
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT))
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT))
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('13'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT)AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('14'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('15'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('16'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT)AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('17'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('18'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('19'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT)AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('20'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('21'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('22'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT)AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('23'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('24'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('25'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT)AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('26'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('27'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('28'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT)AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('29'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('30'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('31'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('2'::CHARACTER VARYING)::TEXT))
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('3'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('4'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('5'::CHARACTER VARYING)::TEXT))
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('6'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('7'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('8'::CHARACTER VARYING)::TEXT))
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('9'::CHARACTER VARYING)::TEXT)) 
						THEN (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('10'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('11'::CHARACTER VARYING)::TEXT))
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('13'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('14'::CHARACTER VARYING)::TEXT))
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('15'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('16'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)|| ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('17'::CHARACTER VARYING)::TEXT))
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('18'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('19'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT)AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('20'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('21'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('22'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT)AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('23'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('24'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('25'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT)AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('26'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('27'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('28'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT)AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('29'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT)
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('30'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
					ELSE CASE WHEN ((((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('12'::CHARACTER VARYING)::TEXT) AND (((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT = ('31'::CHARACTER VARYING)::TEXT)) 
						THEN ((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) ELSE (((((date_part(year, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT || ('0'::CHARACTER VARYING)::TEXT) || ((date_part(month, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) || ((date_part(day, (edw_billing.created_on)::DATE))::CHARACTER VARYING)::TEXT) 
				END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END AS temp1
	FROM (
					SELECT edw_billing_fact.bill_type,
right((edw_billing_fact.sold_to)::TEXT, 6) AS customer_code,
edw_billing_fact.inv_qty,
edw_billing_fact.base_uom,
edw_billing_fact.bill_qty AS invoice_volume_pc,
edw_billing_fact.subtotal_4,
edw_billing_fact.exratexacc,
edw_billing_fact.loc_currcy,
edw_billing_fact.exrate_acc,
edw_billing_fact.distr_chnl,
edw_billing_fact.doc_currcy,
edw_billing_fact.material,
edw_billing_fact.created_on,
edw_billing_fact.sls_off,
CASE WHEN ((edw_billing_fact.doc_currcy)::TEXT <> ('INR'::CHARACTER VARYING)::TEXT) THEN (edw_billing_fact.subtotal_4 * edw_billing_fact.exrate_acc) ELSE edw_billing_fact.subtotal_4 END AS invoice_val
					FROM edw_billing_fact
					WHERE (
	edw_billing_fact.bill_type IN (
		SELECT itg_query_parameters.parameter_value
		FROM itg_query_parameters
		WHERE (((upper((itg_query_parameters.country_code)::TEXT) = ('IN'::CHARACTER VARYING)::TEXT) AND (upper((itg_query_parameters.parameter_type)::TEXT) = ('ORSL_REPORTING'::CHARACTER VARYING)::TEXT)) AND (upper((itg_query_parameters.parameter_name)::TEXT) = ('ORSL_REPORTING_BILL_TYPE'::CHARACTER VARYING)::TEXT))
		)
	))edw_billing) t0
				ON (((((TRUNC((((((((date_part(year, (calendar_dim.DATE)::DATE))::CHARACTER VARYING)::TEXT || (CASE WHEN (date_part(month, (calendar_dim.DATE)::DATE) < 10) THEN 0::CHARACTER VARYING ELSE ''::CHARACTER VARYING END)::TEXT) || ((date_part(month, (calendar_dim.DATE)::DATE))::CHARACTER VARYING)::TEXT) || (CASE WHEN (date_part(day, (calendar_dim.DATE)::DATE) < 10) THEN 0::CHARACTER VARYING ELSE ''::CHARACTER VARYING END)::TEXT) || ((date_part(day, (calendar_dim.DATE)::DATE))::CHARACTER VARYING)::TEXT))::DOUBLE PRECISION))::BIGINT)::CHARACTER VARYING)::TEXT = t0.temp1))) LEFT JOIN edw_customer_dim customer_dim
			ON ((t0.customer_code = (customer_dim.customer_code)::TEXT))
		) LEFT JOIN (
		SELECT product_dim.product_code,
			product_dim.product_name,
			product_dim.product_category_code,
			product_dim.product_desc,
			product_dim.active_flag,
			product_dim.product_category_name,
			product_dim.brand_name,
			product_dim.franchise_name,
			product_dim.sale_conversion_factor,
			product_dim.mothersku_name,
			product_dim.variant_name,
			product_dim.uom,
			product_dim.std_nr,
			product_dim.case_lot,
			product_dim.sale_uom,
			product_dim.base_uom,
			product_dim.int_uom,
			product_dim.gross_wt,
			product_dim.net_wt,
			CASE WHEN (length((product_dim.product_code)::TEXT) = 8) THEN ((('0000000000'::CHARACTER VARYING)::TEXT || (product_dim.product_code)::TEXT))::CHARACTER VARYING ELSE CASE WHEN (length((product_dim.product_code)::TEXT) = 9) THEN ((('000000000'::CHARACTER VARYING)::TEXT || (product_dim.product_code)::TEXT))::CHARACTER VARYING ELSE CASE WHEN (length((product_dim.product_code)::TEXT) = 10) THEN ((('00000000'::CHARACTER VARYING)::TEXT || (product_dim.product_code)::TEXT))::CHARACTER VARYING ELSE NULL::CHARACTER VARYING END END END AS temp2
		FROM edw_product_dim product_dim
		) t1
		ON (((t0.material)::TEXT = (t1.temp2)::TEXT))
	)
)
select * from final