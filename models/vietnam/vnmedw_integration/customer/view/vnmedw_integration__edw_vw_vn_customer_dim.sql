with edw_customer_sales_dim as(
	select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }} 
),
edw_gch_customerhierarchy as (
select * from {{ ref('aspwks_integration__wks_edw_gch_customerhierarchy') }} 
),
edw_customer_base_dim as (
select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_company_dim as(
select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_dstrbtn_chnl as (
select * from {{ ref('aspedw_integration__edw_dstrbtn_chnl') }}
),
edw_sales_org_dim as (
select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
edw_customer_sales_dim as (
select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_code_descriptions as(
select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
edw_subchnl_retail_env_mapping as(
select * from {{ source('aspedw_integration', 'edw_subchnl_retail_env_mapping') }}
),

a as(

		SELECT edw_customer_sales_dim.cust_num
			,min((
					CASE 
						WHEN (
								((edw_customer_sales_dim.cust_del_flag)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
								OR (
									(edw_customer_sales_dim.cust_del_flag IS NULL)
									AND (NULL IS NULL)
									)
								)
							THEN 'O'::CHARACTER VARYING
						WHEN (
								((edw_customer_sales_dim.cust_del_flag)::TEXT = (''::CHARACTER VARYING)::TEXT)
								OR (
									(edw_customer_sales_dim.cust_del_flag IS NULL)
									AND ('' IS NULL)
									)
								)
							THEN 'O'::CHARACTER VARYING
						ELSE edw_customer_sales_dim.cust_del_flag
						END
					)::TEXT) AS cust_del_flag
		FROM edw_customer_sales_dim
		WHERE ((edw_customer_sales_dim.sls_org)::TEXT = ('260S'::CHARACTER VARYING)::TEXT)
		GROUP BY edw_customer_sales_dim.cust_num
		
),
transformed as(
SELECT ecbd.cust_num AS sap_cust_id
	,ecbd.cust_nm AS sap_cust_nm
	,ecsd.sls_org AS sap_sls_org
	,ecd.company AS sap_cmp_id
	,ecd.ctry_key AS sap_cntry_cd
	,ecd.ctry_nm AS sap_cntry_nm
	,ecbd.addr AS sap_addr
	,ecbd.rgn AS sap_region
	,ecbd.dstrc AS sap_state_cd
	,ecbd.city AS sap_city
	,ecbd.pstl_cd AS sap_post_cd
	,ecsd.dstr_chnl AS sap_chnl_cd
	,edc.txtsh AS sap_chnl_desc
	,ecsd.sls_ofc AS sap_sls_office_cd
	,ecsd.sls_ofc_desc AS sap_sls_office_desc
	,ecsd.sls_grp AS sap_sls_grp_cd
	,ecsd.sls_grp_desc AS sap_sls_grp_desc
	,ecsd.crncy_key AS sap_curr_cd
	,ecsd.prnt_cust_key AS sap_prnt_cust_key
	,cddes_pck.code_desc AS sap_prnt_cust_desc
	,ecsd.chnl_key AS sap_cust_chnl_key
	,cddes_chnl.code_desc AS sap_cust_chnl_desc
	,ecsd.sub_chnl_key AS sap_cust_sub_chnl_key
	,cddes_subchnl.code_desc AS sap_sub_chnl_desc
	,ecsd.go_to_mdl_key AS sap_go_to_mdl_key
	,cddes_gtm.code_desc AS sap_go_to_mdl_desc
	,ecsd.bnr_key AS sap_bnr_key
	,cddes_bnrkey.code_desc AS sap_bnr_desc
	,ecsd.bnr_frmt_key AS sap_bnr_frmt_key
	,cddes_bnrfmt.code_desc AS sap_bnr_frmt_desc
	,subchnl_retail_env.retail_env
	,egch.gcgh_region AS gch_region
	,egch.gcgh_cluster AS gch_cluster
	,egch.gcgh_subcluster AS gch_subcluster
	,egch.gcgh_market AS gch_market
	,egch.gcch_retail_banner AS gch_retail_banner
FROM edw_gch_customerhierarchy egch
	,edw_customer_base_dim ecbd
	,edw_company_dim ecd
	,edw_dstrbtn_chnl edc
	,edw_sales_org_dim esod
	, a
	,(
		(
			(
				(
					(
						(
							edw_customer_sales_dim ecsd LEFT JOIN edw_code_descriptions cddes_pck ON (((cddes_pck.code)::TEXT = (ecsd.prnt_cust_key)::TEXT))
							) LEFT JOIN edw_code_descriptions cddes_bnrkey ON (((cddes_bnrkey.code)::TEXT = (ecsd.bnr_key)::TEXT))
						) LEFT JOIN edw_code_descriptions cddes_bnrfmt ON (((cddes_bnrfmt.code)::TEXT = (ecsd.bnr_frmt_key)::TEXT))
					) LEFT JOIN edw_code_descriptions cddes_chnl ON (((cddes_chnl.code)::TEXT = (ecsd.chnl_key)::TEXT))
				) LEFT JOIN edw_code_descriptions cddes_gtm ON (((cddes_gtm.code)::TEXT = (ecsd.go_to_mdl_key)::TEXT))
			) LEFT JOIN (
			edw_code_descriptions cddes_subchnl LEFT JOIN edw_subchnl_retail_env_mapping subchnl_retail_env ON ((upper((subchnl_retail_env.sub_channel)::TEXT) = upper((cddes_subchnl.code_desc)::TEXT)))
			) ON (((cddes_subchnl.code)::TEXT = (ecsd.sub_chnl_key)::TEXT))
		)
WHERE (
		(
			(
				(
					(
						(
							(
								(
									(
										(
											(
												(
													(
														((egch.customer)::TEXT = (ecbd.cust_num)::TEXT)
														AND ((ecsd.cust_num)::TEXT = (ecbd.cust_num)::TEXT)
														)
													AND (
														(
															CASE 
																WHEN (
																		((ecsd.cust_del_flag)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
																		OR (
																			(ecsd.cust_del_flag IS NULL)
																			AND (NULL IS NULL)
																			)
																		)
																	THEN 'O'::CHARACTER VARYING
																WHEN (
																		((ecsd.cust_del_flag)::TEXT = (''::CHARACTER VARYING)::TEXT)
																		OR (
																			(ecsd.cust_del_flag IS NULL)
																			AND ('' IS NULL)
																			)
																		)
																	THEN 'O'::CHARACTER VARYING
																ELSE ecsd.cust_del_flag
																END
															)::TEXT = a.cust_del_flag
														)
													)
												AND ((a.cust_num)::TEXT = (ecsd.cust_num)::TEXT)
												)
											AND ((ecsd.dstr_chnl)::TEXT = (edc.distr_chan)::TEXT)
											)
										AND ((ecsd.sls_org)::TEXT = (esod.sls_org)::TEXT)
										)
									AND ((esod.sls_org_co_cd)::TEXT = (ecd.co_cd)::TEXT)
									)
								AND ((ecsd.sls_org)::TEXT = ('260S'::CHARACTER VARYING)::TEXT)
								)
							AND ((cddes_pck.code_type)::TEXT = ('Parent Customer Key'::CHARACTER VARYING)::TEXT)
							)
						AND ((cddes_bnrkey.code_type)::TEXT = ('Banner Key'::CHARACTER VARYING)::TEXT)
						)
					AND ((cddes_bnrfmt.code_type)::TEXT = ('Banner Format Key'::CHARACTER VARYING)::TEXT)
					)
				AND ((cddes_chnl.code_type)::TEXT = ('Channel Key'::CHARACTER VARYING)::TEXT)
				)
			AND ((cddes_gtm.code_type)::TEXT = ('Go To Model Key'::CHARACTER VARYING)::TEXT)
			)
		AND ((cddes_subchnl.code_type)::TEXT = ('Sub Channel Key'::CHARACTER VARYING)::TEXT)
		)
)
select * from transformed