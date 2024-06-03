with edw_customer_sales_dim as(
	select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }} 
),
edw_customer_base_dim as(
	select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_gch_customerhierarchy as(
	select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),
edw_code_descriptions as(
	select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
edw_sales_org_dim as(
	select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
edw_dstrbtn_chnl as(
	select * from {{ ref('aspedw_integration__edw_dstrbtn_chnl') }}
),
edw_company_dim as(
	select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_subchnl_retail_env_mapping as(
	select * from {{ source('aspedw_integration', 'edw_subchnl_retail_env_mapping') }}
),
t as(
	SELECT ltrim((cbd.cust_num)::TEXT, ((0)::CHARACTER VARYING)::TEXT) AS sap_cust_id
		,cbd.cust_nm AS sap_cust_nm
		,csd.sls_org AS sap_sls_org
		,ltrim((cd.company)::TEXT, ((0)::CHARACTER VARYING)::TEXT) AS sap_cmp_id
		,cd.ctry_key AS sap_cntry_cd
		,cd.ctry_nm AS sap_cntry_nm
		,cbd.addr AS sap_addr
		,cbd.rgn AS sap_region
		,cbd.dstrc AS sap_state_cd
		,cbd.city AS sap_city
		,cbd.pstl_cd AS sap_post_cd
		,csd.dstr_chnl AS sap_chnl_cd
		,dc.txtsh AS sap_chnl_desc
		,csd.sls_ofc AS sap_sls_office_cd
		,csd.sls_ofc_desc AS sap_sls_office_desc
		,csd.sls_grp AS sap_sls_grp_cd
		,csd.sls_grp_desc AS sap_sls_grp_desc
		,csd.crncy_key AS sap_curr_cd
		,csd.prnt_cust_key AS sap_prnt_cust_key
		,cddes_pck.code_desc AS sap_prnt_cust_desc
		,csd.chnl_key AS sap_cust_chnl_key
		,cddes_chnl.code_desc AS sap_cust_chnl_desc
		,csd.sub_chnl_key AS sap_cust_sub_chnl_key
		,cddes_subchnl.code_desc AS sap_sub_chnl_desc
		,csd.go_to_mdl_key AS sap_go_to_mdl_key
		,cddes_gtm.code_desc AS sap_go_to_mdl_desc
		,csd.bnr_key AS sap_bnr_key
		,cddes_bnrkey.code_desc AS sap_bnr_desc
		,csd.bnr_frmt_key AS sap_bnr_frmt_key
		,cddes_bnrfmt.code_desc AS sap_bnr_frmt_desc
		,subchnl_retail_env.retail_env
		,gch.gcgh_region AS gch_region
		,gch.gcgh_cluster AS gch_cluster
		,gch.gcgh_subcluster AS gch_subcluster
		,gch.gcgh_market AS gch_market
		,gch.gcch_retail_banner AS gch_retail_banner
		,row_number() OVER (
			PARTITION BY ltrim((csd.cust_num)::TEXT, ((0)::CHARACTER VARYING)::TEXT) ORDER BY CASE 
					WHEN (
							((csd.cust_del_flag)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
							OR (
								(csd.cust_del_flag IS NULL)
								AND (NULL IS NULL)
								)
							)
						THEN 'O'::CHARACTER VARYING
					WHEN (
							((csd.cust_del_flag)::TEXT = (''::CHARACTER VARYING)::TEXT)
							OR (
								(csd.cust_del_flag IS NULL)
								AND ('' IS NULL)
								)
							)
						THEN 'O'::CHARACTER VARYING
					ELSE csd.cust_del_flag
					END
				,csd.sls_org
				,csd.dstr_chnl
			) AS rnk
	FROM (
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
													edw_customer_sales_dim csd JOIN edw_customer_base_dim cbd ON ((ltrim((csd.cust_num)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ltrim((cbd.cust_num)::TEXT, ((0)::CHARACTER VARYING)::TEXT)))
													) LEFT JOIN edw_gch_customerhierarchy gch ON ((ltrim((gch.customer)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ltrim((cbd.cust_num)::TEXT, ((0)::CHARACTER VARYING)::TEXT)))
												) JOIN edw_dstrbtn_chnl dc ON ((ltrim((csd.dstr_chnl)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ltrim((dc.distr_chan)::TEXT, ((0)::CHARACTER VARYING)::TEXT)))
											) JOIN edw_sales_org_dim sod ON (
												(
													(
														(trim((csd.sls_org)::TEXT) = trim((sod.sls_org)::TEXT))
														AND ((sod.ctry_key)::TEXT = ('ID'::CHARACTER VARYING)::TEXT)
														)
													AND (
														((sod.sls_org)::TEXT = ('2000'::CHARACTER VARYING)::TEXT)
														OR ((sod.sls_org)::TEXT = ('2050'::CHARACTER VARYING)::TEXT)
														)
													)
												)
										) JOIN edw_company_dim cd ON (((sod.sls_org_co_cd)::TEXT = (cd.co_cd)::TEXT))
									) LEFT JOIN edw_code_descriptions cddes_pck ON (
										(
											(upper((cddes_pck.code_type)::TEXT) = ('PARENT CUSTOMER KEY'::CHARACTER VARYING)::TEXT)
											AND ((cddes_pck.code)::TEXT = (csd.prnt_cust_key)::TEXT)
											)
										)
								) LEFT JOIN edw_code_descriptions cddes_bnrkey ON (
									(
										(upper((cddes_bnrkey.code_type)::TEXT) = ('BANNER KEY'::CHARACTER VARYING)::TEXT)
										AND ((cddes_bnrkey.code)::TEXT = (csd.bnr_key)::TEXT)
										)
									)
							) LEFT JOIN edw_code_descriptions cddes_bnrfmt ON (
								(
									(upper((cddes_bnrfmt.code_type)::TEXT) = ('BANNER FORMAT KEY'::CHARACTER VARYING)::TEXT)
									AND ((cddes_bnrfmt.code)::TEXT = (csd.bnr_frmt_key)::TEXT)
									)
								)
						) LEFT JOIN edw_code_descriptions cddes_chnl ON (
							(
								(upper((cddes_chnl.code_type)::TEXT) = ('CHANNEL KEY'::CHARACTER VARYING)::TEXT)
								AND ((cddes_chnl.code)::TEXT = (csd.chnl_key)::TEXT)
								)
							)
					) LEFT JOIN edw_code_descriptions cddes_gtm ON (
						(
							(upper((cddes_gtm.code_type)::TEXT) = ('GO TO MODEL KEY'::CHARACTER VARYING)::TEXT)
							AND ((cddes_gtm.code)::TEXT = (csd.go_to_mdl_key)::TEXT)
							)
						)
				) LEFT JOIN edw_code_descriptions cddes_subchnl ON (
					(
						(upper((cddes_subchnl.code_type)::TEXT) = ('SUB CHANNEL KEY'::CHARACTER VARYING)::TEXT)
						AND ((cddes_subchnl.code)::TEXT = (csd.sub_chnl_key)::TEXT)
						)
					)
			) LEFT JOIN edw_subchnl_retail_env_mapping subchnl_retail_env ON ((upper((subchnl_retail_env.sub_channel)::TEXT) = upper((cddes_subchnl.code_desc)::TEXT)))
		)
	WHERE (
			((csd.sls_org)::TEXT = ('2000'::CHARACTER VARYING)::TEXT)
			OR ((csd.sls_org)::TEXT = ('2050'::CHARACTER VARYING)::TEXT)
			)
),
transformed as(
    SELECT (t.sap_cust_id)::CHARACTER VARYING AS sap_cust_id --customer_dim
        ,t.sap_cust_nm
        ,t.sap_sls_org
        ,(t.sap_cmp_id)::CHARACTER VARYING AS sap_cmp_id
        ,t.sap_cntry_cd
        ,t.sap_cntry_nm
        ,t.sap_addr
        ,t.sap_region
        ,t.sap_state_cd
        ,t.sap_city
        ,t.sap_post_cd
        ,t.sap_chnl_cd
        ,t.sap_chnl_desc
        ,t.sap_sls_office_cd
        ,t.sap_sls_office_desc
        ,t.sap_sls_grp_cd
        ,t.sap_sls_grp_desc
        ,t.sap_curr_cd
        ,t.sap_prnt_cust_key
        ,t.sap_prnt_cust_desc
        ,t.sap_cust_chnl_key
        ,t.sap_cust_chnl_desc
        ,t.sap_cust_sub_chnl_key
        ,t.sap_sub_chnl_desc
        ,t.sap_go_to_mdl_key
        ,t.sap_go_to_mdl_desc
        ,t.sap_bnr_key
        ,t.sap_bnr_desc
        ,t.sap_bnr_frmt_key
        ,t.sap_bnr_frmt_desc
        ,t.retail_env
        ,t.gch_region
        ,t.gch_cluster
        ,t.gch_subcluster
        ,t.gch_market
        ,t.gch_retail_banner
    FROM t
    WHERE (t.rnk = 1)
)
select * from transformed