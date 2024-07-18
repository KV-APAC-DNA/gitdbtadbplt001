with customer_control_tp_accrual_reversal_ac as(
    select * from {{ source('pcfedw_integration', 'customer_control_tp_accrual_reversal_ac') }}
),
edw_customer_base_dim as(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_customer_sales_dim as(
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
dly_sls_cust_attrb_lkp as(
    select * from {{ ref('pcfedw_integration__dly_sls_cust_attrb_lkp') }}
),
customer as(
	SELECT DISTINCT cust.cust_num AS cust_no
				,pac_lkup.cmp_id_code AS cmp_id
				,pac_lkup.dstr_chnl_Code AS channel_cd
				,pac_lkup.dstr_chnl_name AS channel_desc
				,cust.ctry_key
				,pac_lkup.cmp_id_name AS country
				,cust.rgn AS state_cd
				,cust.pstl_cd AS post_cd
				,cust.city AS cust_suburb
				,cust.cust_nm
				,pac_lkup.sls_org_code AS sls_org
				,cust_sales.cust_del_flag
				,pac_lkup.sls_ofc_code AS sales_office_cd
				,pac_lkup.sls_ofc_name AS sales_office_desc
				,CASE 
					WHEN (ltrim((cust.cust_num)::TEXT, '0'::TEXT) = ltrim((CONTROL.cust_no)::TEXT, '0'::TEXT))
						THEN CONTROL.sls_grp
					ELSE pac_lkup.sls_grp_code
					END AS sales_grp_cd
				,pac_lkup.sls_grp_name AS sales_grp_desc
				,cust.fcst_chnl AS mercia_ref
				,cust_sales.crncy_key AS curr_cd
			FROM customer_control_tp_accrual_reversal_ac CONTROL
				,edw_customer_base_dim cust
				,(
					SELECT a.cust_num
						,min((
								CASE 
									WHEN (
											((a.cust_del_flag)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
											OR (
												(a.cust_del_flag IS NULL)
												AND (NULL IS NULL)
												)
											)
										THEN 'O'::CHARACTER VARYING
									WHEN (
											((a.cust_del_flag)::TEXT = (''::CHARACTER VARYING)::TEXT)
											OR (
												(a.cust_del_flag IS NULL)
												AND ('' IS NULL)
												)
											)
										THEN 'O'::CHARACTER VARYING
									ELSE a.cust_del_flag
									END
								)::TEXT) AS cust_del_flag
					FROM edw_customer_sales_dim a
						,(
							SELECT DISTINCT dly_sls_cust_attrb_lkp.sls_org_code
							FROM dly_sls_cust_attrb_lkp
							) b
					WHERE ((a.sls_org)::TEXT = (b.sls_org_code)::TEXT)
					GROUP BY a.cust_num
					) req_cust_rec
				,(
					edw_customer_sales_dim cust_sales LEFT JOIN dly_sls_cust_attrb_lkp pac_lkup ON (((cust_sales.sls_grp)::TEXT = (pac_lkup.sls_grp_code)::TEXT))
					)
			WHERE (
					(
						((cust_sales.cust_num)::TEXT = (cust.cust_num)::TEXT)
						AND ((cust_sales.cust_num)::TEXT = (req_cust_rec.cust_num)::TEXT)
						)
					AND (
						(
							CASE 
								WHEN (
										((cust_sales.cust_del_flag)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
										OR (
											(cust_sales.cust_del_flag IS NULL)
											AND (NULL IS NULL)
											)
										)
									THEN 'O'::CHARACTER VARYING
								WHEN (
										((cust_sales.cust_del_flag)::TEXT = (''::CHARACTER VARYING)::TEXT)
										OR (
											(cust_sales.cust_del_flag IS NULL)
											AND ('' IS NULL)
											)
										)
									THEN 'O'::CHARACTER VARYING
								ELSE cust_sales.cust_del_flag
								END
							)::TEXT = req_cust_rec.cust_del_flag
						)
					)
),
transformed as(
SELECT cust.cust_no
	,pac_lkup.cmp_id_code AS cmp_id
	,pac_lkup.dstr_chnl_code AS channel_cd
	,pac_lkup.dstr_chnl_name AS channel_desc
	,cust.ctry_key
	,pac_lkup.dstr_chnl_name AS country
	,cust.state_cd
	,cust.post_cd
	,cust.cust_suburb
	,cust.cust_nm
	,pac_lkup.sls_org_code AS sls_org
	,cust.cust_del_flag
	,pac_lkup.sls_ofc_code AS sales_office_cd
	,pac_lkup.sls_ofc_name AS sales_office_desc
	,pac_lkup.sls_grp_code AS sales_grp_cd
	,pac_lkup.sls_grp_name AS sales_grp_desc
	,cust.mercia_ref
	,cust.curr_cd
FROM (
	(
		SELECT customer.cust_no
			,min((customer.cmp_id)::TEXT) AS cmp_id
			,min((customer.channel_cd)::TEXT) AS channel_cd
			,min((customer.channel_desc)::TEXT) AS channel_desc
			,min((customer.ctry_key)::TEXT) AS ctry_key
			,min((customer.country)::TEXT) AS country
			,min((customer.state_cd)::TEXT) AS state_cd
			,min((customer.post_cd)::TEXT) AS post_cd
			,min((customer.cust_suburb)::TEXT) AS cust_suburb
			,min((customer.cust_nm)::TEXT) AS cust_nm
			,min((customer.sls_org)::TEXT) AS sls_org
			,min((customer.cust_del_flag)::TEXT) AS cust_del_flag
			,min((customer.sales_office_cd)::TEXT) AS sales_office_cd
			,min((customer.sales_office_desc)::TEXT) AS sales_office_desc
			,min((customer.sales_grp_cd)::TEXT) AS sales_grp_cd
			,min((customer.sales_grp_desc)::TEXT) AS sales_grp_desc
			,min((customer.mercia_ref)::TEXT) AS mercia_ref
			,min((customer.curr_cd)::TEXT) AS curr_cd
		FROM  customer
		GROUP BY customer.cust_no
		) cust LEFT JOIN dly_sls_cust_attrb_lkp pac_lkup ON ((((cust.sales_grp_cd)::CHARACTER VARYING)::TEXT = (pac_lkup.sls_grp_code)::TEXT))
	)
WHERE (
		(ltrim((cust.cust_no)::TEXT, ('0'::CHARACTER VARYING)::TEXT) not like ('7%'::CHARACTER VARYING)::TEXT)
		OR (
			cust.cust_no IN (
				SELECT DISTINCT customer_control_tp_accrual_reversal_ac.cust_no
				FROM customer_control_tp_accrual_reversal_ac
				)
			)
		)
)
select * from transformed