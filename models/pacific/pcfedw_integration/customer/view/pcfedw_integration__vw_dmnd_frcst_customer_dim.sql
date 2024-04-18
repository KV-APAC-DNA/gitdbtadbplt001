with edw_customer_base_dim as(
    select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.edw_customer_base_dim
),
edw_customer_sales_dim as(
    select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_CUSTOMER_SALES_DIM
),
dmnd_frcst_cust_attrb_lkp as(
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.dmnd_frcst_cust_attrb_lkp
),
customer as(
SELECT DISTINCT cust.cust_num AS cust_no
			,pac_lkup.sls_org
			,pac_lkup.cmp_id
			,cust.ctry_key
			,pac_lkup.country
			,cust.rgn AS state_cd
			,cust.pstl_cd AS post_cd
			,cust.city AS cust_suburb
			,cust.cust_nm
			,cust_sales.cust_del_flag
			,pac_lkup.fcst_chnl
			,pac_lkup.fcst_chnl_desc
			,pac_lkup.sls_office
			,pac_lkup.sls_office_desc
			,pac_lkup.sls_grp AS sales_grp_cd
			,pac_lkup.sls_grp_desc AS sales_grp_desc
			,cust_sales.crncy_key AS curr_cd
		FROM edw_customer_base_dim cust
			,edw_customer_sales_dim cust_sales
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
						SELECT DISTINCT dmnd_frcst_cust_attrb_lkp.sls_org
						FROM dmnd_frcst_cust_attrb_lkp
						) b
				WHERE (trim((a.sls_org)::TEXT) = trim(((b.sls_org)::CHARACTER VARYING)::TEXT))
				GROUP BY a.cust_num
				) req_cust_rec
			,dmnd_frcst_cust_attrb_lkp pac_lkup
		WHERE (
				(
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
				AND ((cust_sales.sls_grp)::TEXT = (pac_lkup.sls_grp)::TEXT)
				)
),

cust as(
SELECT customer.cust_no
		,min((customer.cmp_id)::TEXT) AS cmp_id
		,min((customer.ctry_key)::TEXT) AS ctry_key
		,min((customer.country)::TEXT) AS country
		,min((customer.state_cd)::TEXT) AS state_cd
		,min((customer.post_cd)::TEXT) AS post_cd
		,min((customer.cust_suburb)::TEXT) AS cust_suburb
		,min((customer.cust_nm)::TEXT) AS cust_nm
		,min(customer.sls_org) AS sls_org
		,min((customer.cust_del_flag)::TEXT) AS cust_del_flag
		,min((customer.sales_grp_cd)::TEXT) AS sales_grp_cd
		,min((customer.sales_grp_desc)::TEXT) AS sales_grp_desc
		,min((customer.fcst_chnl)::TEXT) AS fcst_chnl
		,min((customer.fcst_chnl_desc)::TEXT) AS fcst_chnl_desc
		,min((customer.curr_cd)::TEXT) AS curr_cd
	FROM customer
	GROUP BY customer.cust_no
),
transformed as(
SELECT cust.cust_no
	,pac_lkup.sls_org
	,pac_lkup.cmp_id
	,cust.ctry_key
	,pac_lkup.country
	,cust.state_cd
	,cust.post_cd
	,cust.cust_suburb
	,cust.cust_nm
	,cust.cust_del_flag
	,pac_lkup.fcst_chnl
	,pac_lkup.fcst_chnl_desc
	,pac_lkup.sls_office AS sales_office_cd
	,pac_lkup.sls_office_desc AS sales_office_desc
	,pac_lkup.sls_grp AS sales_grp_cd
	,pac_lkup.sls_grp_desc AS sales_grp_desc
	,cust.curr_cd
FROM cust,dmnd_frcst_cust_attrb_lkp pac_lkup
WHERE (
		(ltrim((cust.cust_no)::TEXT, ('0'::CHARACTER VARYING)::TEXT) not like ('7%'::CHARACTER VARYING)::TEXT)
		AND (
			CASE 
				WHEN (
						((cust.cust_no)::TEXT = ('0000758531'::CHARACTER VARYING)::TEXT)
						OR (
							(cust.cust_no IS NULL)
							AND ('0000758531' IS NULL)
							)
						)
					THEN ('AUA'::CHARACTER VARYING)::TEXT
				WHEN (
						((cust.cust_no)::TEXT = ('0000758532'::CHARACTER VARYING)::TEXT)
						OR (
							(cust.cust_no IS NULL)
							AND ('0000758532' IS NULL)
							)
						)
					THEN ('AUB'::CHARACTER VARYING)::TEXT
				WHEN (
						((cust.cust_no)::TEXT = ('0000758636'::CHARACTER VARYING)::TEXT)
						OR (
							(cust.cust_no IS NULL)
							AND ('0000758636' IS NULL)
							)
						)
					THEN ('NZA'::CHARACTER VARYING)::TEXT
				ELSE cust.sales_grp_cd
				END = (pac_lkup.sls_grp)::TEXT
			)
		)
)
select * from transformed