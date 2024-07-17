with itg_copa_trans as(
	select * from DEV_DNA_CORE.SNAPASPITG_INTEGRATION.ITG_COPA_TRANS
),
edw_account_dim as(
	select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.edw_account_dim
),
itg_query_parameters as(
	select * from DEV_DNA_CORE.SNAPASPITG_INTEGRATION.itg_query_parameters
),
MT_CONSTANT as(
	select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.MT_CONSTANT
),
union1 as(
	SELECT DISTINCT A.acct_num as ACCOUNT,
	A.caln_day as CALDAY,
	A.chrt_acct as CHRT_ACCTS,
	A.co_cd as COMP_CODE,
	A.crncy_key_trans_crncy as CURKEY_TC,
	A.crncy_key as CURRENCY,
	A.cust_num as CUSTOMER,
	A.cust_sls as CUST_SALES,
	A.dstr_chnl as DISTR_CHAN,
	SUBSTRING(A.caln_yr_mo, 1, 4) || 0 || SUBSTRING(A.caln_yr_mo, 5, 6) as FISCPER,
	A.fisc_yr_var as FISCVARNT,
	A.matl as MATERIAL,
	A.obj_crncy_co_obj as OBJ_CURR,
	'0' as RECORDTP,
	A.sls_grp as SALES_GRP,
	(0 || A.val_type_rpt) as VTYPE,
	SUM(A.amt_cntl_area_crncy) as AMOCAC,
	SUM(round(A.amt_obj_crncy)) as AMOCCC,
	SUM(ROUND(A.grs_amt_trans_crncy)) as GROSSAMTTC,
	D.bravo_acct_l1 as S003_0ACCOUNT,
	D.bravo_acct_l2 as S004_0ACCOUNT,
	D.bravo_acct_l3 as S005_0ACCOUNT,
	D.bravo_acct_l4 as S006_0ACCOUNT,
	D.bravo_acct_l5 as S007_0ACCOUNT,
	A.plnt as plnt-- added this column as part of Kizuna phase 2 DCL Integration to identify plants
	FROM itg_copa_trans A,
	 edw_account_dim d 
   WHERE co_cd IN (
		SELECT parameter_value
		FROM itg_query_parameters
		WHERE parameter_name = 'company_code_filter_Kizuna'
		) --hard coded values paramterised
	AND A.dstr_chnl IN (
		SELECT parameter_value
		FROM itg_query_parameters
		WHERE parameter_name = 'dstr_chnl_filter_Kizuna_phase2'
		) --hard coded values paramterised
	AND (
		A.cust_num = ''
		OR A.matl = ''
		)
	AND trim(A.amt_obj_crncy) = trim(A.grs_amt_trans_crncy)
	AND A.fisc_yr IN (
		SELECT cast(right(identify_value, 4) AS INTEGER) AS fisc_yr
		FROM MT_CONSTANT
		WHERE identify_cd = 'JCP_PAN_FLG'
		)
	AND A.acct_num = D.ACCT_NUM
	AND A.CHRT_ACCT = D.CHRT_ACCT 
    GROUP BY A.acct_num,
	A.caln_day,
	A.chrt_acct,
	A.co_cd,
	A.crncy_key_trans_crncy,
	A.crncy_key,
	A.cust_num,
	A.cust_sls,
	A.dstr_chnl,
	A.caln_yr_mo,
	A.fisc_yr_var,
	A.matl,
	A.obj_crncy_co_obj,
	RECORDTP,
	A.sls_grp,
	A.sls_org,
	VTYPE,
	D.bravo_acct_l1,
	D.bravo_acct_l2,
	D.bravo_acct_l3,
	D.bravo_acct_l4,
	D.bravo_acct_l5,
	A.plnt
	
),
union2 as(
	SELECT DISTINCT A.acct_num as ACCOUNT,
	A.caln_day as CALDAY,
	A.chrt_acct as CHRT_ACCTS,
	A.co_cd as COMP_CODE,
	A.crncy_key_trans_crncy as CURKEY_TC,
	A.crncy_key as CURRENCY,
	A.cust_num as CUSTOMER,
	A.cust_sls as CUST_SALES,
	A.dstr_chnl as DISTR_CHAN,
	SUBSTRING(A.caln_yr_mo, 1, 4) || 0 || SUBSTRING(A.caln_yr_mo, 5, 6) as FISCPER,
	A.fisc_yr_var as FISCVARNT,
	A.matl as MATERIAL,
	A.obj_crncy_co_obj as OBJ_CURR,
	'0' as RECORDTP,
	A.sls_grp as SALES_GRP,
	(0 || A.val_type_rpt) as VTYPE,
	SUM(A.amt_cntl_area_crncy) as AMOCAC,
	SUM(round(A.amt_obj_crncy)) as AMOCCC,
	SUM(A.grs_amt_trans_crncy) as GROSSAMTTC,
	D.bravo_acct_l1 as S003_0ACCOUNT,
	D.bravo_acct_l2 as S004_0ACCOUNT,
	D.bravo_acct_l3 as S005_0ACCOUNT,
	D.bravo_acct_l4 as S006_0ACCOUNT,
	D.bravo_acct_l5 as S007_0ACCOUNT,
	A.plnt as plnt-- added this column as part of Kizuna phase 2 DCL Integration to identify plants
	FROM itg_copa_trans A,
	edw_account_dim d WHERE co_cd IN (
		SELECT parameter_value
		FROM itg_query_parameters
		WHERE parameter_name = 'company_code_filter_Kizuna'
		) --hard coded values paramterised
	AND A.dstr_chnl IN (
		SELECT parameter_value
		FROM itg_query_parameters
		WHERE parameter_name = 'dstr_chnl_filter_Kizuna_phase2'
		) --hard coded values paramterised
	AND (
		A.cust_num = ''
		OR A.matl = ''
		)
	AND trim(A.amt_obj_crncy) != trim(A.grs_amt_trans_crncy)
	AND A.fisc_yr IN (
		SELECT cast(right(identify_value, 4) AS INTEGER) AS fisc_yr
		FROM MT_CONSTANT
		WHERE identify_cd = 'JCP_PAN_FLG'
		)
	AND A.acct_num = D.ACCT_NUM
	AND A.CHRT_ACCT = D.CHRT_ACCT GROUP BY A.acct_num,
	A.caln_day,
	A.chrt_acct,
	A.co_cd,
	A.crncy_key_trans_crncy,
	A.crncy_key,
	A.cust_num,
	A.cust_sls,
	A.dstr_chnl,
	A.caln_yr_mo,
	A.fisc_yr_var,
	A.matl,
	A.obj_crncy_co_obj,
	RECORDTP,
	A.sls_grp,
	A.sls_org,
	VTYPE,
	D.bravo_acct_l1,
	D.bravo_acct_l2,
	D.bravo_acct_l3,
	D.bravo_acct_l4,
	D.bravo_acct_l5,
	A.plnt
	
),
transformed as(
	select * from union1
	union all
	select * from union2
),
final as(
	select 
		account::varchar(30) as account,
		calday::varchar(24) as calday,
		chrt_accts::varchar(12) as chrt_accts,
		comp_code::varchar(12) as comp_code,
		curkey_tc::varchar(15) as curkey_tc,
		currency::varchar(15) as currency,
		customer::varchar(30) as customer,
		cust_sales::varchar(30) as cust_sales,
		distr_chan::varchar(6) as distr_chan,
		fiscper::varchar(21) as fiscper,
		fiscvarnt::varchar(6) as fiscvarnt,
		material::varchar(54) as material,
		obj_curr::varchar(15) as obj_curr,
		recordtp::varchar(3) as recordtp,
		sales_grp::varchar(9) as sales_grp,
		vtype::varchar(9) as vtype,
		amocac::number(17,2) as amocac,
		amoccc::number(17,2) as amoccc,
		NULL::varchar(9) as s016_0cust_sales,
		NULL::varchar(12) as s017_0cust_sales,
		NULL::varchar(24) as s001_0cust_sales,
		grossamttc::number(17,2) as grossamttc,
		NULL::varchar(54) as s023_0material,
		NULL::varchar(54) as s024_0material,
		NULL::varchar(54) as s025_0material,
		NULL::varchar(54) as s026_0material,
		s003_0account::varchar(54) as s003_0account,
		s004_0account::varchar(54) as s004_0account,
		s005_0account::varchar(54) as s005_0account,
		s006_0account::varchar(54) as s006_0account,
		s007_0account::varchar(54) as s007_0account,
		plnt::varchar(10) as plnt
	from transformed
)
select * from final
