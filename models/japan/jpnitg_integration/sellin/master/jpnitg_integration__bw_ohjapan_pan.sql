{{
    config(
        pre_hook= "UPDATE {{ ref('jpnedw_integration__mt_constant_seq') }}
                SET MAX_VALUE=(SELECT MAX(JCP_REC_SEQ) FROM {{ ref('jpnedw_integration__dw_so_sell_out_dly') }});"
    )
}}

with itg_copa_trans as(
	select * from {{ ref('aspitg_integration__itg_copa_trans') }} 
),
edw_customer_sales_dim as(
	select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }} 
),
edw_material_sales_dim as(
	select * from {{ ref('aspedw_integration__edw_material_sales_dim') }} 
),
edw_account_dim as(
	select * from {{ ref('aspedw_integration__edw_account_dim') }} 
),
itg_query_parameters as(
	select * from {{ source('aspitg_integration', 'itg_query_parameters') }}
),
mt_constant as(
	select * from {{ source('jpnedw_integration', 'mt_constant') }}
),
bw_ohjapan_pan_stgf as (
    select * from {{ ref('jpnitg_integration__bw_ohjapan_pan_stgf') }} 
),
union1 as(
	SELECT A.acct_num as ACCOUNT,
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
	B.SLS_GRP as S016_0CUST_SALES,
	B.SLS_OFC as S017_0CUST_SALES,
	LPAD(B.persnl_num, 8, 0) as S001_0CUST_SALES,
	SUM(ROUND(A.grs_amt_trans_crncy)) as GROSSAMTTC,
	SUBSTRING(C.PROD_HIERARCHY, 1, 6) as S023_0MATERIAL,
	SUBSTRING(C.PROD_HIERARCHY, 1, 10) as S024_0MATERIAL,
	SUBSTRING(C.PROD_HIERARCHY, 1, 14) as S025_0MATERIAL,
	C.PROD_HIERARCHY as S026_0MATERIAL,
	D.bravo_acct_l1 as S003_0ACCOUNT,
	D.bravo_acct_l2 as S004_0ACCOUNT,
	D.bravo_acct_l3 as S005_0ACCOUNT,
	D.bravo_acct_l4 as S006_0ACCOUNT,
	D.bravo_acct_l5 as S007_0ACCOUNT,
	A.plnt as plnt-- added this column as part of Kizuna phase 2 DCL Integration to identify plants
	FROM 
	itg_copa_trans A,
	edw_customer_sales_dim B,
	edw_material_sales_dim c,
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
	AND A.amt_obj_crncy = A.grs_amt_trans_crncy
	AND A.cust_num = B.CUST_NUM
	AND A.dstr_chnl = B.DSTR_CHNL
	AND A.dstr_chnl = C.DSTR_CHNL
	AND A.SLS_ORG = B.SLS_ORG
	AND A.MATL = C.MATL_NUM
	AND B.SLS_ORG = C.SLS_ORG
	AND A.acct_num = D.ACCT_NUM
	AND A.CHRT_ACCT = D.CHRT_ACCT
	AND A.fisc_yr IN (
		SELECT cast(right(identify_value, 4) AS INTEGER) AS fisc_yr
		FROM MT_CONSTANT
		WHERE identify_cd = 'JCP_PAN_FLG'
		) GROUP BY A.acct_num,
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
	A.val_type_rpt,
	B.SLS_GRP,
	B.SLS_OFC,
	LPAD(B.persnl_num, 8, 0),
	SUBSTRING(C.PROD_HIERARCHY, 1, 6),
	SUBSTRING(C.PROD_HIERARCHY, 1, 10),
	SUBSTRING(C.PROD_HIERARCHY, 1, 14),
	C.PROD_HIERARCHY,
	D.bravo_acct_l1,
	D.bravo_acct_l2,
	D.bravo_acct_l3,
	D.bravo_acct_l4,
	D.bravo_acct_l5,
	A.plnt
),
union2 as(
	SELECT A.acct_num as ACCOUNT,
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
	B.SLS_GRP as S016_0CUST_SALES,
	B.SLS_OFC as S017_0CUST_SALES,
	LPAD(B.persnl_num, 8, 0) as S001_0CUST_SALES,
	SUM(A.grs_amt_trans_crncy) as GROSSAMTTC,
	SUBSTRING(C.PROD_HIERARCHY, 1, 6) as S023_0MATERIAL,
	SUBSTRING(C.PROD_HIERARCHY, 1, 10) as S024_0MATERIAL,
	SUBSTRING(C.PROD_HIERARCHY, 1, 14) as S025_0MATERIAL,
	C.PROD_HIERARCHY as S026_0MATERIAL,
	D.bravo_acct_l1 as S003_0ACCOUNT,
	D.bravo_acct_l2 as S004_0ACCOUNT,
	D.bravo_acct_l3 as S005_0ACCOUNT,
	D.bravo_acct_l4 as S006_0ACCOUNT,
	D.bravo_acct_l5 as S007_0ACCOUNT,
	A.plnt as plnt -- added this column as part of Kizuna phase 2 DCL Integration to identify plants
	FROM itg_copa_trans A,
	edw_customer_sales_dim B,
	edw_material_sales_dim c,
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
	AND A.SLS_ORG = B.SLS_ORG
	AND A.amt_obj_crncy != A.grs_amt_trans_crncy
	AND A.cust_num = B.CUST_NUM
	AND A.dstr_chnl = B.DSTR_CHNL
	AND A.dstr_chnl = C.DSTR_CHNL
	AND A.MATL = C.MATL_NUM
	AND B.SLS_ORG = C.SLS_ORG
	AND A.acct_num = D.ACCT_NUM
	AND A.CHRT_ACCT = D.CHRT_ACCT
	AND A.fisc_yr IN (
		SELECT cast(right(identify_value, 4) AS INTEGER) AS fisc_yr
		FROM MT_CONSTANT
		WHERE identify_cd = 'JCP_PAN_FLG'
		) GROUP BY A.acct_num,
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
	A.val_type_rpt,
	B.SLS_GRP,
	B.SLS_OFC,
	LPAD(B.persnl_num, 8, 0),
	SUBSTRING(C.PROD_HIERARCHY, 1, 6),
	SUBSTRING(C.PROD_HIERARCHY, 1, 10),
	SUBSTRING(C.PROD_HIERARCHY, 1, 14),
	C.PROD_HIERARCHY,
	D.bravo_acct_l1,
	D.bravo_acct_l2,
	D.bravo_acct_l3,
	D.bravo_acct_l4,
	D.bravo_acct_l5,
	A.plnt
),
union3 as(
    select distinct 
        account,
        calday,
        chrt_accts,
        comp_code,
        curkey_tc,
        currency,
        customer,
        cust_sales,
        distr_chan,
        fiscper,
        fiscvarnt,
        material,
        obj_curr,
        recordtp,
        sales_grp,
        vtype,
        amocac,
        amoccc,
        s016_0cust_sales,
        s017_0cust_sales,
        s001_0cust_sales,
        grossamttc,
        s023_0material,
        s024_0material,
        s025_0material,
        s026_0material,
        s003_0account,
        s004_0account,
        s005_0account,
        s006_0account,
        s007_0account,
        plnt
from  bw_ohjapan_pan_STGF 
),
transformed as(
	select * from union1
	union all
	select * from union2
    union all
    select * from union3
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
		s016_0cust_sales::varchar(9) as s016_0cust_sales,
		s017_0cust_sales::varchar(12) as s017_0cust_sales,
		s001_0cust_sales::varchar(24) as s001_0cust_sales,
		grossamttc::number(17,2) as grossamttc,
		s023_0material::varchar(54) as s023_0material,
		s024_0material::varchar(54) as s024_0material,
		s025_0material::varchar(54) as s025_0material,
		s026_0material::varchar(54) as s026_0material,
		s003_0account::varchar(54) as s003_0account,
		s004_0account::varchar(54) as s004_0account,
		s005_0account::varchar(54) as s005_0account,
		s006_0account::varchar(54) as s006_0account,
		s007_0account::varchar(54) as s007_0account,
		plnt::varchar(10) as plnt
	from transformed
)
select * from final
