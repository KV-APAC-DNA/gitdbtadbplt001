with itg_copa_trans as(
	select *,coalesce(cust_num,'') as cust_num_upd, coalesce(matl,'') as matl_upd, coalesce(dstr_chnl,'') as dstr_chnl_upd, coalesce(sls_grp,'') as SALES_GRP_upd, coalesce(cust_sls,'') as cust_sls_upd, coalesce(sls_org,'') as sls_org_upd, coalesce(plnt,'') as plnt_upd from {{ ref('aspitg_integration__itg_copa_trans') }} 
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
union1 as(
	SELECT DISTINCT 
    rtrim(A.acct_num) as ACCOUNT,
    rtrim(A.caln_day) as CALDAY,
    rtrim(A.chrt_acct) as CHRT_ACCTS,
    rtrim(A.co_cd) as COMP_CODE,
    rtrim(A.crncy_key_trans_crncy) as CURKEY_TC,
    rtrim(A.crncy_key) as CURRENCY,
    rtrim(A.cust_num_upd) as CUSTOMER,
    rtrim(A.cust_sls_upd) as CUST_SALES,
    rtrim(A.dstr_chnl_upd) as DISTR_CHAN,
    SUBSTRING(rtrim(A.caln_yr_mo), 1, 4) || 0 || SUBSTRING(rtrim(A.caln_yr_mo), 5, 6) as FISCPER,
    rtrim(A.fisc_yr_var) as FISCVARNT,
    rtrim(A.matl_upd) as MATERIAL,
    rtrim(A.obj_crncy_co_obj) as OBJ_CURR,
    '0' as RECORDTP,
    rtrim(A.SALES_GRP_upd) as SALES_GRP,
    (0 || (A.val_type_rpt)) as VTYPE,
    SUM(rtrim(A.amt_cntl_area_crncy)) as AMOCAC,
    SUM(round(rtrim(A.amt_obj_crncy))) as AMOCCC,
    SUM(ROUND(rtrim(A.grs_amt_trans_crncy))) as GROSSAMTTC,
    rtrim(D.bravo_acct_l1) as S003_0ACCOUNT,
    rtrim(D.bravo_acct_l2) as S004_0ACCOUNT,
    rtrim(D.bravo_acct_l3) as S005_0ACCOUNT,
    rtrim(D.bravo_acct_l4) as S006_0ACCOUNT,
    rtrim(D.bravo_acct_l5) as S007_0ACCOUNT,
    rtrim(A.plnt_upd) as plnt-- added this column as part of Kizuna phase 2 DCL Integration to identify plants
	FROM itg_copa_trans A,
	 edw_account_dim d 
   WHERE rtrim(co_cd) IN (
		SELECT parameter_value
		FROM itg_query_parameters
		WHERE parameter_name = 'company_code_filter_Kizuna'
		) --hard coded values paramterised
	AND A.dstr_chnl_upd  IN (select rtrim(parameter_value) from itg_query_parameters where rtrim(parameter_name)='dstr_chnl_filter_Kizuna_phase2') --hard coded values paramterised
	AND (
		rtrim(A.cust_num_upd)  = ''  
		OR rtrim(A.matl_upd) = '' 
		)
	AND rtrim(A.amt_obj_crncy) = rtrim(A.grs_amt_trans_crncy)
	AND rtrim(A.fisc_yr) IN (
		SELECT cast(right(identify_value, 4) AS INTEGER) AS fisc_yr
		FROM MT_CONSTANT
		WHERE identify_cd = 'JCP_PAN_FLG'
		)
	AND rtrim(A.acct_num) = rtrim(D.ACCT_NUM)
	AND rtrim(A.CHRT_ACCT) = rtrim(D.CHRT_ACCT) 
    GROUP BY 
    rtrim(A.acct_num),
    rtrim(A.caln_day),
    rtrim(A.chrt_acct),
    rtrim(A.co_cd),
    rtrim(A.crncy_key_trans_crncy),
    rtrim(A.crncy_key),
    rtrim(A.cust_num_upd),
    rtrim(A.cust_sls_upd),
    rtrim(A.dstr_chnl_upd),
    rtrim(A.caln_yr_mo),
    rtrim(A.fisc_yr_var),
    rtrim(A.matl_upd),
    rtrim(A.obj_crncy_co_obj),
    rtrim(RECORDTP),
    rtrim(A.SALES_GRP_upd),
    rtrim(A.sls_org_upd),
    (VTYPE),
    rtrim(D.bravo_acct_l1),
    rtrim(D.bravo_acct_l2),
    rtrim(D.bravo_acct_l3),
    rtrim(D.bravo_acct_l4),
    rtrim(D.bravo_acct_l5),
    rtrim(A.plnt_upd)
	
),
union2 as(
    select distinct
        rtrim(A.acct_num) as ACCOUNT,
            rtrim(A.caln_day) as CALDAY,
            rtrim(A.chrt_acct) as CHRT_ACCTS,
            rtrim(A.co_cd) as COMP_CODE,
            rtrim(A.crncy_key_trans_crncy) as CURKEY_TC,
            rtrim(A.crncy_key) as CURRENCY,
            rtrim(A.cust_num) as CUSTOMER,
            rtrim(A.cust_sls) as CUST_SALES,
            rtrim(A.dstr_chnl) as DISTR_CHAN,
            SUBSTRING(rtrim(A.caln_yr_mo), 1, 4) || 0 || SUBSTRING(rtrim(A.caln_yr_mo), 5, 6) as FISCPER,
            rtrim(A.fisc_yr_var) as FISCVARNT,
            rtrim(A.matl) as MATERIAL,
            rtrim(A.obj_crncy_co_obj) as OBJ_CURR,
            '0' as RECORDTP,
            rtrim(A.sls_grp) as SALES_GRP,
            (0 || (A.val_type_rpt)) as VTYPE,
            SUM(rtrim(A.amt_cntl_area_crncy)) as AMOCAC,
            SUM(round(rtrim(A.amt_obj_crncy))) as AMOCCC,
            SUM(rtrim(A.grs_amt_trans_crncy)) as GROSSAMTTC,
            rtrim(D.bravo_acct_l1) as S003_0ACCOUNT,
            rtrim(D.bravo_acct_l2) as S004_0ACCOUNT,
            rtrim(D.bravo_acct_l3) as S005_0ACCOUNT,
            rtrim(D.bravo_acct_l4) as S006_0ACCOUNT,
            rtrim(D.bravo_acct_l5) as S007_0ACCOUNT,
                rtrim(A.plnt) as plnt-- added this column as part of Kizuna phase 2 DCL Integration to identify plants
        FROM itg_copa_trans A,
        edw_account_dim d WHERE rtrim(co_cd) IN (
            SELECT parameter_value
            FROM itg_query_parameters
            WHERE parameter_name = 'company_code_filter_Kizuna'
            ) --hard coded values paramterised
        AND (A.dstr_chnl  IN (select rtrim(parameter_value) from itg_query_parameters where rtrim(parameter_name)='dstr_chnl_filter_Kizuna_phase2') or A.dstr_chnl is null)--hard coded values paramterised
        AND (
        A.cust_num = '' or A.cust_num is null
            OR A.matl = '' or A.matl is null
            )
        AND rtrim(A.amt_obj_crncy) != rtrim(A.grs_amt_trans_crncy)
        AND rtrim(A.fisc_yr) IN (
            SELECT cast(right(identify_value, 4) AS INTEGER) AS fisc_yr
            FROM MT_CONSTANT
            WHERE identify_cd = 'JCP_PAN_FLG'
            )
        AND rtrim(A.acct_num) = rtrim(D.ACCT_NUM)
        AND rtrim(A.CHRT_ACCT) = rtrim(D.CHRT_ACCT) GROUP BY 
    rtrim(A.acct_num),
    rtrim(A.caln_day),
    rtrim(A.chrt_acct),
    rtrim(A.co_cd),
    rtrim(A.crncy_key_trans_crncy),
    rtrim(A.crncy_key),
    rtrim(A.cust_num),
    rtrim(A.cust_sls),
    rtrim(A.dstr_chnl),
    rtrim(A.caln_yr_mo),
    rtrim(A.fisc_yr_var),
    rtrim(A.matl),
    rtrim(A.obj_crncy_co_obj),
    rtrim(RECORDTP),
    rtrim(A.sls_grp),
    rtrim(A.sls_org),
    (VTYPE),
    rtrim(D.bravo_acct_l1),
    rtrim(D.bravo_acct_l2),
    rtrim(D.bravo_acct_l3),
    rtrim(D.bravo_acct_l4),
    rtrim(D.bravo_acct_l5),
    rtrim(A.plnt)
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
