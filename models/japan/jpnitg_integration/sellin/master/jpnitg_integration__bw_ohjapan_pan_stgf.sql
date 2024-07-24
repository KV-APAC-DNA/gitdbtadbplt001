with edw_material_sales_dim as(
select * from {{ ref('aspedw_integration__edw_material_sales_dim') }} 
),
BW_OHJAPAN_PAN_STG as(
select * from {{ ref('jpnitg_integration__bw_ohjapan_pan_stg') }}
),

a as(
	SELECT DISTINCT A.ACCOUNT,
		A.CALDAY,
		A.CHRT_ACCTS,
		A.COMP_CODE,
		A.CURKEY_TC,
		A.CURRENCY,
		A.CUSTOMER,
		A.CUST_SALES,
		A.DISTR_CHAN,
		A.FISCPER,
		A.FISCVARNT,
		A.MATERIAL,
		A.OBJ_CURR,
		A.RECORDTP,
		A.SALES_GRP,
		A.VTYPE,
		A.AMOCAC,
		A.AMOCCC,
		' ' S016_0CUST_SALES,
		' ' S017_0CUST_SALES,
		' ' s001_0cust_sales,
		A.GROSSAMTTC,
		A.S003_0ACCOUNT,
		A.S004_0ACCOUNT,
		A.S005_0ACCOUNT,
		A.S006_0ACCOUNT,
		A.S007_0ACCOUNT,
		A.plnt -- added this column as part of Kizuna phase 2 DCL Integration to identify plants
	FROM BW_OHJAPAN_PAN_STG A
),
transformed as(
	SELECT DISTINCT 
	A.ACCOUNT::varchar(30) as ACCOUNT,
	A.CALDAY::varchar(24) as CALDAY,
	A.CHRT_ACCTS::varchar(12) as CHRT_ACCTS,
	A.COMP_CODE::varchar(12) as COMP_CODE,
	A.CURKEY_TC::varchar(15) as CURKEY_TC,
	A.CURRENCY::varchar(15) as CURRENCY,
	A.CUSTOMER::varchar(30) as CUSTOMER,
	A.CUST_SALES::varchar(30) as CUST_SALES,
	A.DISTR_CHAN::varchar(6) as DISTR_CHAN,
	A.FISCPER::varchar(21) as FISCPER,
	A.FISCVARNT::varchar(6) as FISCVARNT,
	A.MATERIAL::varchar(54) as MATERIAL,
	A.OBJ_CURR::varchar(15) as OBJ_CURR,
	A.RECORDTP::varchar(3) as RECORDTP,
	A.SALES_GRP::varchar(9) as SALES_GRP,
	A.VTYPE::varchar(9) as VTYPE,
	A.AMOCAC::number(17,2) as AMOCAC,
	A.AMOCCC::number(17,2) as AMOCCC,
	A.S016_0CUST_SALES::varchar(9) as S016_0CUST_SALES,
	A.S017_0CUST_SALES::varchar(12) as S017_0CUST_SALES,
	A.s001_0cust_sales::varchar(24) as S001_0CUST_SALES,
	A.GROSSAMTTC::number(17,2) as GROSSAMTTC,
	CASE 
		WHEN C.MATL_NUM = ' '
			AND c.delv_plnt != ' '
			THEN SUBSTRING(C.PROD_HIERARCHY, 1, 6)
		ELSE ' '
		END::varchar(54) as S023_0MATERIAL,
	CASE 
		WHEN C.MATL_NUM = ' '
			AND c.delv_plnt != ' '
			THEN SUBSTRING(C.PROD_HIERARCHY, 1, 10)
		ELSE ' '
		END::varchar(54) as S024_0MATERIAL,
	CASE 
		WHEN C.MATL_NUM = ' '
			AND c.delv_plnt != ' '
			THEN SUBSTRING(C.PROD_HIERARCHY, 1, 14)
		ELSE ' '
		END::varchar(54) as S025_0MATERIAL,
	CASE 
		WHEN C.MATL_NUM = ' '
			AND c.delv_plnt != ' '
			THEN C.PROD_HIERARCHY
		ELSE ' '
		END::varchar(54) as S026_0MATERIAL,
	A.S003_0ACCOUNT::varchar(54) as S003_0ACCOUNT,
	A.S004_0ACCOUNT::varchar(54) as S004_0ACCOUNT,
	A.S005_0ACCOUNT::varchar(54) as S005_0ACCOUNT,
	A.S006_0ACCOUNT::varchar(54) as S006_0ACCOUNT,
	A.S007_0ACCOUNT::varchar(54) as S007_0ACCOUNT,
	A.plnt::varchar(10) as plnt-- added this column as part of Kizuna phase 2 DCL Integration to identify plants
	FROM A,
	edw_material_sales_dim C WHERE A.MATERIAL = C.MATL_NUM(+)
)
select * from transformed   