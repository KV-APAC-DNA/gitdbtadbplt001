with WKS_RPT_SSS_SCORECARD_BASE as(
    select * from {{ ref('indwks_integration__wks_rpt_sss_scorecard_base') }}
),
WKS_SSS_REBASE_CALCULATION as(
    select * from {{ ref('indwks_integration__wks_sss_rebase_calculation') }}
),
WKS_STORES_WITH_MSL_IN_YEAR_QTR as(
    select * from {{ ref('indwks_integration__wks_stores_with_msl_in_year_qtr') }}
),
union1 as(
    SELECT RPT.TABLE_RN,
	RPT.COUNTRY,
	RPT.REGION,
	RPT.ZONE,
	RPT.TERRITORY,
	RPT.CHANNEL,
	RPT.RETAIL_ENVIRONMENT,
	RPT.SALESMAN_NAME,
	RPT.SALESMAN_CODE,
	RPT.DISTRIBUTOR_CODE,
	RPT.DISTRIBUTOR_NAME,
	RPT.STORE_CODE,
	RPT.STORE_NAME,
	RPT.PROGRAM_TYPE,
	RPT.FRANCHISE,
	RPT.KPI,
	RPT.QUARTER,
	RPT.YEAR,
    null as SOURCE_ACTUAL_VALUE,
    null as SOURCE_TARGET_VALUE,
	RPT.ACTUAL_VALUE,
	RPT.TARGET_VALUE,
	RPT.COMPLIANCE,
	RPT.WEIGHT,
	RPT.KPI_SCORE,
	RPT.KPI_ACHIEVEMENT,
	RPT.MAX_POTENTIAL_BRAND_SCORE,
	RPT.ACHIEVEMENT_NR,
	RPT.PROD_HIER_L1,
	RPT.PROD_HIER_L2,
	RPT.PROD_HIER_L3,
	RPT.PROD_HIER_L4,
	RPT.PROD_HIER_L5,
	RPT.PROD_HIER_L6,
	RPT.PROD_HIER_L7,
	RPT.PROD_HIER_L8,
	RPT.PROD_HIER_L9,
	RPT.ACTUAL_VALUE_MSL_L9,
	RPT.TARGET_VALUE_MSL_L9,
	RPT.RTRUNIQUECODE,
	RPT.PROMOTOR_STORE,
	current_timestamp()::timestamp_ntz(9) AS CRT_DTT,
	current_timestamp()::timestamp_ntz(9) AS UPDT_DTTM,
	CALC.REBASE_SCORE
FROM WKS_RPT_SSS_SCORECARD_BASE RPT,
	WKS_SSS_REBASE_CALCULATION CALC
WHERE RPT.TABLE_RN = CALC.TABLE_RN
	AND RPT.STORE_CODE = CALC.STORE_CODE
	AND RPT.STORE_NAME = CALC.STORE_NAME
	AND RPT.PROGRAM_TYPE = CALC.PROGRAM_TYPE
	AND NVL(RPT.FRANCHISE, 'BRAND NOT APPLICABLE') = NVL(CALC.FRANCHISE, 'BRAND NOT APPLICABLE')
	AND NVL(RPT.PROD_HIER_L4, 'No PROD_HIER_L4') = NVL(CALC.PROD_HIER_L4, 'No PROD_HIER_L4')
	AND RPT.KPI = CALC.KPI
	AND RPT.QUARTER = CALC.QUARTER
	AND RPT.YEAR = CALC.YEAR
	AND RPT.KPI_SCORE = CALC.KPI_SCORE
	AND RPT.KPI <> 'SALES VALUE'
),
union2 as(
    SELECT SSS.TABLE_RN,
	SSS.COUNTRY,
	SSS.REGION,
	SSS.ZONE,
	SSS.TERRITORY,
	SSS.CHANNEL,
	SSS.RETAIL_ENVIRONMENT,
	SSS.SALESMAN_NAME,
	SSS.SALESMAN_CODE,
	SSS.DISTRIBUTOR_CODE,
	SSS.DISTRIBUTOR_NAME,
	SSS.STORE_CODE,
	SSS.STORE_NAME,
	SSS.PROGRAM_TYPE,
	SSS.FRANCHISE,
	SSS.KPI,
	SSS.QUARTER,
	SSS.YEAR,
    null as SOURCE_ACTUAL_VALUE,
    null as SOURCE_TARGET_VALUE,
	SSS.ACTUAL_VALUE,
	SSS.TARGET_VALUE,
	SSS.COMPLIANCE,
	SSS.WEIGHT,
	SSS.KPI_SCORE,
	SSS.KPI_ACHIEVEMENT,
	SSS.MAX_POTENTIAL_BRAND_SCORE,
	SSS.ACHIEVEMENT_NR,
	SSS.PROD_HIER_L1,
	SSS.PROD_HIER_L2,
	SSS.PROD_HIER_L3,
	SSS.PROD_HIER_L4,
	SSS.PROD_HIER_L5,
	SSS.PROD_HIER_L6,
	SSS.PROD_HIER_L7,
	SSS.PROD_HIER_L8,
	SSS.PROD_HIER_L9,
	SSS.ACTUAL_VALUE_MSL_L9,
	SSS.TARGET_VALUE_MSL_L9,
	SSS.RTRUNIQUECODE,
	SSS.PROMOTOR_STORE,
	current_timestamp()::timestamp_ntz(9) AS CRT_DTT,
	current_timestamp()::timestamp_ntz(9) AS UPDT_DTTM,
	0 AS REBASE_SCORE
FROM WKS_RPT_SSS_SCORECARD_BASE SSS,
	WKS_STORES_WITH_MSL_IN_YEAR_QTR STO
WHERE SSS.KPI = 'SALES VALUE'
	AND SSS.STORE_CODE = STO.STORE_CODE
	AND SSS.YEAR = STO.YEAR
	AND SSS.QUARTER = STO.QUARTER
),
transformed as(
    select * from union1
    union all 
    select * from union2
),
final as(
    select
        table_rn::number(38,0) as table_rn,
        country::varchar(5) as country,
        region::varchar(75) as region,
        zone::varchar(75) as zone,
        territory::varchar(75) as territory,
        channel::varchar(225) as channel,
        retail_environment::varchar(75) as retail_environment,
        salesman_name::varchar(200) as salesman_name,
        salesman_code::varchar(100) as salesman_code,
        distributor_code::varchar(50) as distributor_code,
        distributor_name::varchar(150) as distributor_name,
        store_code::varchar(50) as store_code,
        store_name::varchar(1000) as store_name,
        program_type::varchar(50) as program_type,
        franchise::varchar(50) as franchise,
        kpi::varchar(75) as kpi,
        quarter::varchar(50) as quarter,
        year::varchar(50) as year,
        source_actual_value::varchar(50) as source_actual_value,
        source_target_value::varchar(50) as source_target_value,
        actual_value::number(30,24) as actual_value,
        target_value::number(30,24) as target_value,
        compliance::number(30,24) as compliance,
        weight::number(31,2) as weight,
        kpi_score::number(30,24) as kpi_score,
        kpi_achievement::number(30,24) as kpi_achievement,
        max_potential_brand_score::number(38,26) as max_potential_brand_score,
        achievement_nr::number(38,6) as achievement_nr,
        prod_hier_l1::varchar(50) as prod_hier_l1,
        prod_hier_l2::varchar(100) as prod_hier_l2,
        prod_hier_l3::varchar(100) as prod_hier_l3,
        prod_hier_l4::varchar(100) as prod_hier_l4,
        prod_hier_l5::varchar(50) as prod_hier_l5,
        prod_hier_l6::varchar(100) as prod_hier_l6,
        prod_hier_l7::varchar(100) as prod_hier_l7,
        prod_hier_l8::varchar(100) as prod_hier_l8,
        prod_hier_l9::varchar(100) as prod_hier_l9,
        actual_value_msl_l9::varchar(5) as actual_value_msl_l9,
        target_value_msl_l9::varchar(5) as target_value_msl_l9,
        rtruniquecode::varchar(100) as rtruniquecode,
        promotor_store::varchar(1) as promotor_store,
        crt_dtt::timestamp_ntz(9) as crt_dtt,
        updt_dttm::timestamp_ntz(9) as updt_dttm,
        rebase_score::number(38,28) as rebase_score
    from transformed
)
select * from final