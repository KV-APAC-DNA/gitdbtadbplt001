with edw_company_dim as (
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_copa_trans_fact as (
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_material_dim as (
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
v_intrm_reg_crncy_exch_fiscper as (
    select * from {{ ref('aspedw_integration__v_intrm_reg_crncy_exch_fiscper') }}
),
v_edw_customer_sales_dim as (
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),

final as(

 SELECT MAIN.LATEST_DATE, MAIN.LATEST_FISC_YRMNTH, MAIN.FISC_YR, MAIN.FISC_YR_PER, CALENDAR.CAL_WK, MAIN.CALN_DAY, MAIN.FISC_DAY, MAIN.CTRY_NM, MAIN."cluster", MAT.MEGA_BRND_DESC AS "b1 mega-brand", CUS_SALES_EXTN.RETAIL_ENV,		--//  SELECT main.latest_date, main.latest_fisc_yrmnth, main.fisc_yr, main.fisc_yr_per, calendar.cal_wk, main.caln_day, main.fisc_day, main.ctry_nm, main."cluster", mat.mega_brnd_desc AS "b1 mega-brand", cus_sales_extn.retail_env,
 SUM(MAIN.NTS_USD) AS NTS_USD, SUM(MAIN.NTS_LCY) AS NTS_LCY, SUM(MAIN.GTS_USD) AS gts_usd, SUM(MAIN.GTS_LCY) AS gts_lcy,		--//  sum(main.nts_usd) AS nts_usd, sum(main.nts_lcy) AS nts_lcy, sum(main.gts_usd) AS gts_usd, sum(main.gts_lcy) AS gts_lcy,
 SUM(MAIN.EQ_USD) AS EQ_USD, SUM(MAIN.EQ_LCY) AS EQ_LCY, MAIN.FROM_CRNCY, MAIN.CUST_NUM		--//  sum(main.eq_usd) AS eq_usd, sum(main.eq_lcy) AS eq_lcy, main.from_crncy, main.cust_num
   FROM ( SELECT (("DATE_PART"('YEAR', TO_DATE(CALENDAR.FISC_YR::CHARACTER VARYING::TEXT || "SUBSTRING"(CALENDAR.FISC_PER::CHARACTER VARYING::TEXT, 6), 'YYYYMM'::CHARACTER VARYING::TEXT) - 1)::CHARACTER VARYING::TEXT || '0'::CHARACTER VARYING::TEXT) || "DATE_PART"('MONTH', TO_DATE(CALENDAR.FISC_YR::CHARACTER VARYING::TEXT || "SUBSTRING"(CALENDAR.FISC_PER::CHARACTER VARYING::TEXT, 6), 'YYYYMM'::CHARACTER VARYING::TEXT) - 1)::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS PREV_FISC_YR_PER, TO_CHAR(CONVERT_TIMEZONE('Asia/Singapore', CURRENT_TIMESTAMP()::TIMESTAMP WITHOUT TIME ZONE), 'YYYYMMDD'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS LATEST_DATE, (CALENDAR.FISC_YR::CHARACTER VARYING::TEXT || "SUBSTRING"(CALENDAR.FISC_PER::CHARACTER VARYING::TEXT, 6))::CHARACTER VARYING AS LATEST_FISC_YRMNTH, COPA.FISC_YR, COPA.FISC_YR_PER, to_date(("substring"(copa.fisc_yr_per::character varying::text, 6, 8) || '01'::character varying::text) || "substring"(copa.fisc_yr_per::character varying::text, 1, 4), 'MMDDYYYY'::varchar::text) AS fisc_day,		--//    FROM ( SELECT (("date_part"('year'::character varying::text, to_date(calendar.fisc_yr::character varying::text || "substring"(calendar.fisc_per::character varying::text, 6), 'yyyymm'::character varying::text) - 1)::character varying::text || '0'::character varying::text) || "date_part"('month'::character varying::text, to_date(calendar.fisc_yr::character varying::text || "substring"(calendar.fisc_per::character varying::text, 6), 'yyyymm'::character varying::text) - 1)::character varying::text)::character varying AS prev_fisc_yr_per, to_char(convert_timezone('SGT'::character varying::text, 'now'::character varying::timestamp without time zone), 'YYYYMMDD'::character varying::text)::character varying AS latest_date, (calendar.fisc_yr::character varying::text || "substring"(calendar.fisc_per::character varying::text, 6))::character varying AS latest_fisc_yrmnth, copa.fisc_yr, copa.fisc_yr_per, to_date(("substring"(copa.fisc_yr_per::character varying::text, 6, 8) || '01'::character varying::text) || "substring"(copa.fisc_yr_per::character varying::text, 1, 4), 'MMDDYYYY'::character varying::text) AS fisc_day, // character varying //    FROM ( SELECT (("date_part"('year'::character varying::text, to_date(calendar.fisc_yr::character varying::text || "substring"(calendar.fisc_per::character varying::text, 6), 'yyyymm'::character varying::text) - 1)::character varying::text || '0'::character varying::text) || "date_part"('month'::character varying::text, to_date(calendar.fisc_yr::character varying::text || "substring"(calendar.fisc_per::character varying::text, 6), 'yyyymm'::character varying::text) - 1)::character varying::text)::character varying AS prev_fisc_yr_per, to_char(convert_timezone('SGT'::character varying::text, 'CURRENT_TIMESTAMP()'::character varying::timestamp without time zone), 'YYYYMMDD'::character varying::text)::character varying AS latest_date, (calendar.fisc_yr::character varying::text || "substring"(calendar.fisc_per::character varying::text, 6))::character varying AS latest_fisc_yrmnth, copa.fisc_yr, copa.fisc_yr_per, to_date(("substring"(copa.fisc_yr_per::character varying::text, 6, 8) || '01'::character varying::text) || "substring"(copa.fisc_yr_per::character varying::text, 1, 4), 'MMDDYYYY'::varchar::text) AS fisc_day,
                CASE
                    WHEN (LTRIM(COPA.CUST_NUM::TEXT, 0::CHARACTER VARYING::TEXT) = '134559'::CHARACTER VARYING::TEXT OR LTRIM(COPA.CUST_NUM::TEXT, 0::CHARACTER VARYING::TEXT) = '134106'::CHARACTER VARYING::TEXT OR LTRIM(COPA.CUST_NUM::TEXT, 0::CHARACTER VARYING::TEXT) = '134258'::CHARACTER VARYING::TEXT OR LTRIM(COPA.CUST_NUM::TEXT, 0::CHARACTER VARYING::TEXT) = '134855'::CHARACTER VARYING::TEXT) AND LTRIM(COPA.ACCT_NUM::TEXT, 0::CHARACTER VARYING::TEXT) <> '403185'::CHARACTER VARYING::TEXT AND MAT.MEGA_BRND_DESC::text <> 'Vogue Int\'l'::character varying::text AND COPA.FISC_YR = 2018 THEN 'China Selfcare'::varchar		--// character varying //                     WHEN (ltrim(copa.cust_num::text, 0::character varying::text) = '134559'::character varying::text OR ltrim(copa.cust_num::text, 0::character varying::text) = '134106'::character varying::text OR ltrim(copa.cust_num::text, 0::character varying::text) = '134258'::character varying::text OR ltrim(copa.cust_num::text, 0::character varying::text) = '134855'::character varying::text) AND ltrim(copa.acct_num::text, 0::character varying::text) <> '403185'::character varying::text AND mat.mega_brnd_desc::text <> 'Vogue Int\'l'::character varying::text AND copa.fisc_yr = 2018 THEN 'China Selfcare'::varchar
                    ELSE CMP.CTRY_GROUP		--//                     ELSE cmp.ctry_group
                END AS ctry_nm,
                CASE
                    WHEN (LTRIM(COPA.CUST_NUM::TEXT, 0::CHARACTER VARYING::TEXT) = '134559'::CHARACTER VARYING::TEXT OR LTRIM(COPA.CUST_NUM::TEXT, 0::CHARACTER VARYING::TEXT) = '134106'::CHARACTER VARYING::TEXT OR LTRIM(COPA.CUST_NUM::TEXT, 0::CHARACTER VARYING::TEXT) = '134258'::CHARACTER VARYING::TEXT OR LTRIM(COPA.CUST_NUM::TEXT, 0::CHARACTER VARYING::TEXT) = '134855'::CHARACTER VARYING::TEXT) AND LTRIM(COPA.ACCT_NUM::TEXT, 0::CHARACTER VARYING::TEXT) <> '403185'::CHARACTER VARYING::TEXT AND MAT.MEGA_BRND_DESC::text <> 'Vogue Int\'l'::character varying::text AND COPA.FISC_YR = 2018 THEN 'China'::varchar		--// character varying //                     WHEN (ltrim(copa.cust_num::text, 0::character varying::text) = '134559'::character varying::text OR ltrim(copa.cust_num::text, 0::character varying::text) = '134106'::character varying::text OR ltrim(copa.cust_num::text, 0::character varying::text) = '134258'::character varying::text OR ltrim(copa.cust_num::text, 0::character varying::text) = '134855'::character varying::text) AND ltrim(copa.acct_num::text, 0::character varying::text) <> '403185'::character varying::text AND mat.mega_brnd_desc::text <> 'Vogue Int\'l'::character varying::text AND copa.fisc_yr = 2018 THEN 'China'::varchar
                    ELSE CMP."cluster"		--//                     ELSE cmp."cluster"
                END AS "cluster",
                CASE
                    WHEN CMP.CTRY_GROUP::text = 'India'::character varying::text THEN 'INR'::varchar		--// character varying //                     WHEN cmp.ctry_group::text = 'India'::character varying::text THEN 'INR'::varchar
                    WHEN CMP.CTRY_GROUP::text = 'Philippines'::character varying::text THEN 'PHP'::varchar		--// character varying //                     WHEN cmp.ctry_group::text = 'Philippines'::character varying::text THEN 'PHP'::varchar
                    WHEN CMP.CTRY_GROUP::text = 'China Selfcare'::character varying::text OR CMP.CTRY_GROUP::text = 'China Personal Care'::character varying::text THEN 'RMB'::varchar		--// character varying //                     WHEN cmp.ctry_group::text = 'China Selfcare'::character varying::text OR cmp.ctry_group::text = 'China Personal Care'::character varying::text THEN 'RMB'::varchar
                    ELSE COPA.OBJ_CRNCY_CO_OBJ		--//                     ELSE copa.obj_crncy_co_obj
                END AS OBJ_CRNCY_CO_OBJ, COPA.MATL_NUM, COPA.CO_CD,		--//                 END AS obj_crncy_co_obj, copa.matl_num, copa.co_cd,
                CASE
                    WHEN LTRIM(COPA.CUST_NUM::TEXT, '0'::CHARACTER VARYING::TEXT) = '135520'::CHARACTER VARYING::TEXT AND (COPA.CO_CD::text = '703A'::character varying::text OR COPA.CO_CD::text = '8888'::character varying::text) THEN '100A'::varchar		--// character varying //                     WHEN ltrim(copa.cust_num::text, '0'::character varying::text) = '135520'::character varying::text AND (copa.co_cd::text = '703A'::character varying::text OR copa.co_cd::text = '8888'::character varying::text) THEN '100A'::varchar
                    ELSE COPA.SLS_ORG		--//                     ELSE copa.sls_org
                END AS sls_org,
                CASE
                    WHEN LTRIM(COPA.CUST_NUM::TEXT, '0'::CHARACTER VARYING::TEXT) = '135520'::CHARACTER VARYING::TEXT AND (COPA.CO_CD::text = '703A'::character varying::text OR COPA.CO_CD::text = '8888'::character varying::text) THEN '15'::varchar		--// character varying //                     WHEN ltrim(copa.cust_num::text, '0'::character varying::text) = '135520'::character varying::text AND (copa.co_cd::text = '703A'::character varying::text OR copa.co_cd::text = '8888'::character varying::text) THEN '15'::varchar
                    ELSE COPA.DSTR_CHNL		--//                     ELSE copa.dstr_chnl
                END AS DSTR_CHNL, COPA.DIV, COPA.CUST_NUM,		--//                 END AS dstr_chnl, copa.div, copa.cust_num,
                CASE
                    WHEN COPA.ACCT_HIER_SHRT_DESC::TEXT = 'NTS'::CHARACTER VARYING::TEXT AND EXCH_RATE.TO_CRNCY::TEXT = 'USD'::VARCHAR::TEXT THEN SUM(COPA.AMT_OBJ_CRNCY * EXCH_RATE.EX_RT)		--// character varying //                     WHEN copa.acct_hier_shrt_desc::text = 'NTS'::character varying::text AND exch_rate.to_crncy::text = 'USD'::varchar::text THEN sum(copa.amt_obj_crncy * exch_rate.ex_rt)
                    ELSE 0::numeric::numeric(18,0)
                END AS nts_usd,
                CASE
                    WHEN COPA.ACCT_HIER_SHRT_DESC::text = 'NTS'::varchar::text AND EXCH_RATE.TO_CRNCY::text =		--// character varying //                     WHEN copa.acct_hier_shrt_desc::text = 'NTS'::varchar::text AND exch_rate.to_crncy::text =
                    CASE
                        WHEN CMP.CTRY_GROUP::text = 'India'::character varying::text OR CMP.CTRY_GROUP IS NULL AND 'India' IS NULL THEN 'INR'::varchar		--// character varying //                         WHEN cmp.ctry_group::text = 'India'::character varying::text OR cmp.ctry_group IS NULL AND 'India' IS NULL THEN 'INR'::varchar
                        WHEN CMP.CTRY_GROUP::text = 'Philippines'::character varying::text OR CMP.CTRY_GROUP IS NULL AND 'Philippines' IS NULL THEN 'PHP'::varchar		--// character varying //                         WHEN cmp.ctry_group::text = 'Philippines'::character varying::text OR cmp.ctry_group IS NULL AND 'Philippines' IS NULL THEN 'PHP'::varchar
                        WHEN CMP.CTRY_GROUP::text = 'China Selfcare'::character varying::text OR CMP.CTRY_GROUP IS NULL AND 'China Selfcare' IS NULL THEN 'RMB'::varchar		--// character varying //                         WHEN cmp.ctry_group::text = 'China Selfcare'::character varying::text OR cmp.ctry_group IS NULL AND 'China Selfcare' IS NULL THEN 'RMB'::varchar
                        WHEN CMP.CTRY_GROUP::text = 'China Personal Care'::character varying::text OR CMP.CTRY_GROUP IS NULL AND 'China Personal Care' IS NULL THEN 'RMB'::varchar		--// character varying //                         WHEN cmp.ctry_group::text = 'China Personal Care'::character varying::text OR cmp.ctry_group IS NULL AND 'China Personal Care' IS NULL THEN 'RMB'::varchar
                        ELSE COPA.OBJ_CRNCY_CO_OBJ		--//                         ELSE copa.obj_crncy_co_obj
                    END::TEXT THEN SUM(COPA.AMT_OBJ_CRNCY * EXCH_RATE.EX_RT)		--//                     END::text THEN sum(copa.amt_obj_crncy * exch_rate.ex_rt)
                    ELSE 0::numeric::numeric(18,0)
                END AS nts_lcy,
                CASE
                    WHEN COPA.ACCT_HIER_SHRT_DESC::TEXT = 'GTS'::CHARACTER VARYING::TEXT AND EXCH_RATE.TO_CRNCY::TEXT = 'USD'::VARCHAR::TEXT THEN SUM(COPA.AMT_OBJ_CRNCY * EXCH_RATE.EX_RT)		--// character varying //                     WHEN copa.acct_hier_shrt_desc::text = 'GTS'::character varying::text AND exch_rate.to_crncy::text = 'USD'::varchar::text THEN sum(copa.amt_obj_crncy * exch_rate.ex_rt)
                    ELSE 0::numeric::numeric(18,0)
                END AS gts_usd,
                CASE
                    WHEN COPA.ACCT_HIER_SHRT_DESC::text = 'GTS'::varchar::text AND EXCH_RATE.TO_CRNCY::text =		--// character varying //                     WHEN copa.acct_hier_shrt_desc::text = 'GTS'::varchar::text AND exch_rate.to_crncy::text =
                    CASE
                        WHEN CMP.CTRY_GROUP::text = 'India'::character varying::text OR CMP.CTRY_GROUP IS NULL AND 'India' IS NULL THEN 'INR'::varchar		--// character varying //                         WHEN cmp.ctry_group::text = 'India'::character varying::text OR cmp.ctry_group IS NULL AND 'India' IS NULL THEN 'INR'::varchar
                        WHEN CMP.CTRY_GROUP::text = 'Philippines'::character varying::text OR CMP.CTRY_GROUP IS NULL AND 'Philippines' IS NULL THEN 'PHP'::varchar		--// character varying //                         WHEN cmp.ctry_group::text = 'Philippines'::character varying::text OR cmp.ctry_group IS NULL AND 'Philippines' IS NULL THEN 'PHP'::varchar
                        WHEN CMP.CTRY_GROUP::text = 'China Selfcare'::character varying::text OR CMP.CTRY_GROUP IS NULL AND 'China Selfcare' IS NULL THEN 'RMB'::varchar		--// character varying //                         WHEN cmp.ctry_group::text = 'China Selfcare'::character varying::text OR cmp.ctry_group IS NULL AND 'China Selfcare' IS NULL THEN 'RMB'::varchar
                        WHEN CMP.CTRY_GROUP::text = 'China Personal Care'::character varying::text OR CMP.CTRY_GROUP IS NULL AND 'China Personal Care' IS NULL THEN 'RMB'::varchar		--// character varying //                         WHEN cmp.ctry_group::text = 'China Personal Care'::character varying::text OR cmp.ctry_group IS NULL AND 'China Personal Care' IS NULL THEN 'RMB'::varchar
                        ELSE COPA.OBJ_CRNCY_CO_OBJ		--//                         ELSE copa.obj_crncy_co_obj
                    END::TEXT THEN SUM(COPA.AMT_OBJ_CRNCY * EXCH_RATE.EX_RT)		--//                     END::text THEN sum(copa.amt_obj_crncy * exch_rate.ex_rt)
                    ELSE 0::numeric::numeric(18,0)
                END AS gts_lcy,
                CASE
                    WHEN COPA.ACCT_HIER_SHRT_DESC::TEXT = 'EQ'::CHARACTER VARYING::TEXT AND EXCH_RATE.TO_CRNCY::TEXT = 'USD'::VARCHAR::TEXT THEN SUM(COPA.AMT_OBJ_CRNCY * EXCH_RATE.EX_RT)		--// character varying //                     WHEN copa.acct_hier_shrt_desc::text = 'EQ'::character varying::text AND exch_rate.to_crncy::text = 'USD'::varchar::text THEN sum(copa.amt_obj_crncy * exch_rate.ex_rt)
                    ELSE 0::numeric::numeric(18,0)
                END AS eq_usd,
                CASE
                    WHEN COPA.ACCT_HIER_SHRT_DESC::text = 'EQ'::varchar::text AND EXCH_RATE.TO_CRNCY::text =		--// character varying //                     WHEN copa.acct_hier_shrt_desc::text = 'EQ'::varchar::text AND exch_rate.to_crncy::text =
                    CASE
                        WHEN CMP.CTRY_GROUP::text = 'India'::character varying::text OR CMP.CTRY_GROUP IS NULL AND 'India' IS NULL THEN 'INR'::varchar		--// character varying //                         WHEN cmp.ctry_group::text = 'India'::character varying::text OR cmp.ctry_group IS NULL AND 'India' IS NULL THEN 'INR'::varchar
                        WHEN CMP.CTRY_GROUP::text = 'Philippines'::character varying::text OR CMP.CTRY_GROUP IS NULL AND 'Philippines' IS NULL THEN 'PHP'::varchar		--// character varying //                         WHEN cmp.ctry_group::text = 'Philippines'::character varying::text OR cmp.ctry_group IS NULL AND 'Philippines' IS NULL THEN 'PHP'::varchar
                        WHEN CMP.CTRY_GROUP::text = 'China Selfcare'::character varying::text OR CMP.CTRY_GROUP IS NULL AND 'China Selfcare' IS NULL THEN 'RMB'::varchar		--// character varying //                         WHEN cmp.ctry_group::text = 'China Selfcare'::character varying::text OR cmp.ctry_group IS NULL AND 'China Selfcare' IS NULL THEN 'RMB'::varchar
                        WHEN CMP.CTRY_GROUP::text = 'China Personal Care'::character varying::text OR CMP.CTRY_GROUP IS NULL AND 'China Personal Care' IS NULL THEN 'RMB'::varchar		--// character varying //                         WHEN cmp.ctry_group::text = 'China Personal Care'::character varying::text OR cmp.ctry_group IS NULL AND 'China Personal Care' IS NULL THEN 'RMB'::varchar
                        ELSE COPA.OBJ_CRNCY_CO_OBJ		--//                         ELSE copa.obj_crncy_co_obj
                    END::TEXT THEN SUM(COPA.AMT_OBJ_CRNCY * EXCH_RATE.EX_RT)		--//                     END::text THEN sum(copa.amt_obj_crncy * exch_rate.ex_rt)
                    ELSE NULL::numeric::numeric(18,0)
                END AS eq_lcy,
                CASE
                    WHEN CMP.CTRY_GROUP::text = 'India'::character varying::text THEN 'INR'::varchar		--// character varying //                     WHEN cmp.ctry_group::text = 'India'::character varying::text THEN 'INR'::varchar
                    WHEN CMP.CTRY_GROUP::text = 'Philippines'::character varying::text THEN 'PHP'::varchar		--// character varying //                     WHEN cmp.ctry_group::text = 'Philippines'::character varying::text THEN 'PHP'::varchar
                    WHEN CMP.CTRY_GROUP::text = 'China Selfcare'::character varying::text OR CMP.CTRY_GROUP::text = 'China Personal Care'::character varying::text THEN 'RMB'::varchar		--// character varying //                     WHEN cmp.ctry_group::text = 'China Selfcare'::character varying::text OR cmp.ctry_group::text = 'China Personal Care'::character varying::text THEN 'RMB'::varchar
                    ELSE EXCH_RATE.FROM_CRNCY		--//                     ELSE exch_rate.from_crncy
                END AS from_crncy, EXCH_RATE.TO_CRNCY,		--//                 END AS from_crncy, exch_rate.to_crncy,
                CASE
                    WHEN COPA.ACCT_HIER_SHRT_DESC::text = 'NTS'::character varying::text AND EXCH_RATE.TO_CRNCY::text = 'USD'::varchar::text THEN sum(copa.qty)		--// character varying //                     WHEN copa.acct_hier_shrt_desc::text = 'NTS'::character varying::text AND exch_rate.to_crncy::text = 'USD'::varchar::text THEN sum(copa.qty)
                    ELSE 0::numeric::numeric(18,0)
                END AS nts_qty,
                CASE
                    WHEN COPA.ACCT_HIER_SHRT_DESC::text = 'GTS'::character varying::text AND EXCH_RATE.TO_CRNCY::text = 'USD'::varchar::text THEN sum(copa.qty)		--// character varying //                     WHEN copa.acct_hier_shrt_desc::text = 'GTS'::character varying::text AND exch_rate.to_crncy::text = 'USD'::varchar::text THEN sum(copa.qty)
                    ELSE 0::numeric::numeric(18,0)
                END AS gts_qty,
                CASE
                    WHEN COPA.ACCT_HIER_SHRT_DESC::text = 'EQ'::character varying::text AND EXCH_RATE.TO_CRNCY::text = 'USD'::varchar::text THEN sum(copa.qty)		--// character varying //                     WHEN copa.acct_hier_shrt_desc::text = 'EQ'::character varying::text AND exch_rate.to_crncy::text = 'USD'::varchar::text THEN sum(copa.qty)
                    ELSE 0::numeric::numeric(18,0)
                END AS eq_qty, 0 AS ord_pc_qty, 0 AS unspp_qty, COPA.CALN_DAY		--//                 END AS eq_qty, 0 AS ord_pc_qty, 0 AS unspp_qty, copa.caln_day
           FROM edw_copa_trans_fact COPA
      LEFT JOIN edw_calendar_dim CALENDAR ON CALENDAR.CAL_DAY = to_date(to_char(convert_timezone('Asia/Singapore', CURRENT_TIMESTAMP()::timestamp without time zone), 'YYYY-MM-DD'))		--//       LEFT JOIN edw_calendar_dim calendar ON calendar.cal_day = to_date(convert_timezone('SGT'::character varying::text, 'now'::character varying::timestamp without time zone)::character varying::text, 'YYYY-MM-DD'::character varying::text) // character varying //       LEFT JOIN edw_calendar_dim calendar ON calendar.cal_day = to_date(convert_timezone('SGT'::character varying::text, 'CURRENT_TIMESTAMP()'::character varying::timestamp without time zone)::character varying::text, 'YYYY-MM-DD'::varchar::text)
   LEFT JOIN EDW_COMPANY_DIM CMP ON COPA.CO_CD::text = CMP.CO_CD::text		--//    LEFT JOIN edw_company_dim cmp ON copa.co_cd::text = cmp.co_cd::text
   LEFT JOIN EDW_MATERIAL_DIM MAT ON COPA.MATL_NUM::text = MAT.MATL_NUM::text		--//    LEFT JOIN edw_material_dim mat ON copa.matl_num::text = mat.matl_num::text
   LEFT JOIN V_INTRM_REG_CRNCY_EXCH_FISCPER EXCH_RATE ON COPA.OBJ_CRNCY_CO_OBJ::TEXT = EXCH_RATE.FROM_CRNCY::TEXT AND COPA.FISC_YR_PER = EXCH_RATE.FISC_PER AND		--//    LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate ON copa.obj_crncy_co_obj::text = exch_rate.from_crncy::text AND copa.fisc_yr_per = exch_rate.fisc_per AND
CASE
WHEN EXCH_RATE.TO_CRNCY::text <> 'USD'::varchar::text THEN EXCH_RATE.TO_CRNCY::text =		--// character varying // WHEN exch_rate.to_crncy::text <> 'USD'::varchar::text THEN exch_rate.to_crncy::text =
CASE
    WHEN CMP.CTRY_GROUP::text = 'India'::character varying::text OR CMP.CTRY_GROUP IS NULL AND 'India' IS NULL THEN 'INR'::varchar		--// character varying //     WHEN cmp.ctry_group::text = 'India'::character varying::text OR cmp.ctry_group IS NULL AND 'India' IS NULL THEN 'INR'::varchar
    WHEN CMP.CTRY_GROUP::text = 'Philippines'::character varying::text OR CMP.CTRY_GROUP IS NULL AND 'Philippines' IS NULL THEN 'PHP'::varchar		--// character varying //     WHEN cmp.ctry_group::text = 'Philippines'::character varying::text OR cmp.ctry_group IS NULL AND 'Philippines' IS NULL THEN 'PHP'::varchar
    WHEN CMP.CTRY_GROUP::text = 'China Selfcare'::character varying::text OR CMP.CTRY_GROUP IS NULL AND 'China Selfcare' IS NULL THEN 'RMB'::varchar		--// character varying //     WHEN cmp.ctry_group::text = 'China Selfcare'::character varying::text OR cmp.ctry_group IS NULL AND 'China Selfcare' IS NULL THEN 'RMB'::varchar
    WHEN CMP.CTRY_GROUP::text = 'China Personal Care'::character varying::text OR CMP.CTRY_GROUP IS NULL AND 'China Personal Care' IS NULL THEN 'RMB'::varchar		--// character varying //     WHEN cmp.ctry_group::text = 'China Personal Care'::character varying::text OR cmp.ctry_group IS NULL AND 'China Personal Care' IS NULL THEN 'RMB'::varchar
    ELSE COPA.OBJ_CRNCY_CO_OBJ		--//     ELSE copa.obj_crncy_co_obj
END::text
ELSE EXCH_RATE.TO_CRNCY::text = 'USD'::varchar::text		--// character varying // ELSE exch_rate.to_crncy::text = 'USD'::varchar::text
END
  WHERE (COPA.ACCT_HIER_SHRT_DESC::TEXT = 'GTS'::CHARACTER VARYING::TEXT OR COPA.ACCT_HIER_SHRT_DESC::TEXT = 'NTS'::CHARACTER VARYING::TEXT OR COPA.ACCT_HIER_SHRT_DESC::text = 'EQ'::character varying::text) AND COPA.FISC_YR_PER::character varying::text >= (((("DATE_PART"('YEAR', CURRENT_TIMESTAMP()::timestamp without time zone) - 2::double precision)::character varying::text || 0::character varying::text) || 0::character varying::text) || 1::varchar::text)		--//   WHERE (copa.acct_hier_shrt_desc::text = 'GTS'::character varying::text OR copa.acct_hier_shrt_desc::text = 'NTS'::character varying::text OR copa.acct_hier_shrt_desc::text = 'EQ'::character varying::text) AND copa.fisc_yr_per::character varying::text >= ((((pgdate_part('YEAR'::character varying::text, 'now'::character varying::date::timestamp without time zone) - 2::double precision)::character varying::text || 0::character varying::text) || 0::character varying::text) || 1::character varying::text) // character varying //   WHERE (copa.acct_hier_shrt_desc::text = 'GTS'::character varying::text OR copa.acct_hier_shrt_desc::text = 'NTS'::character varying::text OR copa.acct_hier_shrt_desc::text = 'EQ'::character varying::text) AND copa.fisc_yr_per::character varying::text >= ((((pgdate_part('YEAR'::character varying::text, 'CURRENT_TIMESTAMP()'::date::timestamp without time zone) - 2::double precision)::character varying::text || 0::character varying::text) || 0::character varying::text) || 1::varchar::text)
  GROUP BY CALENDAR.FISC_YR, CALENDAR.FISC_PER, COPA.FISC_YR, COPA.FISC_YR_PER, COPA.CALN_DAY, COPA.OBJ_CRNCY_CO_OBJ, COPA.MATL_NUM, COPA.CO_CD, COPA.SLS_ORG, COPA.DSTR_CHNL, COPA.DIV, COPA.CUST_NUM, COPA.ACCT_NUM, COPA.ACCT_HIER_SHRT_DESC, EXCH_RATE.FROM_CRNCY, EXCH_RATE.TO_CRNCY, CMP.CTRY_GROUP, CMP."cluster", MAT.MEGA_BRND_DESC) MAIN		--//   GROUP BY calendar.fisc_yr, calendar.fisc_per, copa.fisc_yr, copa.fisc_yr_per, copa.caln_day, copa.obj_crncy_co_obj, copa.matl_num, copa.co_cd, copa.sls_org, copa.dstr_chnl, copa.div, copa.cust_num, copa.acct_num, copa.acct_hier_shrt_desc, exch_rate.from_crncy, exch_rate.to_crncy, cmp.ctry_group, cmp."cluster", mat.mega_brnd_desc) main
   LEFT JOIN EDW_MATERIAL_DIM MAT ON MAIN.MATL_NUM::text = MAT.MATL_NUM::text		--//    LEFT JOIN edw_material_dim mat ON main.matl_num::text = mat.matl_num::text
   JOIN EDW_COMPANY_DIM COMPANY ON MAIN.CO_CD::text = COMPANY.CO_CD::text		--//    JOIN edw_company_dim company ON main.co_cd::text = company.co_cd::text
   LEFT JOIN V_EDW_CUSTOMER_SALES_DIM CUS_SALES_EXTN ON MAIN.SLS_ORG::TEXT = CUS_SALES_EXTN.SLS_ORG::TEXT AND MAIN.DSTR_CHNL::TEXT = CUS_SALES_EXTN.DSTR_CHNL::TEXT AND MAIN.DIV::TEXT = CUS_SALES_EXTN.DIV::TEXT AND MAIN.CUST_NUM::text = CUS_SALES_EXTN.CUST_NUM::text		--//    LEFT JOIN v_edw_customer_sales_dim cus_sales_extn ON main.sls_org::text = cus_sales_extn.sls_org::text AND main.dstr_chnl::text = cus_sales_extn.dstr_chnl::text AND main.div::text = cus_sales_extn.div::text AND main.cust_num::text = cus_sales_extn.cust_num::text
   LEFT JOIN EDW_CALENDAR_DIM CALENDAR ON CALENDAR.CAL_DAY = MAIN.FISC_DAY		--//    LEFT JOIN edw_calendar_dim calendar ON calendar.cal_day = main.fisc_day
  GROUP BY MAIN.LATEST_DATE, MAIN.LATEST_FISC_YRMNTH, MAIN.FISC_YR, MAIN.FISC_YR_PER, CALENDAR.CAL_WK, MAIN.CALN_DAY, MAIN.FISC_DAY, MAIN.CTRY_NM, MAIN."cluster", MAT.MEGA_BRND_DESC, CUS_SALES_EXTN.RETAIL_ENV, MAIN.FROM_CRNCY, MAIN.CUST_NUM		--//   GROUP BY main.latest_date, main.latest_fisc_yrmnth, main.fisc_yr, main.fisc_yr_per, calendar.cal_wk, main.caln_day, main.fisc_day, main.ctry_nm, main."cluster", mat.mega_brnd_desc, cus_sales_extn.retail_env, main.from_crncy, main.cust_num;


)

select * from final