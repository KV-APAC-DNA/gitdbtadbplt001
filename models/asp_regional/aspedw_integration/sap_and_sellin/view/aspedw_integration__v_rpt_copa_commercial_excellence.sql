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

 SELECT main.latest_date, main.latest_fisc_yrmnth, main.fisc_yr, main.fisc_yr_per, calendar.cal_wk, main.caln_day, main.fisc_day, main.ctry_nm, main."cluster", mat.mega_brnd_desc AS "b1 mega-brand", cus_sales_extn.retail_env, 
 sum(main.nts_usd) AS nts_usd, sum(main.nts_lcy) AS nts_lcy, sum(main.gts_usd) AS gts_usd, sum(main.gts_lcy) AS gts_lcy, 
 sum(main.eq_usd) AS eq_usd, sum(main.eq_lcy) AS eq_lcy, main.from_crncy, main.cust_num
   FROM ( SELECT (("date_part"('year'::character varying::text, to_date(calendar.fisc_yr::character varying::text || "substring"(calendar.fisc_per::character varying::text, 6), 'yyyymm'::character varying::text) - 1)::character varying::text || '0'::character varying::text) || "date_part"('month'::character varying::text, to_date(calendar.fisc_yr::character varying::text || "substring"(calendar.fisc_per::character varying::text, 6), 'yyyymm'::character varying::text) - 1)::character varying::text)::character varying AS prev_fisc_yr_per, to_char(convert_timezone('SGT'::character varying::text, 'now'::character varying::timestamp without time zone), 'YYYYMMDD'::character varying::text)::character varying AS latest_date, (calendar.fisc_yr::character varying::text || "substring"(calendar.fisc_per::character varying::text, 6))::character varying AS latest_fisc_yrmnth, copa.fisc_yr, copa.fisc_yr_per, to_date(("substring"(copa.fisc_yr_per::character varying::text, 6, 8) || '01'::character varying::text) || "substring"(copa.fisc_yr_per::character varying::text, 1, 4), 'MMDDYYYY'::character varying::text) AS fisc_day, 
                CASE
                    WHEN (ltrim(copa.cust_num::text, 0::character varying::text) = '134559'::character varying::text OR ltrim(copa.cust_num::text, 0::character varying::text) = '134106'::character varying::text OR ltrim(copa.cust_num::text, 0::character varying::text) = '134258'::character varying::text OR ltrim(copa.cust_num::text, 0::character varying::text) = '134855'::character varying::text) AND ltrim(copa.acct_num::text, 0::character varying::text) <> '403185'::character varying::text AND mat.mega_brnd_desc::text <> 'Vogue Int\'l'::character varying::text AND copa.fisc_yr = 2018 THEN 'China Selfcare'::character varying
                    ELSE cmp.ctry_group
                END AS ctry_nm, 
                CASE
                    WHEN (ltrim(copa.cust_num::text, 0::character varying::text) = '134559'::character varying::text OR ltrim(copa.cust_num::text, 0::character varying::text) = '134106'::character varying::text OR ltrim(copa.cust_num::text, 0::character varying::text) = '134258'::character varying::text OR ltrim(copa.cust_num::text, 0::character varying::text) = '134855'::character varying::text) AND ltrim(copa.acct_num::text, 0::character varying::text) <> '403185'::character varying::text AND mat.mega_brnd_desc::text <> 'Vogue Int\'l'::character varying::text AND copa.fisc_yr = 2018 THEN 'China'::character varying
                    ELSE cmp."cluster"
                END AS "cluster", 
                CASE
                    WHEN cmp.ctry_group::text = 'India'::character varying::text THEN 'INR'::character varying
                    WHEN cmp.ctry_group::text = 'Philippines'::character varying::text THEN 'PHP'::character varying
                    WHEN cmp.ctry_group::text = 'China Selfcare'::character varying::text OR cmp.ctry_group::text = 'China Personal Care'::character varying::text THEN 'RMB'::character varying
                    ELSE copa.obj_crncy_co_obj
                END AS obj_crncy_co_obj, copa.matl_num, copa.co_cd, 
                CASE
                    WHEN ltrim(copa.cust_num::text, '0'::character varying::text) = '135520'::character varying::text AND (copa.co_cd::text = '703A'::character varying::text OR copa.co_cd::text = '8888'::character varying::text) THEN '100A'::character varying
                    ELSE copa.sls_org
                END AS sls_org, 
                CASE
                    WHEN ltrim(copa.cust_num::text, '0'::character varying::text) = '135520'::character varying::text AND (copa.co_cd::text = '703A'::character varying::text OR copa.co_cd::text = '8888'::character varying::text) THEN '15'::character varying
                    ELSE copa.dstr_chnl
                END AS dstr_chnl, copa.div, copa.cust_num, 
                CASE
                    WHEN copa.acct_hier_shrt_desc::text = 'NTS'::character varying::text AND exch_rate.to_crncy::text = 'USD'::character varying::text THEN sum(copa.amt_obj_crncy * exch_rate.ex_rt)
                    ELSE 0::numeric::numeric(18,0)
                END AS nts_usd, 
                CASE
                    WHEN copa.acct_hier_shrt_desc::text = 'NTS'::character varying::text AND exch_rate.to_crncy::text = 
                    CASE
                        WHEN cmp.ctry_group::text = 'India'::character varying::text OR cmp.ctry_group IS NULL AND 'India' IS NULL THEN 'INR'::character varying
                        WHEN cmp.ctry_group::text = 'Philippines'::character varying::text OR cmp.ctry_group IS NULL AND 'Philippines' IS NULL THEN 'PHP'::character varying
                        WHEN cmp.ctry_group::text = 'China Selfcare'::character varying::text OR cmp.ctry_group IS NULL AND 'China Selfcare' IS NULL THEN 'RMB'::character varying
                        WHEN cmp.ctry_group::text = 'China Personal Care'::character varying::text OR cmp.ctry_group IS NULL AND 'China Personal Care' IS NULL THEN 'RMB'::character varying
                        ELSE copa.obj_crncy_co_obj
                    END::text THEN sum(copa.amt_obj_crncy * exch_rate.ex_rt)
                    ELSE 0::numeric::numeric(18,0)
                END AS nts_lcy, 
                CASE
                    WHEN copa.acct_hier_shrt_desc::text = 'GTS'::character varying::text AND exch_rate.to_crncy::text = 'USD'::character varying::text THEN sum(copa.amt_obj_crncy * exch_rate.ex_rt)
                    ELSE 0::numeric::numeric(18,0)
                END AS gts_usd, 
                CASE
                    WHEN copa.acct_hier_shrt_desc::text = 'GTS'::character varying::text AND exch_rate.to_crncy::text = 
                    CASE
                        WHEN cmp.ctry_group::text = 'India'::character varying::text OR cmp.ctry_group IS NULL AND 'India' IS NULL THEN 'INR'::character varying
                        WHEN cmp.ctry_group::text = 'Philippines'::character varying::text OR cmp.ctry_group IS NULL AND 'Philippines' IS NULL THEN 'PHP'::character varying
                        WHEN cmp.ctry_group::text = 'China Selfcare'::character varying::text OR cmp.ctry_group IS NULL AND 'China Selfcare' IS NULL THEN 'RMB'::character varying
                        WHEN cmp.ctry_group::text = 'China Personal Care'::character varying::text OR cmp.ctry_group IS NULL AND 'China Personal Care' IS NULL THEN 'RMB'::character varying
                        ELSE copa.obj_crncy_co_obj
                    END::text THEN sum(copa.amt_obj_crncy * exch_rate.ex_rt)
                    ELSE 0::numeric::numeric(18,0)
                END AS gts_lcy, 
                CASE
                    WHEN copa.acct_hier_shrt_desc::text = 'EQ'::character varying::text AND exch_rate.to_crncy::text = 'USD'::character varying::text THEN sum(copa.amt_obj_crncy * exch_rate.ex_rt)
                    ELSE 0::numeric::numeric(18,0)
                END AS eq_usd, 
                CASE
                    WHEN copa.acct_hier_shrt_desc::text = 'EQ'::character varying::text AND exch_rate.to_crncy::text = 
                    CASE
                        WHEN cmp.ctry_group::text = 'India'::character varying::text OR cmp.ctry_group IS NULL AND 'India' IS NULL THEN 'INR'::character varying
                        WHEN cmp.ctry_group::text = 'Philippines'::character varying::text OR cmp.ctry_group IS NULL AND 'Philippines' IS NULL THEN 'PHP'::character varying
                        WHEN cmp.ctry_group::text = 'China Selfcare'::character varying::text OR cmp.ctry_group IS NULL AND 'China Selfcare' IS NULL THEN 'RMB'::character varying
                        WHEN cmp.ctry_group::text = 'China Personal Care'::character varying::text OR cmp.ctry_group IS NULL AND 'China Personal Care' IS NULL THEN 'RMB'::character varying
                        ELSE copa.obj_crncy_co_obj
                    END::text THEN sum(copa.amt_obj_crncy * exch_rate.ex_rt)
                    ELSE NULL::numeric::numeric(18,0)
                END AS eq_lcy, 
                CASE
                    WHEN cmp.ctry_group::text = 'India'::character varying::text THEN 'INR'::character varying
                    WHEN cmp.ctry_group::text = 'Philippines'::character varying::text THEN 'PHP'::character varying
                    WHEN cmp.ctry_group::text = 'China Selfcare'::character varying::text OR cmp.ctry_group::text = 'China Personal Care'::character varying::text THEN 'RMB'::character varying
                    ELSE exch_rate.from_crncy
                END AS from_crncy, exch_rate.to_crncy, 
                CASE
                    WHEN copa.acct_hier_shrt_desc::text = 'NTS'::character varying::text AND exch_rate.to_crncy::text = 'USD'::character varying::text THEN sum(copa.qty)
                    ELSE 0::numeric::numeric(18,0)
                END AS nts_qty, 
                CASE
                    WHEN copa.acct_hier_shrt_desc::text = 'GTS'::character varying::text AND exch_rate.to_crncy::text = 'USD'::character varying::text THEN sum(copa.qty)
                    ELSE 0::numeric::numeric(18,0)
                END AS gts_qty, 
                CASE
                    WHEN copa.acct_hier_shrt_desc::text = 'EQ'::character varying::text AND exch_rate.to_crncy::text = 'USD'::character varying::text THEN sum(copa.qty)
                    ELSE 0::numeric::numeric(18,0)
                END AS eq_qty, 0 AS ord_pc_qty, 0 AS unspp_qty, copa.caln_day
           FROM edw_copa_trans_fact copa
      LEFT JOIN edw_calendar_dim calendar ON calendar.cal_day = to_date(convert_timezone('SGT'::character varying::text, 'now'::character varying::timestamp without time zone)::character varying::text, 'YYYY-MM-DD'::character varying::text)
   LEFT JOIN edw_company_dim cmp ON copa.co_cd::text = cmp.co_cd::text
   LEFT JOIN edw_material_dim mat ON copa.matl_num::text = mat.matl_num::text
   LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate ON copa.obj_crncy_co_obj::text = exch_rate.from_crncy::text AND copa.fisc_yr_per = exch_rate.fisc_per AND 
CASE
WHEN exch_rate.to_crncy::text <> 'USD'::character varying::text THEN exch_rate.to_crncy::text = 
CASE
    WHEN cmp.ctry_group::text = 'India'::character varying::text OR cmp.ctry_group IS NULL AND 'India' IS NULL THEN 'INR'::character varying
    WHEN cmp.ctry_group::text = 'Philippines'::character varying::text OR cmp.ctry_group IS NULL AND 'Philippines' IS NULL THEN 'PHP'::character varying
    WHEN cmp.ctry_group::text = 'China Selfcare'::character varying::text OR cmp.ctry_group IS NULL AND 'China Selfcare' IS NULL THEN 'RMB'::character varying
    WHEN cmp.ctry_group::text = 'China Personal Care'::character varying::text OR cmp.ctry_group IS NULL AND 'China Personal Care' IS NULL THEN 'RMB'::character varying
    ELSE copa.obj_crncy_co_obj
END::text
ELSE exch_rate.to_crncy::text = 'USD'::character varying::text
END
  WHERE (copa.acct_hier_shrt_desc::text = 'GTS'::character varying::text OR copa.acct_hier_shrt_desc::text = 'NTS'::character varying::text OR copa.acct_hier_shrt_desc::text = 'EQ'::character varying::text) AND copa.fisc_yr_per::character varying::text >= ((((pgdate_part('YEAR'::character varying::text, 'now'::character varying::date::timestamp without time zone) - 2::double precision)::character varying::text || 0::character varying::text) || 0::character varying::text) || 1::character varying::text)
  GROUP BY calendar.fisc_yr, calendar.fisc_per, copa.fisc_yr, copa.fisc_yr_per, copa.caln_day, copa.obj_crncy_co_obj, copa.matl_num, copa.co_cd, copa.sls_org, copa.dstr_chnl, copa.div, copa.cust_num, copa.acct_num, copa.acct_hier_shrt_desc, exch_rate.from_crncy, exch_rate.to_crncy, cmp.ctry_group, cmp."cluster", mat.mega_brnd_desc) main
   LEFT JOIN edw_material_dim mat ON main.matl_num::text = mat.matl_num::text
   JOIN edw_company_dim company ON main.co_cd::text = company.co_cd::text
   LEFT JOIN v_edw_customer_sales_dim cus_sales_extn ON main.sls_org::text = cus_sales_extn.sls_org::text AND main.dstr_chnl::text = cus_sales_extn.dstr_chnl::text AND main.div::text = cus_sales_extn.div::text AND main.cust_num::text = cus_sales_extn.cust_num::text
   LEFT JOIN edw_calendar_dim calendar ON calendar.cal_day = main.fisc_day
  GROUP BY main.latest_date, main.latest_fisc_yrmnth, main.fisc_yr, main.fisc_yr_per, calendar.cal_wk, main.caln_day, main.fisc_day, main.ctry_nm, main."cluster", mat.mega_brnd_desc, cus_sales_extn.retail_env, main.from_crncy, main.cust_num;

)

select * from final