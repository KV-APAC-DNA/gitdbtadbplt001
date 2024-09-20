with edw_custgp_cogs_automation as
(
    select * from {{ ref('aspedw_integration__edw_custgp_cogs_automation') }}
),
v_rpt_copa_gl_hid_disc as
(
    select * from {{ ref('aspedw_integration__v_rpt_copa_gl_hid_disc') }}
),
edw_company_dim as
(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
v_intrm_reg_crncy_exch_fiscper as
(
    select * from {{ ref('aspedw_integration__v_intrm_reg_crncy_exch_fiscper') }}
),
edw_calendar_dim as
(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
itg_custgp_vn_cogs_fg as
(
    select * from {{ source('aspitg_integration', 'itg_custgp_vn_cogs_fg') }}
),
final as
(
    SELECT fact.fisc_yr,
        fact.fisc_yr_per,
        CASE
            WHEN timedim.pstng_per >= 1 AND timedim.pstng_per <= 3 THEN 'Q1'::CHARACTER VARYING
            WHEN timedim.pstng_per >= 4 AND timedim.pstng_per <= 6 THEN 'Q2'::CHARACTER VARYING
            WHEN timedim.pstng_per >= 7 AND timedim.pstng_per <= 9 THEN 'Q3'::CHARACTER VARYING
            WHEN timedim.pstng_per >= 10 AND timedim.pstng_per <= 12 THEN 'Q4'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
        END AS jj_qtr,
        fact.fisc_day,
        fact.ctry_group AS ctry_nm,
        fact."cluster",
        fact.sls_org,
        ltrim(fact.prft_ctr,'0') as prft_Ctr,
        fact.obj_crncy_co_obj,
        fact.from_crncy,
        fact.to_crncy,
        fact.matl_num,
        fact.cust_num AS cust_num,
        fact.acct_hier_shrt_desc  AS ciw_tt_tp_classification,
        fact.acct_hier_shrt_desc,
        SUM(fact.qty) AS qty,
        SUM(fact.amt_lcy) AS amt_lcy,
        SUM(fact.amt_usd) AS amt_usd
    FROM 
        (  SELECT cogs.fisc_yr,
                cogs.fisc_yr_per,
                TO_DATE(("substring" (cogs.fisc_yr_per::CHARACTER VARYING::TEXT,6,8) || '01'::CHARACTER VARYING::TEXT) || "substring" (cogs.fisc_yr_per::CHARACTER VARYING::TEXT,1,4),'MMDDYYYY'::CHARACTER VARYING::TEXT) AS fisc_day,
                company.ctry_group,
                company."cluster",
                cogs.prft_ctr,
                NULL::CHARACTER VARYING AS acct_num,
                cogs.currency AS obj_crncy_co_obj,
                LTRIM(cogs.material_code::TEXT,'0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS matl_num,
                cogs.company_code AS co_cd,
                cogs.sls_org,
                --so.sls_org,
                NULL::CHARACTER VARYING AS dstr_chnl,
                NULL::CHARACTER VARYING AS DIV,
                LTRIM(cogs.cust_code::TEXT,'0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS cust_num,
                exch_rate.from_crncy,
                exch_rate.to_crncy,
                cogs.acct_hier_shrt_desc,
                NULL::NUMERIC::NUMERIC(18,0) AS qty,
                CASE
                WHEN exch_rate.to_crncy::TEXT = cogs.currency::TEXT THEN
                    CASE
                    WHEN cogs.acct_hier_shrt_desc::TEXT = 'STD.COGS'::CHARACTER VARYING::TEXT THEN cogs.std_cogs
                    WHEN cogs.acct_hier_shrt_desc::TEXT = 'CONS.FREE.GOODS'::CHARACTER VARYING::TEXT THEN cogs.cons_freegoods
                    WHEN cogs.acct_hier_shrt_desc::TEXT = 'HDPM'::CHARACTER VARYING::TEXT THEN cogs.amt_gl_hd
                    ELSE 0::NUMERIC::NUMERIC(18,0)
                    END 
                ELSE 0::NUMERIC::NUMERIC(18,0)
                END AS amt_lcy,
                CASE
                WHEN exch_rate.to_crncy::TEXT = 'USD'::CHARACTER VARYING::TEXT THEN
                    CASE
                    WHEN cogs.acct_hier_shrt_desc::TEXT = 'STD.COGS'::CHARACTER VARYING::TEXT THEN cogs.std_cogs*exch_rate.ex_rt
                    WHEN cogs.acct_hier_shrt_desc::TEXT = 'CONS.FREE.GOODS'::CHARACTER VARYING::TEXT THEN cogs.cons_freegoods*exch_rate.ex_rt
                    WHEN cogs.acct_hier_shrt_desc::TEXT = 'HDPM'::CHARACTER VARYING::TEXT THEN cogs.amt_gl_hd*exch_rate.ex_rt
                    ELSE 0::NUMERIC::NUMERIC(18,0)
                    END 
                ELSE NULL::NUMERIC::NUMERIC(18,0)
                END AS amt_usd
        FROM (SELECT 'NA' as market,
                        fisc_yr,
                        period as fisc_yr_per,
                        currency,
                        matl_num as material_code,
                        co_cd as company_code,
                        plant as sls_org,
                        profit_cntr as prft_ctr,
                        cust_num as cust_code,
                        cogs_at_pre_apsc as std_cogs,
                        0 as cons_freegoods,
                        0 as amt_gl_hd,
                        'STD.COGS'::CHARACTER VARYING AS acct_hier_shrt_desc
                FROM edw_custgp_cogs_automation 
                WHERE cogs_at_pre_apsc <> 0 

                UNION ALL

                SELECT 'NA' as market,
                        fisc_yr,
                        period as fisc_yr_per,
                        currency,
                        matl_num as material_code,
                        co_cd as company_code,
                        plant as sls_org,
                        profit_cntr as prft_ctr,
                        cust_num as cust_code,
                        0 as std_cogs,
                        free_goods_cogs_at_pre_apsc as cons_freegoods,
                        0 as amt_gl_hd,
                        'CONS.FREE.GOODS'::CHARACTER VARYING AS acct_hier_shrt_desc
                FROM edw_custgp_cogs_automation
                WHERE free_goods_cogs_at_pre_apsc <> 0 
        
                 /* Append manual COGS and FG - exclusive for VN market (restricted from COGS datamart)*/
      
            UNION ALL
            SELECT 'NA' as market,
                fisc_yr,
                fisc_yr_per,
                currency,
                material_code,
                company_code,
                sls_org,
                profit_center,
                cust_code,
                std_cogs,
                0 as cons_freegoods,
                0 as amt_gl_hd,
                'STD.COGS'::CHARACTER VARYING AS acct_hier_shrt_desc
            FROM itg_custgp_vn_cogs_fg

            UNION ALL

            SELECT 'NA' as market,
                fisc_yr,
                fisc_yr_per,
                currency,
                material_code,
                company_code,
                sls_org,
                profit_center,
                cust_code,
                0 as std_cogs,
                cons_freegoods,
                0 as amt_gl_hd,
                'CONS.FREE.GOODS'::CHARACTER VARYING AS acct_hier_shrt_desc
            FROM itg_custgp_vn_cogs_fg

                
        UNION ALL 
                        SELECT 'NA' as market,
                        fisc_yr,
                        fisc_yr_per,
                        currency,
                        matl_num as material_code,
                        co_cd as company_code,
                        sls_org,
                        prft_ctr,
                        cust_num as cust_code,
                        0 as std_Cogs,
                        0 as cons_freegoods,
                        amt_hiddisc as amt_gl_hd,
                        'HDPM'::CHARACTER VARYING AS acct_hier_shrt_desc
                FROM v_rpt_copa_gl_hid_disc
                WHERE amt_hiddisc <> 0 			
                ) cogs
            LEFT JOIN edw_company_dim company ON cogs.company_code::TEXT = company.co_cd::TEXT
            LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate
                ON cogs.currency::TEXT = exch_rate.from_crncy::TEXT
                AND cogs.fisc_yr_per = exch_rate.fisc_per
                AND CASE WHEN exch_rate.to_crncy::TEXT <> 'USD'::CHARACTER VARYING::TEXT 
                THEN exch_rate.to_crncy::TEXT = cogs.currency::TEXT
                ELSE exch_rate.to_crncy::TEXT = 'USD'::CHARACTER VARYING::TEXT END 
    ) fact

    LEFT JOIN edw_calendar_dim timedim ON fact.fisc_day::CHARACTER VARYING::TEXT = timedim.cal_day::CHARACTER VARYING::TEXT
                    
    GROUP BY fact.fisc_yr,
        fact.fisc_yr_per,
        CASE
            WHEN timedim.pstng_per >= 1 AND timedim.pstng_per <= 3 THEN 'Q1'::CHARACTER VARYING
            WHEN timedim.pstng_per >= 4 AND timedim.pstng_per <= 6 THEN 'Q2'::CHARACTER VARYING
            WHEN timedim.pstng_per >= 7 AND timedim.pstng_per <= 9 THEN 'Q3'::CHARACTER VARYING
            WHEN timedim.pstng_per >= 10 AND timedim.pstng_per <= 12 THEN 'Q4'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
        END  ,
        fact.fisc_day,
        fact.ctry_group  ,
        fact."cluster",
        fact.sls_org,
        ltrim(fact.prft_ctr,'0'), 
        fact.obj_crncy_co_obj,
        fact.from_crncy,
        fact.to_crncy,
        fact.matl_num,
        fact.cust_num,
            ciw_tt_tp_classification,
            fact.acct_hier_shrt_desc
)
select * from final