with edw_copa_trans_fact as
(
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_company_dim as
(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
v_intrm_reg_crncy_exch_fiscper as
(
    select * from {{ ref('aspedw_integration__v_intrm_reg_crncy_exch_fiscper') }}
),
itg_account_ciw_tt_tp_mapping as
(
    select * from {{ source('aspitg_integration', 'itg_account_ciw_tt_tp_mapping') }}
),
edw_calendar_dim as
(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
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
        LTRIM(fact.prft_ctr,'0') AS prft_Ctr,
        fact.obj_crncy_co_obj,
        fact.from_crncy,
        fact.to_crncy,
        fact.matl_num,
        fact.cust_num AS cust_num,
        CASE
            WHEN fact.acct_hier_shrt_desc::TEXT = 'NTS'::CHARACTER VARYING::TEXT THEN
            CASE
                WHEN ciw_mapping.ciw_tt_tp_classification::TEXT = 'Trade Promotion'::CHARACTER VARYING::TEXT OR ciw_mapping.ciw_tt_tp_classification::TEXT = 'Trade Term'::CHARACTER VARYING::TEXT THEN ciw_mapping.ciw_tt_tp_classification
                ELSE fact.acct_hier_shrt_desc
            END 
            ELSE fact.acct_hier_shrt_desc
        END AS ciw_tt_tp_classification,
        fact.acct_hier_shrt_desc,
        SUM(fact.qty) AS qty,
        SUM(fact.amt_lcy) AS amt_lcy,
        SUM(fact.amt_usd) AS amt_usd
    FROM (SELECT copa.fisc_yr,
                copa.fisc_yr_per,
                TO_DATE((substring (copa.fisc_yr_per::CHARACTER VARYING::TEXT,6,8) || '01'::CHARACTER VARYING::TEXT) || substring (copa.fisc_yr_per::CHARACTER VARYING::TEXT,1,4),'MMDDYYYY'::CHARACTER VARYING::TEXT) AS fisc_day,
                company.ctry_group,
                company."cluster",
                copa.acct_num,
                copa.prft_ctr,
                copa.obj_crncy_co_obj,
                LTRIM(copa.matl_num::TEXT,'0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS matl_num,
                copa.co_cd,
                copa.sls_org,
                copa.dstr_chnl,
                copa.div,
                LTRIM(copa.cust_num::TEXT,'0'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS cust_num,
                exch_rate.from_crncy,
                exch_rate.to_crncy,
                copa.acct_hier_shrt_desc,
                CASE
                WHEN exch_rate.to_crncy::TEXT = 'USD'::CHARACTER VARYING::TEXT THEN SUM(copa.qty)
                ELSE 0::NUMERIC::NUMERIC(18,0)
                END AS qty,
                CASE
                WHEN exch_rate.to_crncy::TEXT = copa.obj_crncy_co_obj::TEXT THEN SUM(copa.amt_obj_crncy*exch_rate.ex_rt)
                ELSE 0::NUMERIC::NUMERIC(18,0)
                END AS amt_lcy,
                CASE
                WHEN exch_rate.to_crncy::TEXT = 'USD'::CHARACTER VARYING::TEXT THEN SUM(copa.amt_obj_crncy*exch_rate.ex_rt)
                ELSE 0::NUMERIC::NUMERIC(18,0)
                END AS amt_usd
        FROM edw_copa_trans_fact copa
            LEFT JOIN edw_company_dim company ON copa.co_cd::TEXT = company.co_cd::TEXT
            LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate
                ON copa.obj_crncy_co_obj::TEXT = exch_rate.from_crncy::TEXT
                AND copa.fisc_yr_per = exch_rate.fisc_per
                AND CASE WHEN exch_rate.to_crncy::TEXT <> 'USD'::CHARACTER VARYING::TEXT THEN exch_rate.to_crncy::TEXT = copa.obj_crncy_co_obj::TEXT ELSE exch_rate.to_crncy::TEXT = 'USD'::CHARACTER VARYING::TEXT END
        WHERE (copa.acct_hier_shrt_desc::TEXT = 'NTS'::CHARACTER VARYING::TEXT OR copa.acct_hier_shrt_desc::TEXT = 'GTS'::CHARACTER VARYING::TEXT OR copa.acct_hier_shrt_desc::TEXT = 'RTN'::CHARACTER VARYING::TEXT)
        AND   copa.fisc_yr_per::CHARACTER VARYING::TEXT >= ((((date_part(year,convert_timezone('UTC',current_timestamp())) - 2::DOUBLE PRECISION)::CHARACTER VARYING::TEXT || 0::CHARACTER VARYING::TEXT) || 0::CHARACTER VARYING::TEXT) || 1::CHARACTER VARYING::TEXT)
        AND   (company.ctry_group::TEXT = 'Malaysia'::CHARACTER VARYING::TEXT OR company.ctry_group::TEXT = 'Singapore'::CHARACTER VARYING::TEXT OR company.ctry_group::TEXT = 'Korea'::CHARACTER VARYING::TEXT OR company.ctry_group::TEXT = 'Thailand'::CHARACTER VARYING::TEXT OR company.ctry_group::TEXT = 'Hong Kong'::CHARACTER VARYING::TEXT OR company.ctry_group::TEXT = 'Taiwan'::CHARACTER VARYING::TEXT OR company.ctry_group = 'Philippines' OR company.ctry_group = 'Vietnam' OR company.ctry_group = 'India')
        GROUP BY company.ctry_group,
                company."cluster",
                copa.fisc_yr,
                copa.fisc_yr_per,
                copa.obj_crncy_co_obj,
                copa.acct_num,
                copa.prft_ctr,
                LTRIM(copa.matl_num::TEXT,'0'::CHARACTER VARYING::TEXT),
                copa.co_cd,
                copa.sls_org,
                copa.dstr_chnl,
                copa.div,
                LTRIM(copa.cust_num::TEXT,'0'::CHARACTER VARYING::TEXT),
                copa.acct_hier_shrt_desc,
                exch_rate.from_crncy,
                exch_rate.to_crncy) fact
    LEFT JOIN itg_account_ciw_tt_tp_mapping ciw_mapping ON 
    /*Exclude hidden discount G/L account# 402041 from CIW TT TP allocation, if hidden discounts are separately allocated for the market*/
    CASE WHEN co_cd in ('3820','4880','8266','4330') and ltrim(fact.acct_num,'0') = '402041' then '0' else 
    LTRIM (fact.acct_num::TEXT,0::CHARACTER VARYING::TEXT) end  = LTRIM (ciw_mapping.acct_num::TEXT,0::CHARACTER VARYING::TEXT)
    LEFT JOIN edw_calendar_dim timedim ON fact.fisc_day::CHARACTER VARYING::TEXT = timedim.cal_day::CHARACTER VARYING::TEXT

    GROUP BY fact.fisc_yr,
            fact.fisc_yr_per,
            CASE
            WHEN timedim.pstng_per >= 1 AND timedim.pstng_per <= 3 THEN 'Q1'::CHARACTER VARYING
            WHEN timedim.pstng_per >= 4 AND timedim.pstng_per <= 6 THEN 'Q2'::CHARACTER VARYING
            WHEN timedim.pstng_per >= 7 AND timedim.pstng_per <= 9 THEN 'Q3'::CHARACTER VARYING
            WHEN timedim.pstng_per >= 10 AND timedim.pstng_per <= 12 THEN 'Q4'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END,
            fact.fisc_day,
            fact.ctry_group,
            fact."cluster",
            fact.sls_org,
            LTRIM(fact.prft_ctr,'0'),
            fact.obj_crncy_co_obj,
            fact.from_crncy,
            fact.to_crncy,
            fact.matl_num,
            fact.cust_num,
            ciw_mapping.ciw_tt_tp_classification,
            fact.acct_hier_shrt_desc
)
select * from final