{{
    config(
        materialized='view'
    )
}}

with edw_copa_trans_fact as
(
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
itg_custgp_cogs_fg_control as
(
    select * from {{ source('aspitg_integration', 'itg_custgp_cogs_fg_control') }}
),
final as
(
    SELECT copa.co_cd,
        copa.sls_org,
        copa.fisc_yr,
        copa.fisc_yr_per,
        copa.prft_ctr,
        LTRIM((copa.acct_num)::TEXT,('0'::CHARACTER VARYING)::TEXT) AS acct_num,
        copa.obj_crncy_co_obj AS currency,
        LTRIM((copa.cust_num)::TEXT,('0'::CHARACTER VARYING)::TEXT) AS cust_num,
        LTRIM((copa.matl_num)::TEXT,('0'::CHARACTER VARYING)::TEXT) AS matl_num,
        SUM(copa.amt_obj_crncy) AS amt_hiddisc
    FROM edw_copa_trans_fact copa,
        itg_custgp_cogs_fg_control ctrl
    WHERE (((((((ctrl.acct_hier_shrt_desc)::TEXT = ('HDPM'::CHARACTER VARYING)::TEXT) AND ((copa.co_cd)::TEXT = ((ctrl.co_cd)::CHARACTER VARYING)::TEXT)) AND ((copa.acct_hier_shrt_desc)::TEXT = (ctrl.acct_hier_shrt_desc)::TEXT)) AND (LTRIM((copa.acct_num)::TEXT,('0'::CHARACTER VARYING)::TEXT) = ((ctrl.gl_acct_num)::CHARACTER VARYING)::TEXT)) AND ((((copa.fisc_yr_per)::CHARACTER VARYING)::TEXT >= (ctrl.valid_from)::TEXT) AND (((copa.fisc_yr_per)::CHARACTER VARYING)::TEXT <= (ctrl.valid_to)::TEXT))) AND ((ctrl.active)::TEXT = ('Y'::CHARACTER VARYING)::TEXT))
    GROUP BY copa.co_cd,
            copa.sls_org,
            copa.fisc_yr,
            copa.fisc_yr_per,
            LTRIM((copa.acct_num)::TEXT,('0'::CHARACTER VARYING)::TEXT),
            copa.obj_crncy_co_obj,
            copa.prft_ctr,
            LTRIM((copa.cust_num)::TEXT,('0'::CHARACTER VARYING)::TEXT),
            LTRIM((copa.matl_num)::TEXT,('0'::CHARACTER VARYING)::TEXT)
)
select * from final