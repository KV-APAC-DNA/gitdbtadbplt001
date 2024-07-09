with v_intrm_curr_copa_plan as
(
    select * from {{ ref('ntaedw_integration__v_intrm_curr_copa_plan') }}
),
t1 as
(
    SELECT v_intrm_copa_plan.ctry_nm,
        v_intrm_copa_plan.cust_num,
        v_intrm_copa_plan.cust_nm,
        v_intrm_copa_plan.matl_num,
        v_intrm_copa_plan.matl_desc,
        v_intrm_copa_plan.mega_brnd_desc,
        v_intrm_copa_plan.brnd_desc,
        v_intrm_copa_plan.ean_num,
        v_intrm_copa_plan.prod_hier_l1,
        v_intrm_copa_plan.prod_hier_l2,
        v_intrm_copa_plan.prod_hier_l3,
        v_intrm_copa_plan.prod_hier_l4,
        v_intrm_copa_plan.prod_hier_l5,
        v_intrm_copa_plan.prod_hier_l6,
        v_intrm_copa_plan.prod_hier_l7,
        v_intrm_copa_plan.prod_hier_l8,
        v_intrm_copa_plan.prod_hier_l9,
        v_intrm_copa_plan.fisc_yr,
        v_intrm_copa_plan.fisc_yr_per,
        v_intrm_copa_plan.category,
        v_intrm_copa_plan.acct_hier_shrt_desc,
        v_intrm_copa_plan.plan_val,
        v_intrm_copa_plan.from_crncy,
        v_intrm_copa_plan.to_crncy,
        v_intrm_copa_plan.ex_rt
    FROM v_intrm_curr_copa_plan v_intrm_copa_plan
    WHERE ((v_intrm_copa_plan.category)::TEXT = ('RF'::CHARACTER VARYING)::TEXT)
),
final as
(
    SELECT DISTINCT y.ctry_nm,
    COALESCE(y.cust_num, '#'::CHARACTER VARYING) AS cust_num,
    COALESCE(y.cust_nm, 'Not Available'::CHARACTER VARYING) AS cust_nm,
    COALESCE(y.matl_num, '#'::CHARACTER VARYING) AS matl_num,
    COALESCE(y.matl_desc, 'Not Available'::CHARACTER VARYING) AS matl_desc,
    y.ean_num,
    COALESCE(y.mega_brnd_desc, 'Not Available'::CHARACTER VARYING) AS mega_brnd_desc,
    COALESCE(y.brnd_desc, 'Not Available'::CHARACTER VARYING) AS brnd_desc,
    COALESCE(y.prod_hier_l1, '#'::CHARACTER VARYING) AS prod_hier_l1,
    COALESCE(y.prod_hier_l2, '#'::CHARACTER VARYING) AS prod_hier_l2,
    COALESCE(y.prod_hier_l3, '#'::CHARACTER VARYING) AS prod_hier_l3,
    COALESCE(y.prod_hier_l4, '#'::CHARACTER VARYING) AS prod_hier_l4,
    COALESCE(y.prod_hier_l5, '#'::CHARACTER VARYING) AS prod_hier_l5,
    COALESCE(y.prod_hier_l6, '#'::CHARACTER VARYING) AS prod_hier_l6,
    COALESCE(y.prod_hier_l7, '#'::CHARACTER VARYING) AS prod_hier_l7,
    COALESCE(y.prod_hier_l8, '#'::CHARACTER VARYING) AS prod_hier_l8,
    COALESCE(y.prod_hier_l9, '#'::CHARACTER VARYING) AS prod_hier_l9,
    y.acct_hier_shrt_desc,
    y.fisc_yr_per,
    y.plan_val AS rf_total,
    y.from_crncy,
    y.to_crncy,
    y.ex_rt
    from t1 y
)
select * from final