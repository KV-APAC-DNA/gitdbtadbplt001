{{
    config(
        materialized='view'
    )
}}

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
edw_invoice_fact as
(
    select * from {{ ref('aspedw_integration__edw_invoice_fact') }}
),
edw_calendar_dim as
(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
v_edw_customer_sales_dim as
(
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),
edw_gch_producthierarchy as
(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
edw_gch_customerhierarchy as
(
    select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),
final as
(
    SELECT 
    main.fisc_yr, 
    main.fisc_yr_per, 
    main.fisc_day, 
    main.ctry_nm, 
    main."cluster", 
    main.obj_crncy_co_obj, 
    mat.mega_brnd_desc AS "b1 mega-brand", 
    mat.brnd_desc AS "b2 brand", 
    mat.base_prod_desc AS "b3 base product", 
    mat.varnt_desc AS "b4 variant", 
    mat.put_up_desc AS "b5 put-up", 
    mat.prodh1_txtmd AS "prod h1 operating group", 
    mat.prodh2_txtmd AS "prod h2 franchise group", 
    mat.prodh3_txtmd AS "prod h3 franchise", 
    mat.prodh4_txtmd AS "prod h4 product franchise", 
    mat.prodh5_txtmd AS "prod h5 product major", 
    mat.prodh6_txtmd AS "prod h6 product minor", 
    cus_sales_extn."parent customer", 
    cus_sales_extn.banner, 
    cus_sales_extn."banner format", 
    cus_sales_extn.channel, 
    cus_sales_extn."go to model", 
    cus_sales_extn."sub channel", 
    cus_sales_extn.retail_env, 
    sum(main.nts_usd) AS nts_usd, 
    sum(main.nts_lcy) AS nts_lcy, 
    sum(main.gts_usd) AS gts_usd, 
    sum(main.gts_lcy) AS gts_lcy, 
    sum(main.eq_usd) AS eq_usd, 
    sum(main.eq_lcy) AS eq_lcy, 
    main.from_crncy, 
    main.to_crncy, 
    sum(main.nts_qty) AS nts_qty, 
    sum(main.gts_qty) AS gts_qty, 
    sum(main.eq_qty) AS eq_qty, 
    sum(main.ord_pc_qty) AS ord_pc_qty, 
    sum(main.unspp_qty) AS unspp_qty, 
    gph.gcph_franchise, 
    gph.gcph_brand, 
    gph.gcph_subbrand, 
    gph.gcph_variant, 
    gph.put_up_description, 
    gph.gcph_needstate, 
    gph.gcph_category, 
    gph.gcph_subcategory, 
    gph.gcph_segment, 
    gph.gcph_subsegment, 
    gch.gcch_total_enterprise, 
    gch.gcch_retail_banner, 
    gch.primary_format AS gcch_primay_format, 
    gch.distributor_attribute AS gcch_distributor_attribute, 
    main.cust_num, 
    (
        (
        (gph.gcph_segment):: text + ('/' :: character varying):: text
        ) + (gph.gcph_subsegment):: text
    ) AS segment_subsegment, 
    (
        (
        (gph.gcph_subbrand):: text + ('/' :: character varying):: text
        ) + (gph.gcph_variant):: text
    ) AS subbrand_variant 
    FROM 
    (
        (
        (
            (
            (
                (
                SELECT 
                    copa.fisc_yr, 
                    copa.fisc_yr_per, 
                    to_date(
                    (
                        (
                        substring(
                            (
                            (copa.fisc_yr_per):: character varying
                            ):: text, 
                            6, 
                            8
                        ) || ('01' :: character varying):: text
                        ) || substring(
                        (
                            (copa.fisc_yr_per):: character varying
                        ):: text, 
                        1, 
                        4
                        )
                    ), 
                    ('MMDDYYYY' :: character varying):: text
                    ) AS fisc_day, 
                    CASE WHEN (
                    (
                        (
                        (
                            (
                            (
                                ltrim(
                                (copa.cust_num):: text, 
                                (
                                    (0):: character varying
                                ):: text
                                ) = ('134559' :: character varying):: text
                            ) 
                            OR (
                                ltrim(
                                (copa.cust_num):: text, 
                                (
                                    (0):: character varying
                                ):: text
                                ) = ('134106' :: character varying):: text
                            )
                            ) 
                            OR (
                            ltrim(
                                (copa.cust_num):: text, 
                                (
                                (0):: character varying
                                ):: text
                            ) = ('134258' :: character varying):: text
                            )
                        ) 
                        OR (
                            ltrim(
                            (copa.cust_num):: text, 
                            (
                                (0):: character varying
                            ):: text
                            ) = ('134855' :: character varying):: text
                        )
                        ) 
                        AND (
                        ltrim(
                            (copa.acct_num):: text, 
                            (
                            (0):: character varying
                            ):: text
                        ) <> ('403185' :: character varying):: text
                        )
                    ) 
                    AND (copa.fisc_yr = 2018)
                    ) THEN 'China Selfcare' :: character varying ELSE cmp.ctry_group END AS ctry_nm, 
                    CASE WHEN (
                    (
                        (
                        (
                            (
                            (
                                ltrim(
                                (copa.cust_num):: text, 
                                (
                                    (0):: character varying
                                ):: text
                                ) = ('134559' :: character varying):: text
                            ) 
                            OR (
                                ltrim(
                                (copa.cust_num):: text, 
                                (
                                    (0):: character varying
                                ):: text
                                ) = ('134106' :: character varying):: text
                            )
                            ) 
                            OR (
                            ltrim(
                                (copa.cust_num):: text, 
                                (
                                (0):: character varying
                                ):: text
                            ) = ('134258' :: character varying):: text
                            )
                        ) 
                        OR (
                            ltrim(
                            (copa.cust_num):: text, 
                            (
                                (0):: character varying
                            ):: text
                            ) = ('134855' :: character varying):: text
                        )
                        ) 
                        AND (
                        ltrim(
                            (copa.acct_num):: text, 
                            (
                            (0):: character varying
                            ):: text
                        ) <> ('403185' :: character varying):: text
                        )
                    ) 
                    AND (copa.fisc_yr = 2018)
                    ) THEN 'China' :: character varying ELSE cmp."cluster" END AS "cluster", 
                    CASE WHEN (
                    (cmp.ctry_group):: text = ('India' :: character varying):: text
                    ) THEN 'INR' :: character varying WHEN (
                    (cmp.ctry_group):: text = (
                        'Philippines' :: character varying
                    ):: text
                    ) THEN 'PHP' :: character varying WHEN (
                    (
                        (cmp.ctry_group):: text = (
                        'China Selfcare' :: character varying
                        ):: text
                    ) 
                    OR (
                        (cmp.ctry_group):: text = (
                        'China Personal Care' :: character varying
                        ):: text
                    )
                    ) THEN 'RMB' :: character varying ELSE copa.obj_crncy_co_obj END AS obj_crncy_co_obj, 
                    copa.matl_num, 
                    copa.co_cd, 
                    copa.sls_org, 
                    copa.dstr_chnl, 
                    copa.div, 
                    copa.cust_num, 
                    CASE WHEN (
                    (
                        (copa.acct_hier_shrt_desc):: text = ('NTS' :: character varying):: text
                    ) 
                    AND (
                        (exch_rate.to_crncy):: text = ('USD' :: character varying):: text
                    )
                    ) THEN sum(
                    (
                        copa.amt_obj_crncy * exch_rate.ex_rt
                    )
                    ) ELSE (
                    (0):: numeric
                    ):: numeric(18, 0) END AS nts_usd, 
                    CASE WHEN (
                    (
                        (copa.acct_hier_shrt_desc):: text = ('NTS' :: character varying):: text
                    ) 
                    AND (
                        (exch_rate.to_crncy):: text = (
                        CASE WHEN (
                            (
                            (cmp.ctry_group):: text = ('India' :: character varying):: text
                            ) 
                            OR (
                            (cmp.ctry_group IS NULL) 
                            AND ('India' IS NULL)
                            )
                        ) THEN 'INR' :: character varying WHEN (
                            (
                            (cmp.ctry_group):: text = (
                                'Philippines' :: character varying
                            ):: text
                            ) 
                            OR (
                            (cmp.ctry_group IS NULL) 
                            AND ('Philippines' IS NULL)
                            )
                        ) THEN 'PHP' :: character varying WHEN (
                            (
                            (cmp.ctry_group):: text = (
                                'China Selfcare' :: character varying
                            ):: text
                            ) 
                            OR (
                            (cmp.ctry_group IS NULL) 
                            AND ('China Selfcare' IS NULL)
                            )
                        ) THEN 'RMB' :: character varying WHEN (
                            (
                            (cmp.ctry_group):: text = (
                                'China Personal Care' :: character varying
                            ):: text
                            ) 
                            OR (
                            (cmp.ctry_group IS NULL) 
                            AND ('China Personal Care' IS NULL)
                            )
                        ) THEN 'RMB' :: character varying ELSE copa.obj_crncy_co_obj END
                        ):: text
                    )
                    ) THEN sum(
                    (
                        copa.amt_obj_crncy * exch_rate.ex_rt
                    )
                    ) ELSE (
                    (0):: numeric
                    ):: numeric(18, 0) END AS nts_lcy, 
                    CASE WHEN (
                    (
                        (copa.acct_hier_shrt_desc):: text = ('GTS' :: character varying):: text
                    ) 
                    AND (
                        (exch_rate.to_crncy):: text = ('USD' :: character varying):: text
                    )
                    ) THEN sum(
                    (
                        copa.amt_obj_crncy * exch_rate.ex_rt
                    )
                    ) ELSE (
                    (0):: numeric
                    ):: numeric(18, 0) END AS gts_usd, 
                    CASE WHEN (
                    (
                        (copa.acct_hier_shrt_desc):: text = ('GTS' :: character varying):: text
                    ) 
                    AND (
                        (exch_rate.to_crncy):: text = (
                        CASE WHEN (
                            (
                            (cmp.ctry_group):: text = ('India' :: character varying):: text
                            ) 
                            OR (
                            (cmp.ctry_group IS NULL) 
                            AND ('India' IS NULL)
                            )
                        ) THEN 'INR' :: character varying WHEN (
                            (
                            (cmp.ctry_group):: text = (
                                'Philippines' :: character varying
                            ):: text
                            ) 
                            OR (
                            (cmp.ctry_group IS NULL) 
                            AND ('Philippines' IS NULL)
                            )
                        ) THEN 'PHP' :: character varying WHEN (
                            (
                            (cmp.ctry_group):: text = (
                                'China Selfcare' :: character varying
                            ):: text
                            ) 
                            OR (
                            (cmp.ctry_group IS NULL) 
                            AND ('China Selfcare' IS NULL)
                            )
                        ) THEN 'RMB' :: character varying WHEN (
                            (
                            (cmp.ctry_group):: text = (
                                'China Personal Care' :: character varying
                            ):: text
                            ) 
                            OR (
                            (cmp.ctry_group IS NULL) 
                            AND ('China Personal Care' IS NULL)
                            )
                        ) THEN 'RMB' :: character varying ELSE copa.obj_crncy_co_obj END
                        ):: text
                    )
                    ) THEN sum(
                    (
                        copa.amt_obj_crncy * exch_rate.ex_rt
                    )
                    ) ELSE (
                    (0):: numeric
                    ):: numeric(18, 0) END AS gts_lcy, 
                    CASE WHEN (
                    (
                        (copa.acct_hier_shrt_desc):: text = ('EQ' :: character varying):: text
                    ) 
                    AND (
                        (exch_rate.to_crncy):: text = ('USD' :: character varying):: text
                    )
                    ) THEN sum(
                    (
                        copa.amt_obj_crncy * exch_rate.ex_rt
                    )
                    ) ELSE (
                    (0):: numeric
                    ):: numeric(18, 0) END AS eq_usd, 
                    CASE WHEN (
                    (
                        (copa.acct_hier_shrt_desc):: text = ('EQ' :: character varying):: text
                    ) 
                    AND (
                        (exch_rate.to_crncy):: text = (
                        CASE WHEN (
                            (
                            (cmp.ctry_group):: text = ('India' :: character varying):: text
                            ) 
                            OR (
                            (cmp.ctry_group IS NULL) 
                            AND ('India' IS NULL)
                            )
                        ) THEN 'INR' :: character varying WHEN (
                            (
                            (cmp.ctry_group):: text = (
                                'Philippines' :: character varying
                            ):: text
                            ) 
                            OR (
                            (cmp.ctry_group IS NULL) 
                            AND ('Philippines' IS NULL)
                            )
                        ) THEN 'PHP' :: character varying WHEN (
                            (
                            (cmp.ctry_group):: text = (
                                'China Selfcare' :: character varying
                            ):: text
                            ) 
                            OR (
                            (cmp.ctry_group IS NULL) 
                            AND ('China Selfcare' IS NULL)
                            )
                        ) THEN 'RMB' :: character varying WHEN (
                            (
                            (cmp.ctry_group):: text = (
                                'China Personal Care' :: character varying
                            ):: text
                            ) 
                            OR (
                            (cmp.ctry_group IS NULL) 
                            AND ('China Personal Care' IS NULL)
                            )
                        ) THEN 'RMB' :: character varying ELSE copa.obj_crncy_co_obj END
                        ):: text
                    )
                    ) THEN sum(
                    (
                        copa.amt_obj_crncy * exch_rate.ex_rt
                    )
                    ) ELSE (
                    (0):: numeric
                    ):: numeric(18, 0) END AS eq_lcy, 
                    CASE WHEN (
                    (cmp.ctry_group):: text = ('India' :: character varying):: text
                    ) THEN 'INR' :: character varying WHEN (
                    (cmp.ctry_group):: text = (
                        'Philippines' :: character varying
                    ):: text
                    ) THEN 'PHP' :: character varying WHEN (
                    (
                        (cmp.ctry_group):: text = (
                        'China Selfcare' :: character varying
                        ):: text
                    ) 
                    OR (
                        (cmp.ctry_group):: text = (
                        'China Personal Care' :: character varying
                        ):: text
                    )
                    ) THEN 'RMB' :: character varying ELSE exch_rate.from_crncy END AS from_crncy, 
                    exch_rate.to_crncy, 
                    CASE WHEN (
                    (
                        (copa.acct_hier_shrt_desc):: text = ('NTS' :: character varying):: text
                    ) 
                    AND (
                        (exch_rate.to_crncy):: text = ('USD' :: character varying):: text
                    )
                    ) THEN sum(copa.qty) ELSE (
                    (0):: numeric
                    ):: numeric(18, 0) END AS nts_qty, 
                    CASE WHEN (
                    (
                        (copa.acct_hier_shrt_desc):: text = ('GTS' :: character varying):: text
                    ) 
                    AND (
                        (exch_rate.to_crncy):: text = ('USD' :: character varying):: text
                    )
                    ) THEN sum(copa.qty) ELSE (
                    (0):: numeric
                    ):: numeric(18, 0) END AS gts_qty, 
                    CASE WHEN (
                    (
                        (copa.acct_hier_shrt_desc):: text = ('EQ' :: character varying):: text
                    ) 
                    AND (
                        (exch_rate.to_crncy):: text = ('USD' :: character varying):: text
                    )
                    ) THEN sum(copa.qty) ELSE (
                    (0):: numeric
                    ):: numeric(18, 0) END AS eq_qty, 
                    0 AS ord_pc_qty, 
                    0 AS unspp_qty 
                FROM 
                    (
                    (
                        edw_copa_trans_fact copa 
                        LEFT JOIN edw_company_dim cmp ON (
                        (
                            (copa.co_cd):: text = (cmp.co_cd):: text
                        )
                        )
                    ) 
                    LEFT JOIN v_intrm_reg_crncy_exch_fiscper exch_rate ON (
                        (
                        (
                            (
                            (copa.obj_crncy_co_obj):: text = (exch_rate.from_crncy):: text
                            ) 
                            AND (
                            copa.fisc_yr_per = exch_rate.fisc_per
                            )
                        ) 
                        AND CASE WHEN (
                            (exch_rate.to_crncy):: text <> ('USD' :: character varying):: text
                        ) THEN (
                            (exch_rate.to_crncy):: text = (
                            CASE WHEN (
                                (
                                (cmp.ctry_group):: text = ('India' :: character varying):: text
                                ) 
                                OR (
                                (cmp.ctry_group IS NULL) 
                                AND ('India' IS NULL)
                                )
                            ) THEN 'INR' :: character varying WHEN (
                                (
                                (cmp.ctry_group):: text = (
                                    'Philippines' :: character varying
                                ):: text
                                ) 
                                OR (
                                (cmp.ctry_group IS NULL) 
                                AND ('Philippines' IS NULL)
                                )
                            ) THEN 'PHP' :: character varying WHEN (
                                (
                                (cmp.ctry_group):: text = (
                                    'China Selfcare' :: character varying
                                ):: text
                                ) 
                                OR (
                                (cmp.ctry_group IS NULL) 
                                AND ('China Selfcare' IS NULL)
                                )
                            ) THEN 'RMB' :: character varying WHEN (
                                (
                                (cmp.ctry_group):: text = (
                                    'China Personal Care' :: character varying
                                ):: text
                                ) 
                                OR (
                                (cmp.ctry_group IS NULL) 
                                AND ('China Personal Care' IS NULL)
                                )
                            ) THEN 'RMB' :: character varying ELSE copa.obj_crncy_co_obj END
                            ):: text
                        ) ELSE (
                            (exch_rate.to_crncy):: text = ('USD' :: character varying):: text
                        ) END
                        )
                    )
                    ) 
                WHERE 
                    (
                    (
                        (
                        (
                            (copa.acct_hier_shrt_desc):: text = ('GTS' :: character varying):: text
                        ) 
                        OR (
                            (copa.acct_hier_shrt_desc):: text = ('NTS' :: character varying):: text
                        )
                        ) 
                        OR (
                        (copa.acct_hier_shrt_desc):: text = ('EQ' :: character varying):: text
                        )
                    ) 
                    AND (
                        (
                        (copa.fisc_yr_per):: character varying
                        ):: text >= (
                        (
                            (
                            (
                                (
                                (
                                    date_part(
                                    year, 
                                    convert_timezone('UTC',current_timestamp()):: date
                                    ) -2
                                )
                                ):: character varying
                            ):: text || (
                                (0):: character varying
                            ):: text
                            ) || (
                            (0):: character varying
                            ):: text
                        ) || (
                            (1):: character varying
                        ):: text
                        )
                    )
                    ) 
                GROUP BY 
                    copa.fisc_yr, 
                    copa.fisc_yr_per, 
                    copa.obj_crncy_co_obj, 
                    copa.matl_num, 
                    copa.co_cd, 
                    copa.sls_org, 
                    copa.dstr_chnl, 
                    copa.div, 
                    copa.cust_num, 
                    copa.acct_num, 
                    copa.acct_hier_shrt_desc, 
                    exch_rate.from_crncy, 
                    exch_rate.to_crncy, 
                    cmp.ctry_group, 
                    cmp."cluster" 
                UNION ALL 
                SELECT 
                    (
                    substring(
                        (
                        (cal.fisc_per):: character varying
                        ):: text, 
                        1, 
                        4
                    )
                    ):: integer AS fisc_yr, 
                    cal.fisc_per AS fisc_yr_per, 
                    to_date(
                    (
                        (
                        substring(
                            (
                            (cal.fisc_per):: character varying
                            ):: text, 
                            6, 
                            8
                        ) || ('01' :: character varying):: text
                        ) || substring(
                        (
                            (cal.fisc_per):: character varying
                        ):: text, 
                        1, 
                        4
                        )
                    ), 
                    ('MMDDYYYY' :: character varying):: text
                    ) AS fisc_day, 
                    cmp.ctry_group AS ctry_nm, 
                    cmp."cluster", 
                    invc.curr_key AS obj_crncy_co_obj, 
                    invc.matl_num, 
                    invc.co_cd, 
                    invc.sls_org, 
                    invc.dstr_chnl, 
                    invc.div, 
                    invc.cust_num, 
                    sum(0) AS nts_usd, 
                    sum(0) AS nts_lcy, 
                    sum(0) AS gts_usd, 
                    sum(0) AS gts_lcy, 
                    sum(0) AS eq_usd, 
                    sum(0) AS eq_lcy, 
                    'N/A' :: character varying AS from_crncy, 
                    'N/A' :: character varying AS to_crncy, 
                    sum(0) AS nts_qty, 
                    sum(0) AS gts_qty, 
                    sum(0) AS eq_qty, 
                    sum(invc.ord_pc_qty) AS ord_pc_qty, 
                    sum(invc.unspp_qty) AS unspp_qty 
                FROM 
                    (
                    (
                        edw_invoice_fact invc 
                        LEFT JOIN edw_company_dim cmp ON (
                        (
                            (invc.co_cd):: text = (cmp.co_cd):: text
                        )
                        )
                    ) 
                    LEFT JOIN edw_calendar_dim cal ON (
                        (invc.rqst_delv_dt = cal.cal_day)
                    )
                    ) 
                WHERE 
                    (
                    (
                        (cal.fisc_per):: character varying
                    ):: text >= (
                        (
                        (
                            (
                            (
                                (
                                date_part(
                                    year, 
                                    convert_timezone('UTC',current_timestamp()):: date
                                ) -2
                                )
                            ):: character varying
                            ):: text || (
                            (0):: character varying
                            ):: text
                        ) || (
                            (0):: character varying
                        ):: text
                        ) || (
                        (1):: character varying
                        ):: text
                    )
                    ) 
                GROUP BY 
                    cal.fisc_per, 
                    invc.curr_key, 
                    invc.matl_num, 
                    invc.co_cd, 
                    invc.sls_org, 
                    invc.dstr_chnl, 
                    invc.div, 
                    invc.cust_num, 
                    cmp.ctry_group, 
                    cmp."cluster"
                ) main 
                LEFT JOIN edw_material_dim mat ON (
                (
                    (main.matl_num):: text = (mat.matl_num):: text
                )
                )
            ) 
            JOIN edw_company_dim company ON (
                (
                (main.co_cd):: text = (company.co_cd):: text
                )
            )
            ) 
            LEFT JOIN v_edw_customer_sales_dim cus_sales_extn ON (
            (
                (
                (
                    (
                    (main.sls_org):: text = (cus_sales_extn.sls_org):: text
                    ) 
                    AND (
                    (main.dstr_chnl):: text = (cus_sales_extn.dstr_chnl):: text
                    )
                ) 
                AND (
                    (main.div):: text = (cus_sales_extn.div):: text
                )
                ) 
                AND (
                (main.cust_num):: text = (cus_sales_extn.cust_num):: text
                )
            )
            )
        ) 
        LEFT JOIN edw_gch_producthierarchy gph ON (
            (
            (main.matl_num):: text = (gph.materialnumber):: text
            )
        )
        ) 
        LEFT JOIN edw_gch_customerhierarchy gch ON (
        (
            (main.cust_num):: text = (gch.customer):: text
        )
        )
    ) 
    GROUP BY 
    main.fisc_yr, 
    main.fisc_yr_per, 
    main.fisc_day, 
    main.ctry_nm, 
    main."cluster", 
    main.obj_crncy_co_obj, 
    mat.mega_brnd_desc, 
    mat.brnd_desc, 
    mat.base_prod_desc, 
    mat.varnt_desc, 
    mat.put_up_desc, 
    mat.prodh1_txtmd, 
    mat.prodh2_txtmd, 
    mat.prodh3_txtmd, 
    mat.prodh4_txtmd, 
    mat.prodh5_txtmd, 
    mat.prodh6_txtmd, 
    cus_sales_extn."parent customer", 
    cus_sales_extn.banner, 
    cus_sales_extn."banner format", 
    cus_sales_extn.channel, 
    cus_sales_extn."go to model", 
    cus_sales_extn."sub channel", 
    cus_sales_extn.retail_env, 
    main.from_crncy, 
    main.to_crncy, 
    gph.gcph_franchise, 
    gph.gcph_brand, 
    gph.gcph_subbrand, 
    gph.gcph_variant, 
    gph.put_up_description, 
    gph.gcph_needstate, 
    gph.gcph_category, 
    gph.gcph_subcategory, 
    gph.gcph_segment, 
    gph.gcph_subsegment, 
    gch.gcch_total_enterprise, 
    gch.gcch_retail_banner, 
    gch.primary_format, 
    gch.distributor_attribute, 
    main.cust_num
)
select * from final