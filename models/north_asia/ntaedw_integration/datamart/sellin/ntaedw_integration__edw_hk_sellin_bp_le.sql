with edw_copa_trans_fact as
(
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_copa_plan_fact as
(
    select * from {{ ref('aspedw_integration__edw_copa_plan_fact') }}
),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_calendar_dim as
(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
itg_query_parameters as
(
    select * from {{ source('ntaitg_integration','itg_query_parameters') }}
),
v_edw_customer_sales_dim as
(
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),
itg_mds_hk_le_targets as
(
    select * from {{ source('ntaitg_integration', 'itg_mds_hk_le_targets') }}
),
v_intrm_reg_crncy_exch_fiscper as
(
    select * from {{ ref('aspedw_integration__v_intrm_reg_crncy_exch_fiscper') }}
),
edw_gch_producthierarchy as
(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
tp_acc_date as 
(
    select fisc_yr_per,
        "banner format",
        min(caln_day) tp_acc_start
    from (
            SELECT (
                    left(fisc_yr_per, 4)::text || right(fisc_yr_per, 2)::text
                )::character varying AS fisc_yr_per,
                "banner format",
                caln_day,
                SUM(
                    COALESCE(
                        edw_copa_trans_fact.amt_obj_crncy::DOUBLE precision,
                        0::DOUBLE precision
                    )
                ) AS amt_obj_crncy
            FROM edw_copa_trans_fact
                LEFT JOIN edw_material_dim mat ON ltrim(
                    edw_copa_trans_fact.matl_num::character varying::text,
                    0
                ) = ltrim(mat.matl_num::text, '0'::character varying::text)
                LEFT JOIN v_edw_customer_sales_dim cus_sales_extn ON edw_copa_trans_fact.sls_org::text = cus_sales_extn.sls_org::text
                AND edw_copa_trans_fact.dstr_chnl::text = cus_sales_extn.dstr_chnl::text
                AND edw_copa_trans_fact.div::text = cus_sales_extn.div::text
                AND ltrim(
                    edw_copa_trans_fact.cust_num::character varying::text,
                    0
                ) = ltrim(
                    cus_sales_extn.cust_num::text,
                    '0'::character varying::text
                )
            WHERE edw_copa_trans_fact.sls_org::TEXT IN (
                    SELECT parameter_value
                    FROM itg_query_parameters
                    WHERE country_code = 'HK'
                        AND parameter_name = 'copa'
                        AND parameter_type = 'sls_org'
                )
                AND acct_hier_shrt_desc::TEXT = 'NTS'::CHARACTER VARYING::TEXT
                AND LTRIM(acct_num::TEXT, '0'::CHARACTER VARYING::TEXT) IN (
                    SELECT parameter_value
                    FROM itg_query_parameters
                    WHERE country_code = 'HK'
                        AND parameter_name = 'TP'
                        AND parameter_type = 'GL_Account'
                )
            GROUP BY 1,
                2,
                3
            having sum(amt_obj_crncy) <> 0
        )
    group by fisc_yr_per,
        "banner format"
),
time_gone as 
(
    select fisc_per,
        fisc_start,
        fisc_end,
        today,
        case
            when to_date(current_timestamp) >= fisc_start
            and to_date(current_timestamp) <= fisc_end then (
                to_date(current_timestamp) - fisc_start + 1
            ) /(fisc_end - fisc_start + 1)::float
            else 1::float
        end time_gone
    from (
            SELECT (
                    LEFT (fisc_per, 4)::TEXT || RIGHT (fisc_per, 2)::TEXT
                )::CHARACTER VARYING as fisc_per,
                MIN(cal_day) fisc_start,
                MAX(cal_day) fisc_end,
                MAX(to_date(current_timestamp)) today
            FROM edw_calendar_dim
            GROUP BY 1
        )
),
actual as
(   
    select 
        'Actual'::character varying AS subsource_type,
        fisc_yr_per,
        mega_brnd_desc,
        brnd_desc,
        "go to model",
        "banner format",
        "sub channel",
        "parent customer",
        usd_rt,
        lcl_rt,
        tp_acc_start,
        tp_perc,
        time_gone,
        sum(PREWW7_ACT) preww7_act,
        0.0::double precision AS nts_act,
        sum(tp_act) * -1 tp_act,
        sum(tp_calc) as tp_act_calc,
        0.0::double precision AS nts_bp,
        0.0::double precision AS tp_bp,
        0.0::double precision AS nts_le,
        0.0::double precision AS tp_le
    from
        (
            select 
                act.fisc_yr_per,
                act.mega_brnd_desc,
                act.brnd_desc,
                act."banner format",
                act."go to model",
                act."sub channel",
                act."parent customer",
                usd_rt,
                lcl_rt,
                act.caln_day,
                tp_perc,
                tp_acc_start,
                act.preww7_act,
                act.tp_act,
                case
                    when (
                        caln_day < tp_acc_start
                        or tp_acc_start is null
                    ) then act.preww7_act * tp_perc
                    else 0
                end tp_calc
            from 
                (
                    SELECT 
                        (
                            left(copa.fisc_yr_per, 4)::text || right(copa.fisc_yr_per, 2)::text
                        )::character varying AS fisc_yr_per,
                        caln_day,
                        mat.mega_brnd_desc,
                        gchph.gcph_brand brnd_desc,
                        cus_sales_extn."banner format",
                        cus_sales_extn."go to model",
                        cus_sales_extn."sub channel",
                        cus_sales_extn."parent customer",
                        sum(gts_act) + sum(preww_d) as preww7_act,
                        sum(tp_act) tp_act
                    from 
                        (
                            select 
                                fisc_yr_per,
                                matl_num,
                                sls_org,
                                div,
                                dstr_chnl,
                                cust_num,
                                caln_day,
                                sum(
                                    CASE
                                        WHEN copa.acct_hier_shrt_desc::text = 'GTS'::character varying::text THEN COALESCE(copa.amt_obj_crncy, 0::double precision)
                                        ELSE 0::numeric::numeric(18, 0)::double precision
                                    END
                                ) AS gts_act,
                                sum(
                                    CASE
                                        WHEN copa.acct_hier_shrt_desc::text = 'NTS'::character varying::text
                                        AND LTRIM(acct_num::TEXT, '0'::CHARACTER VARYING::TEXT) IN (
                                            SELECT parameter_value
                                            FROM itg_query_parameters
                                            WHERE country_code = 'HK'
                                                AND parameter_name = 'PreWW'
                                                AND parameter_type = 'GL_Account'
                                        ) THEN COALESCE(copa.amt_obj_crncy, 0::double precision)
                                        ELSE 0::numeric::numeric(18, 0)::double precision
                                    END
                                ) preww_d,
                                sum(
                                    CASE
                                        WHEN copa.acct_hier_shrt_desc::text = 'NTS'::character varying::text
                                        AND LTRIM(acct_num::TEXT, '0'::CHARACTER VARYING::TEXT) IN (
                                            SELECT parameter_value
                                            FROM itg_query_parameters
                                            WHERE country_code = 'HK'
                                                AND parameter_name = 'TP'
                                                AND parameter_type = 'GL_Account'
                                        ) THEN COALESCE(copa.amt_obj_crncy, 0::double precision)
                                        ELSE 0::numeric::numeric(18, 0)::double precision
                                    END
                                ) AS tp_act
                            FROM edw_copa_trans_fact copa
                            WHERE copa.sls_org::TEXT IN (
                                    SELECT parameter_value
                                    FROM itg_query_parameters
                                    WHERE country_code = 'HK'
                                        AND parameter_name = 'copa'
                                        AND parameter_type = 'sls_org'
                                )
                            group by fisc_yr_per,
                                matl_num,
                                sls_org,
                                div,
                                dstr_chnl,
                                cust_num,
                                caln_day
                            having (
                                    sum(
                                        CASE
                                            WHEN copa.acct_hier_shrt_desc::text = 'GTS'::character varying::text THEN COALESCE(copa.amt_obj_crncy, 0::double precision)
                                            ELSE 0::numeric::numeric(18, 0)::double precision
                                        END
                                    ) + sum(
                                        CASE
                                            WHEN copa.acct_hier_shrt_desc::text = 'NTS'::character varying::text
                                            AND LTRIM(acct_num::TEXT, '0'::CHARACTER VARYING::TEXT) IN (
                                                SELECT parameter_value
                                                FROM itg_query_parameters
                                                WHERE country_code = 'HK'
                                                    AND parameter_name = 'PreWW'
                                                    AND parameter_type = 'GL_Account'
                                            ) THEN COALESCE(copa.amt_obj_crncy, 0::double precision)
                                            ELSE 0::numeric::numeric(18, 0)::double precision
                                        END
                                    ) + sum(
                                        CASE
                                            WHEN copa.acct_hier_shrt_desc::text = 'NTS'::character varying::text
                                            AND LTRIM(acct_num::TEXT, '0'::CHARACTER VARYING::TEXT) IN (
                                                SELECT parameter_value
                                                FROM itg_query_parameters
                                                WHERE country_code = 'HK'
                                                    AND parameter_name = 'TP'
                                                    AND parameter_type = 'GL_Account'
                                            ) THEN COALESCE(copa.amt_obj_crncy, 0::double precision)
                                            ELSE 0::numeric::numeric(18, 0)::double precision
                                        END
                                    ) <> 0
                                )
                        ) copa
                        LEFT JOIN edw_material_dim mat 
                        ON ltrim(copa.matl_num::character varying::text, 0) = ltrim(mat.matl_num::text, '0'::character varying::text)
                        LEFT JOIN (
                            select distinct materialnumber,
                                gcph_brand
                            from edw_gch_producthierarchy
                        ) gchph on ltrim(mat.matl_num::text, '0'::character varying::text) = ltrim(
                            gchph.materialnumber::text,
                            '0'::character varying::text
                        )
                        LEFT JOIN v_edw_customer_sales_dim cus_sales_extn ON copa.sls_org::text = cus_sales_extn.sls_org::text
                        AND copa.dstr_chnl::text = cus_sales_extn.dstr_chnl::text
                        AND copa.div::text = cus_sales_extn.div::text
                        AND ltrim(copa.cust_num::character varying::text, 0) = ltrim(
                            cus_sales_extn.cust_num::text,
                            '0'::character varying::text
                        )
                    group by 1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8
                ) act
                left outer join -- TP %
                (
                    select fisc_yr_per,
                        brand_nm,
                        cust_cd,
                        sum(tp_le) /(sum(tp_le) + sum(nts_le)) tp_perc
                    from (
                            select (t.year::text || t.month_no::text)::character varying AS fisc_yr_per,
                                cust_cd,
                                brand_nm,
                                sum(
                                    CASE
                                        WHEN t.metric_cd::text = 'NTS'::character varying::text THEN COALESCE(
                                            t.target_value::double precision,
                                            0::double precision
                                        )
                                        ELSE NULL::numeric::numeric(18, 0)::double precision
                                    END
                                ) AS nts_le,
                                sum(
                                    CASE
                                        WHEN t.metric_cd::text = 'TP'::character varying::text THEN COALESCE(
                                            t.target_value::double precision,
                                            0::double precision
                                        )
                                        ELSE NULL::numeric::numeric(18, 0)::double precision
                                    END
                                ) AS tp_le
                            from itg_mds_hk_le_targets t
                            where target_type_cd = 'LE'
                            group by 1,
                                2,
                                3
                        )
                    group by 1,
                        2,
                        3
                    having sum(nts_le) > 0
                ) tp_perc on act.fisc_yr_per = tp_perc.fisc_yr_per
                and act.mega_brnd_desc = tp_perc.brand_nm
                and act."banner format" = tp_perc.cust_cd
                left outer join tp_acc_date on act.fisc_yr_per = tp_acc_date.fisc_yr_per
                and act."banner format" = tp_acc_date."banner format"
                inner join 
                (
                    SELECT exch_rate.from_crncy,
                        exch_rate.fisc_per,
                        sum(
                            CASE
                                WHEN exch_rate.to_crncy::text = 'USD'::character varying::text THEN exch_rate.ex_rt
                                ELSE 0::numeric::numeric(18, 0)
                            END
                        ) AS usd_rt,
                        sum(
                            CASE
                                WHEN exch_rate.to_crncy::text = 'HKD'::character varying::text THEN exch_rate.ex_rt
                                ELSE 0::numeric::numeric(18, 0)
                            END
                        ) AS lcl_rt
                    FROM v_intrm_reg_crncy_exch_fiscper exch_rate
                    WHERE exch_rate.from_crncy::text = 'HKD'::character varying::text
                        AND (
                            exch_rate.to_crncy::text = 'USD'::character varying::text
                            OR exch_rate.to_crncy::text = 'HKD'::character varying::text
                        )
                    GROUP BY exch_rate.from_crncy,
                        exch_rate.fisc_per
                ) exch_rate ON act.fisc_yr_per = (
                    left(exch_rate.fisc_per, 4)::text || right(exch_rate.fisc_per, 2)::text
                )::character varying
        ) a
        inner join time_gone t on t.fisc_per = a.fisc_yr_per
    group by subsource_type,
        fisc_yr_per,
        mega_brnd_desc,
        brnd_desc,
        "go to model",
        "banner format",
        "sub channel",
        "parent customer",
        usd_rt,
        lcl_rt,
        tp_acc_start,
        tp_perc,
        time_gone
),
le as
(    
    select 
        subsource_type,
        fisc_yr_per,
        mega_brnd_desc,
        'N/A' as brnd_desc,
        "go to model",
        "banner format",
        "sub channel",
        "parent customer",
        usd_rt,
        lcl_rt,
        tp_acc_start,
        case
            when nts_le = 0 then 0
            else tp_le / nts_le
        end as tp_perc,
        time_gone,
        preww7_act,
        nts_act,
        tp_act,
        tp_act_calc,
        nts_bp,
        tp_bp,
        nts_le,
        tp_le
    from 
        (
            SELECT 
                'LE'::character varying AS subsource_type,
                (t.year::text || t.month_no::text)::character varying AS fisc_yr_per,
                t.brand_nm AS mega_brnd_desc,
                t.channel_type_nm AS "go to model",
                t.cust_cd AS "banner format",
                cus_sales_extn."sub channel" AS "sub channel",
                cus_sales_extn."parent customer" AS "parent customer",
                exch_rate.usd_rt,
                exch_rate.lcl_rt,
                'N/A' as tp_acc_start,
                0.0::double precision as tp_perc,
                0.0::double precision AS preww7_act,
                0.0::double precision AS nts_act,
                0.0::double precision AS tp_act,
                0.0::double precision as tp_act_calc,
                0.0::double precision AS nts_bp,
                0.0::double precision AS tp_bp,
                sum(
                    CASE
                        WHEN t.metric_cd::text = 'NTS'::character varying::text THEN COALESCE(
                            t.target_value::double precision,
                            0::double precision
                        )
                        ELSE NULL::numeric::numeric(18, 0)::double precision
                    END
                ) AS nts_le,
                sum(
                    CASE
                        WHEN t.metric_cd::text = 'TP'::character varying::text THEN COALESCE(
                            t.target_value::double precision,
                            0::double precision
                        )
                        ELSE NULL::numeric::numeric(18, 0)::double precision
                    END
                ) AS tp_le
            FROM itg_mds_hk_le_targets t
            LEFT JOIN 
            (
                SELECT exch_rate.from_crncy,
                    exch_rate.fisc_per,
                    sum(
                        CASE
                            WHEN exch_rate.to_crncy::text = 'USD'::character varying::text THEN exch_rate.ex_rt
                            ELSE 0::numeric::numeric(18, 0)
                        END
                    ) AS usd_rt,
                    sum(
                        CASE
                            WHEN exch_rate.to_crncy::text = 'HKD'::character varying::text THEN exch_rate.ex_rt
                            ELSE 0::numeric::numeric(18, 0)
                        END
                    ) AS lcl_rt
                FROM v_intrm_reg_crncy_exch_fiscper exch_rate
                WHERE exch_rate.from_crncy::text = 'HKD'::character varying::text
                    AND (
                        exch_rate.to_crncy::text = 'USD'::character varying::text
                        OR exch_rate.to_crncy::text = 'HKD'::character varying::text
                    )
                GROUP BY exch_rate.from_crncy,
                    exch_rate.fisc_per
            ) exch_rate ON 
            (
                (
                    (
                        (t.year::text || '0'::character varying::text) || t.month_no::text
                    )::character varying
                )::text
            ) = exch_rate.fisc_per::character varying::text
            LEFT JOIN 
            (
                select distinct "banner format",
                    "sub channel",
                    "parent customer"
                from v_edw_customer_sales_dim
            ) cus_sales_extn on cus_sales_extn."banner format" = t.cust_cd
            where t.target_type_cd = 'LE'
            GROUP BY 1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11
        ) a
        inner join time_gone t on t.fisc_per = a.fisc_yr_per
),
bp as
(

        SELECT 
            'BP'::character varying AS subsource_type,
            (
                left(copa.fisc_yr_per, 4)::text || right(copa.fisc_yr_per, 2)::text
            )::character varying AS fisc_yr_per,
            copa.mega_brnd_desc,
            copa.brnd_desc,
            'N/A'::text AS "go to model",
            'N/A'::text AS "banner format",
            'N/A'::text AS "sub channel",
            'N/A'::text AS "parent customer",
            exch_rate.usd_rt,
            exch_rate.lcl_rt,
            'N/A' as tp_acc_start,
            0.0::double precision as tp_perc,
            time_gone,
            0.0::double precision AS preww7_act,
            0.0::double precision AS nts_act,
            0.0::double precision AS tp_act,
            0.0::double precision as tp_act_calc,
            sum(amt_obj_crcy) AS nts_bp,
            0.0::double precision AS tp_bp,
            0.0::double precision AS nts_le,
            0.0::double precision AS tp_le
        FROM 
            (
                SELECT 
                    mega_brnd_desc,
                    gcph_brand as brnd_desc,
                    copa.fisc_yr_per,
                    'HKD' as obj_crncy_co_obj,
                    copa.category,
                    SUM(amt_obj_crcy) AS amt_obj_crcy
                FROM edw_copa_plan_fact copa
                LEFT JOIN edw_material_dim mat 
                ON LTRIM (copa.matl_num::CHARACTER VARYING::TEXT, 0) = LTRIM (mat.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT)
                LEFT JOIN 
                (
                    select distinct materialnumber,
                        gcph_brand
                    from edw_gch_producthierarchy
                ) gchph on ltrim(mat.matl_num::text, '0'::character varying::text) = ltrim(
                    gchph.materialnumber::text,
                    '0'::character varying::text
                )
                INNER JOIN 
                (
                    SELECT fisc_yr_per,
                        MAX(category) category
                    FROM edw_copa_plan_fact copa
                    WHERE ctry_key = 'HK'
                        AND obj_crncy = 'HKD'
                        AND acct_hier_shrt_desc = 'NTS'
                        AND category IN ('BP', 'JU', 'NU')
                    GROUP BY fisc_yr_per
                ) lf ON copa.fisc_yr_per = lf.fisc_yr_per
                AND copa.category = lf.category
                WHERE ctry_key = 'HK'
                    AND obj_crncy = 'HKD'
                    AND acct_hier_shrt_desc = 'NTS'
                    AND copa.category IN ('BP', 'JU', 'NU')
                GROUP BY mega_brnd_desc,
                    gcph_brand,
                    copa.fisc_yr_per,
                    copa.category,
                    obj_crncy_co_obj
            ) copa
            LEFT JOIN 
            (
                SELECT exch_rate.from_crncy,
                    exch_rate.fisc_per,
                    sum(
                        CASE
                            WHEN exch_rate.to_crncy::text = 'USD'::character varying::text THEN exch_rate.ex_rt
                            ELSE 0::numeric::numeric(18, 0)
                        END
                    ) AS usd_rt,
                    sum(
                        CASE
                            WHEN exch_rate.to_crncy::text = 'HKD'::character varying::text THEN exch_rate.ex_rt
                            ELSE 0::numeric::numeric(18, 0)
                        END
                    ) AS lcl_rt
                FROM v_intrm_reg_crncy_exch_fiscper exch_rate
                WHERE exch_rate.from_crncy::text = 'HKD'::character varying::text
                    AND (
                        exch_rate.to_crncy::text = 'USD'::character varying::text
                        OR exch_rate.to_crncy::text = 'HKD'::character varying::text
                    )
                GROUP BY exch_rate.from_crncy,
                    exch_rate.fisc_per
            ) exch_rate ON copa.obj_crncy_co_obj::text = exch_rate.from_crncy::text
            AND copa.fisc_yr_per = exch_rate.fisc_per::numeric::numeric(18, 0)
            inner join time_gone t on t.fisc_per = (
                LEFT (copa.fisc_yr_per, 4)::TEXT || RIGHT (copa.fisc_yr_per, 2)::TEXT
            )::CHARACTER VARYING
        GROUP BY subsource_type,
            mega_brnd_desc,
            brnd_desc,
            "go to model",
            "banner format",
            "sub channel",
            "parent customer",
            exch_rate.usd_rt,
            exch_rate.lcl_rt,
            tp_acc_start,
            tp_perc,
            copa.fisc_yr_per,
            time_gone
        union all
        select 
            subsource_type,
            fisc_yr_per::character varying,
            mega_brnd_desc,
            brnd_desc,
            "go to model",
            "banner format",
            "sub channel",
            "parent customer",
            usd_rt,
            lcl_rt,
            tp_acc_start,
            tp_perc,
            time_gone,
            preww7_act,
            nts_act,
            tp_act,
            tp_act_calc,
            nts_bp,
            tp_bp,
            nts_le,
            tp_le
        from 
            (
                SELECT 
                    'BP'::character varying AS subsource_type,
                    (t.year::text || t.month_no::text)::character varying AS fisc_yr_per,
                    t.brand_nm AS mega_brnd_desc,
                    'N/A'::text AS brnd_desc,
                    'N/A'::text AS "go to model",
                    'N/A'::text AS "banner format",
                    'N/A'::text AS "sub channel",
                    'N/A'::text AS "parent customer",
                    exch_rate.usd_rt,
                    exch_rate.lcl_rt,
                    'N/A' as tp_acc_start,
                    0.0::double precision as tp_perc,
                    0.0::double precision AS preww7_act,
                    0.0::double precision AS nts_act,
                    0.0::double precision AS tp_act,
                    0.0::double precision as tp_act_calc,
                    0.0::double precision AS nts_bp,
                    sum(
                        CASE
                            WHEN t.metric_cd::text = 'TP'::character varying::text THEN COALESCE(
                                t.target_value::double precision,
                                0::double precision
                            )
                            ELSE NULL::numeric::numeric(18, 0)::double precision
                        END
                    ) AS tp_bp,
                    0.0::double precision AS nts_le,
                    0.0::double precision AS tp_le
                FROM itg_mds_hk_le_targets t
                LEFT JOIN 
                (
                    SELECT exch_rate.from_crncy,
                        exch_rate.fisc_per,
                        sum(
                            CASE
                                WHEN exch_rate.to_crncy::text = 'USD'::character varying::text THEN exch_rate.ex_rt
                                ELSE 0::numeric::numeric(18, 0)
                            END
                        ) AS usd_rt,
                        sum(
                            CASE
                                WHEN exch_rate.to_crncy::text = 'HKD'::character varying::text THEN exch_rate.ex_rt
                                ELSE 0::numeric::numeric(18, 0)
                            END
                        ) AS lcl_rt
                    FROM v_intrm_reg_crncy_exch_fiscper exch_rate
                    WHERE exch_rate.from_crncy::text = 'HKD'::character varying::text
                        AND (
                            exch_rate.to_crncy::text = 'USD'::character varying::text
                            OR exch_rate.to_crncy::text = 'HKD'::character varying::text
                        )
                    GROUP BY exch_rate.from_crncy,
                        exch_rate.fisc_per
                ) exch_rate ON (
                    (
                        (
                            (t.year::text || '0'::character varying::text) || t.month_no::text
                        )::character varying
                    )::text
                ) = exch_rate.fisc_per::character varying::text
                LEFT JOIN v_edw_customer_sales_dim cus_sales_extn 
                on cus_sales_extn."banner format" = t.cust_cd
                where t.target_type_cd = 'Financial Target'
                GROUP BY 1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11
            ) a
            inner join time_gone t on t.fisc_per = a.fisc_yr_per
),
tp_acc_date_gt as 
(
    select fisc_yr_per,
        --"banner format",
        "go to model",
        max(caln_day) tp_acc_start
    from (
            SELECT (
                    left(fisc_yr_per, 4)::text || right(fisc_yr_per, 2)::text
                )::character varying AS fisc_yr_per,
                "banner format",
                "go to model",
                caln_day,
                SUM(
                    COALESCE(
                        edw_copa_trans_fact.amt_obj_crncy::DOUBLE precision,
                        0::DOUBLE precision
                    )
                ) AS amt_obj_crncy
            FROM edw_copa_trans_fact
                LEFT JOIN edw_material_dim mat ON ltrim(
                    edw_copa_trans_fact.matl_num::character varying::text,
                    0
                ) = ltrim(mat.matl_num::text, '0'::character varying::text)
                LEFT JOIN v_edw_customer_sales_dim cus_sales_extn ON edw_copa_trans_fact.sls_org::text = cus_sales_extn.sls_org::text
                AND edw_copa_trans_fact.dstr_chnl::text = cus_sales_extn.dstr_chnl::text
                AND edw_copa_trans_fact.div::text = cus_sales_extn.div::text
                AND ltrim(
                    edw_copa_trans_fact.cust_num::character varying::text,
                    0
                ) = ltrim(
                    cus_sales_extn.cust_num::text,
                    '0'::character varying::text
                )
            WHERE edw_copa_trans_fact.sls_org::TEXT IN (
                    SELECT parameter_value
                    FROM itg_query_parameters
                    WHERE country_code = 'HK'
                        AND parameter_name = 'copa'
                        AND parameter_type = 'sls_org'
                )
                AND acct_hier_shrt_desc::TEXT = 'NTS'::CHARACTER VARYING::TEXT
                AND LTRIM(acct_num::TEXT, '0'::CHARACTER VARYING::TEXT) IN (
                    SELECT parameter_value
                    FROM itg_query_parameters
                    WHERE country_code = 'HK'
                        AND parameter_name = 'TP'
                        AND parameter_type = 'GL_Account'
                )
            GROUP BY 1,
                2,
                3,
                4
            having sum(amt_obj_crncy) <> 0
        )
    where "go to model" = 'Indirect Accounts'
    group by fisc_yr_per,
        "go to model"
),
gt_act as 
(     
    select 
        'GT_Act'::character varying AS subsource_type,
        fisc_yr_per,
        mega_brnd_desc,
        brnd_desc,
        "go to model",
        'N/A' as "banner format",
        'N/A' as "sub channel",
        'N/A' as "parent customer",
        usd_rt,
        lcl_rt,
        tp_acc_start,
        tp_perc,
        time_gone,
        sum(PREWW7_ACT) preww7_act,
        0.0::double precision AS nts_act,
        sum(tp_act) * -1 tp_act,
        sum(tp_calc) as tp_act_calc,
        0.0::double precision AS nts_bp,
        0.0::double precision AS tp_bp,
        0.0::double precision AS nts_le,
        0.0::double precision AS tp_le
    from
        (
            select 
                act.fisc_yr_per,
                act.mega_brnd_desc,
                brnd_desc,
                --act."banner format",
                act."go to model",
                --act."sub channel",
                --act."parent customer",
                usd_rt,
                lcl_rt,
                act.caln_day,
                tp_perc,
                tp_acc_start,
                act.preww7_act,
                act.tp_act,
                case
                    when (
                        caln_day < tp_acc_start
                        or tp_acc_start is null
                    ) then act.preww7_act * tp_perc
                    else 0
                end tp_calc
            from 
                (
                    SELECT 
                        (
                            left(copa.fisc_yr_per, 4)::text || right(copa.fisc_yr_per, 2)::text
                        )::character varying AS fisc_yr_per,
                        caln_day,
                        mat.mega_brnd_desc,
                        gchph.gcph_brand brnd_desc,
                        --cus_sales_extn."banner format",
                        cus_sales_extn."go to model",
                        --cus_sales_extn."sub channel",
                        --cus_sales_extn."parent customer",
                        sum(gts_act) + sum(preww_d) preww7_act,
                        sum(tp_act) tp_act
                    from (
                            select 
                                fisc_yr_per,
                                matl_num,
                                sls_org,
                                div,
                                dstr_chnl,
                                cust_num,
                                caln_day,
                                sum(
                                    CASE
                                        WHEN copa.acct_hier_shrt_desc::text = 'GTS'::character varying::text THEN COALESCE(copa.amt_obj_crncy, 0::double precision)
                                        ELSE 0::numeric::numeric(18, 0)::double precision
                                    END
                                ) AS gts_act,
                                sum(
                                    CASE
                                        WHEN copa.acct_hier_shrt_desc::text = 'NTS'::character varying::text
                                        AND LTRIM(acct_num::TEXT, '0'::CHARACTER VARYING::TEXT) IN (
                                            SELECT parameter_value
                                            FROM itg_query_parameters
                                            WHERE country_code = 'HK'
                                                AND parameter_name = 'PreWW'
                                                AND parameter_type = 'GL_Account'
                                        ) THEN COALESCE(copa.amt_obj_crncy, 0::double precision)
                                        ELSE 0::numeric::numeric(18, 0)::double precision
                                    END
                                ) preww_d,
                                sum(
                                    CASE
                                        WHEN copa.acct_hier_shrt_desc::text = 'NTS'::character varying::text
                                        AND LTRIM(acct_num::TEXT, '0'::CHARACTER VARYING::TEXT) IN (
                                            SELECT parameter_value
                                            FROM itg_query_parameters
                                            WHERE country_code = 'HK'
                                                AND parameter_name = 'TP'
                                                AND parameter_type = 'GL_Account'
                                        ) THEN COALESCE(copa.amt_obj_crncy, 0::double precision)
                                        ELSE 0::numeric::numeric(18, 0)::double precision
                                    END
                                ) AS tp_act
                            FROM edw_copa_trans_fact copa
                            WHERE copa.sls_org::TEXT IN (
                                    SELECT parameter_value
                                    FROM itg_query_parameters
                                    WHERE country_code = 'HK'
                                        AND parameter_name = 'copa'
                                        AND parameter_type = 'sls_org'
                                )
                            group by fisc_yr_per,
                                matl_num,
                                sls_org,
                                div,
                                dstr_chnl,
                                cust_num,
                                caln_day
                            having 
                                (
                                    sum(
                                        CASE
                                            WHEN copa.acct_hier_shrt_desc::text = 'GTS'::character varying::text THEN COALESCE(copa.amt_obj_crncy, 0::double precision)
                                            ELSE 0::numeric::numeric(18, 0)::double precision
                                        END
                                    ) + sum(
                                        CASE
                                            WHEN copa.acct_hier_shrt_desc::text = 'NTS'::character varying::text
                                            AND LTRIM(acct_num::TEXT, '0'::CHARACTER VARYING::TEXT) IN (
                                                SELECT parameter_value
                                                FROM itg_query_parameters
                                                WHERE country_code = 'HK'
                                                    AND parameter_name = 'PreWW'
                                                    AND parameter_type = 'GL_Account'
                                            ) THEN COALESCE(copa.amt_obj_crncy, 0::double precision)
                                            ELSE 0::numeric::numeric(18, 0)::double precision
                                        END
                                    ) + sum(
                                        CASE
                                            WHEN copa.acct_hier_shrt_desc::text = 'NTS'::character varying::text
                                            AND LTRIM(acct_num::TEXT, '0'::CHARACTER VARYING::TEXT) IN (
                                                SELECT parameter_value
                                                FROM itg_query_parameters
                                                WHERE country_code = 'HK'
                                                    AND parameter_name = 'TP'
                                                    AND parameter_type = 'GL_Account'
                                            ) THEN COALESCE(copa.amt_obj_crncy, 0::double precision)
                                            ELSE 0::numeric::numeric(18, 0)::double precision
                                        END
                                    ) <> 0
                                )
                        ) copa
                        LEFT JOIN edw_material_dim mat ON ltrim(copa.matl_num::character varying::text, 0) = ltrim(mat.matl_num::text, '0'::character varying::text)
                        LEFT JOIN (
                            select distinct materialnumber,
                                gcph_brand
                            from edw_gch_producthierarchy
                        ) gchph on ltrim(mat.matl_num::text, '0'::character varying::text) = ltrim(
                            gchph.materialnumber::text,
                            '0'::character varying::text
                        )
                        LEFT JOIN v_edw_customer_sales_dim cus_sales_extn ON copa.sls_org::text = cus_sales_extn.sls_org::text
                        AND copa.dstr_chnl::text = cus_sales_extn.dstr_chnl::text
                        AND copa.div::text = cus_sales_extn.div::text
                        AND ltrim(copa.cust_num::character varying::text, 0) = ltrim(
                            cus_sales_extn.cust_num::text,
                            '0'::character varying::text
                        )
                    where "go to model" = 'Indirect Accounts'
                    group by 1,
                        2,
                        3,
                        4,
                        5
                ) act
                left outer join -- TP %
                (
                    select 
                        fisc_yr_per,
                        brand_nm,
                        cust_cd,
                        sum(tp_le) /(sum(tp_le) + sum(nts_le)) tp_perc
                    from 
                        (
                            select 
                                (t.year::text || t.month_no::text)::character varying AS fisc_yr_per,
                                cust_cd,
                                brand_nm,
                                sum(
                                    CASE
                                        WHEN t.metric_cd::text = 'NTS'::character varying::text THEN COALESCE(
                                            t.target_value::double precision,
                                            0::double precision
                                        )
                                        ELSE NULL::numeric::numeric(18, 0)::double precision
                                    END
                                ) AS nts_le,
                                sum(
                                    CASE
                                        WHEN t.metric_cd::text = 'TP'::character varying::text THEN COALESCE(
                                            t.target_value::double precision,
                                            0::double precision
                                        )
                                        ELSE NULL::numeric::numeric(18, 0)::double precision
                                    END
                                ) AS tp_le
                            from itg_mds_hk_le_targets t
                            where target_type_cd = 'LE'
                            group by 1,
                                2,
                                3
                        )
                    where cust_cd = 'General Trade'
                    group by 1,
                        2,
                        3
                    having sum(nts_le) > 0
                ) tp_perc on act.fisc_yr_per = tp_perc.fisc_yr_per
                and act.mega_brnd_desc = tp_perc.brand_nm
                left outer join tp_acc_date_gt on act.fisc_yr_per = tp_acc_date_gt.fisc_yr_per
                inner join 
                (
                    SELECT exch_rate.from_crncy,
                        exch_rate.fisc_per,
                        sum(
                            CASE
                                WHEN exch_rate.to_crncy::text = 'USD'::character varying::text THEN exch_rate.ex_rt
                                ELSE 0::numeric::numeric(18, 0)
                            END
                        ) AS usd_rt,
                        sum(
                            CASE
                                WHEN exch_rate.to_crncy::text = 'HKD'::character varying::text THEN exch_rate.ex_rt
                                ELSE 0::numeric::numeric(18, 0)
                            END
                        ) AS lcl_rt
                    FROM v_intrm_reg_crncy_exch_fiscper exch_rate
                    WHERE exch_rate.from_crncy::text = 'HKD'::character varying::text
                        AND (
                            exch_rate.to_crncy::text = 'USD'::character varying::text
                            OR exch_rate.to_crncy::text = 'HKD'::character varying::text
                        )
                    GROUP BY exch_rate.from_crncy,
                        exch_rate.fisc_per
                ) exch_rate ON act.fisc_yr_per = (
                    left(exch_rate.fisc_per, 4)::text || right(exch_rate.fisc_per, 2)::text
                )::character varying
        ) a
        inner join time_gone t on t.fisc_per = a.fisc_yr_per
    group by subsource_type,
        fisc_yr_per,
        mega_brnd_desc,
        brnd_desc,
        "go to model",
        "banner format",
        "sub channel",
        "parent customer",
        usd_rt,
        lcl_rt,
        tp_acc_start,
        tp_perc,
        time_gone
),
final as 
(
    select
        subsource_type::varchar(6) subsource_type,
        fisc_yr_per::varchar(22) fisc_yr_per,
        mega_brnd_desc::varchar(100) mega_brnd_desc,
        brnd_desc::varchar(100) brnd_desc,
        "go to model"::varchar(50) "go to model",
        "banner format"::varchar(50) "banner format",
        "sub channel"::varchar(50) "sub channel",
        "parent customer"::varchar(50) "parent customer",
        usd_rt::number(38,10) usd_rt,
        lcl_rt::number(38,10) lcl_rt,
        tp_acc_start::varchar(20) tp_acc_start,
        tp_perc::float tp_perc,
        time_gone::float time_gone,
        preww7_act::float preww7_act,
        nts_act::float nts_act,
        tp_act::float tp_act,
        tp_act_calc::float tp_act_calc,
        nts_bp::float nts_bp,
        tp_bp::float tp_bp,
        nts_le::float nts_le,
        tp_le::float tp_le
    from
    (
        select * from actual
        union all
        select * from le
        union all
        select * from bp
        union all
        select * from gt_act
    )
    

)
select * from final