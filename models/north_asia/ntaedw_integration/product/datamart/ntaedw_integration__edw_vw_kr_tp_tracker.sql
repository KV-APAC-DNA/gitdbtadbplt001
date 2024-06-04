with itg_query_parameters_rg as(
    select * from {{ source('aspitg_integration', 'itg_query_parameters') }}
),
edw_acct_ciw_hier as(
    select * from {{ ref('aspedw_integration__edw_acct_ciw_hier') }} 
),
v_intrm_reg_crncy_exch_fiscper as(
    select * from  {{ ref('aspedw_integration__v_intrm_reg_crncy_exch_fiscper') }}
),
edw_customer_attr_flat_dim as(
    select * from aspedw_integration.edw_customer_attr_flat_dim
),
edw_intrm_calendar as(
    select * from {{ ref('ntaedw_integration__edw_intrm_calendar') }}
),
v_intrm_copa_trans as(
    select * from {{ ref('ntaedw_integration__v_intrm_copa_trans') }}
),
itg_kr_tp_tracker_target as(
    select * from {{ ref('ntaitg_integration__itg_kr_tp_tracker_target') }}
),
itg_query_parameters_na as(
    select * from {{ source('ntaitg_integration', 'itg_query_parameters') }}
),
nts_actuals as 
(
    SELECT 
        'NTS_ACTUALS'::character varying AS identifier,
        v_intrm_copa_trans.sls_org,
        v_intrm_copa_trans.matl_num,
        v_intrm_copa_trans.cust_num,
        v_intrm_copa_trans.acct_num,
        v_intrm_copa_trans.sls_ofc,
        v_intrm_copa_trans.ctry_nm,
        v_intrm_copa_trans.ctry_key,
        v_intrm_copa_trans.sls_grp,
        to_date(
            (v_intrm_copa_trans.caln_day)::text,
            ('YYYYMMDD'::character varying)::text
        ) AS caln_day,
        v_intrm_copa_trans.caln_yr_mo,
        v_intrm_copa_trans.fisc_yr,
        v_intrm_copa_trans.fisc_yr_per,
        v_intrm_copa_trans.b3_base_prod,
        v_intrm_copa_trans.b4_var,
        v_intrm_copa_trans.b5_put_up,
        v_intrm_copa_trans.b1_mega_brnd,
        v_intrm_copa_trans.b2_brnd,
        v_intrm_copa_trans.prod_minor,
        v_intrm_copa_trans.prod_maj,
        v_intrm_copa_trans.prod_fran,
        v_intrm_copa_trans.fran,
        v_intrm_copa_trans.matl_sls,
        v_intrm_copa_trans.prod_hier,
        v_intrm_copa_trans.amt_obj_crncy AS nts_actuals,
        0 AS tp_actuals,
        v_intrm_copa_trans.qty AS nts_qty,
        0 AS tp_qty,
        v_intrm_copa_trans.sls_grp_desc,
        v_intrm_copa_trans.sls_ofc_desc,
        v_intrm_copa_trans.matl_desc,
        v_intrm_copa_trans.mega_brnd_desc,
        v_intrm_copa_trans.brnd_desc,
        v_intrm_copa_trans.varnt_desc,
        v_intrm_copa_trans.base_prod_desc,
        v_intrm_copa_trans.put_up_desc,
        v_intrm_copa_trans.channel,
        v_intrm_copa_trans.med_desc,
        v_intrm_copa_trans.edw_cust_nm,
        v_intrm_copa_trans.from_crncy,
        v_intrm_copa_trans.to_crncy,
        v_intrm_copa_trans.ex_rt_typ,
        v_intrm_copa_trans.ex_rt,
        v_intrm_copa_trans.acct_hier_desc,
        v_intrm_copa_trans.acct_hier_shrt_desc,
        v_intrm_copa_trans.company_nm,
        v_intrm_copa_trans.ean_num,
        v_intrm_copa_trans.prod_hier_lvl1,
        v_intrm_copa_trans.prod_hier_lvl2,
        v_intrm_copa_trans.prod_hier_lvl3,
        v_intrm_copa_trans.prod_hier_lvl4,
        v_intrm_copa_trans.prod_hier_lvl5,
        v_intrm_copa_trans.prod_hier_lvl6,
        v_intrm_copa_trans.prod_hier_lvl7,
        v_intrm_copa_trans.prod_hier_lvl8,
        v_intrm_copa_trans.prod_hier_lvl9,
        v_intrm_copa_trans.store_type,
        'NA'::character varying AS account_classification,
        'NA'::character varying AS target_type,
        0 AS tgt_value
    FROM v_intrm_copa_trans v_intrm_copa_trans
    WHERE 
        (
            (
                upper((v_intrm_copa_trans.acct_hier_shrt_desc)::text) = ('NTS'::character varying)::text
            )
            AND (
                upper((v_intrm_copa_trans.ctry_key)::text) = ('KR'::character varying)::text
            )
        )
),
tp_actuals as 
(
    SELECT 
        'TP_ACTUALS'::character varying AS identifier,
        v_intrm_copa_trans.sls_org,
        v_intrm_copa_trans.matl_num,
        v_intrm_copa_trans.cust_num,
        v_intrm_copa_trans.acct_num,
        v_intrm_copa_trans.sls_ofc,
        v_intrm_copa_trans.ctry_nm,
        v_intrm_copa_trans.ctry_key,
        v_intrm_copa_trans.sls_grp,
        to_date(
            (v_intrm_copa_trans.caln_day)::text,
            ('YYYYMMDD'::character varying)::text
        ) AS caln_day,
        v_intrm_copa_trans.caln_yr_mo,
        v_intrm_copa_trans.fisc_yr,
        v_intrm_copa_trans.fisc_yr_per,
        v_intrm_copa_trans.b3_base_prod,
        v_intrm_copa_trans.b4_var,
        v_intrm_copa_trans.b5_put_up,
        v_intrm_copa_trans.b1_mega_brnd,
        v_intrm_copa_trans.b2_brnd,
        v_intrm_copa_trans.prod_minor,
        v_intrm_copa_trans.prod_maj,
        v_intrm_copa_trans.prod_fran,
        v_intrm_copa_trans.fran,
        v_intrm_copa_trans.matl_sls,
        v_intrm_copa_trans.prod_hier,
        0 AS nts_actuals,
        (
            (
                v_intrm_copa_trans.amt_obj_crncy * ((ciw.multiplication_factor)::numeric)::numeric(18, 0)
            )
        )::numeric(20, 5) AS tp_actuals,
        0 AS nts_qty,
        v_intrm_copa_trans.qty AS tp_qty,
        v_intrm_copa_trans.sls_grp_desc,
        v_intrm_copa_trans.sls_ofc_desc,
        v_intrm_copa_trans.matl_desc,
        v_intrm_copa_trans.mega_brnd_desc,
        v_intrm_copa_trans.brnd_desc,
        v_intrm_copa_trans.varnt_desc,
        v_intrm_copa_trans.base_prod_desc,
        v_intrm_copa_trans.put_up_desc,
        v_intrm_copa_trans.channel,
        v_intrm_copa_trans.med_desc,
        v_intrm_copa_trans.edw_cust_nm,
        v_intrm_copa_trans.from_crncy,
        v_intrm_copa_trans.to_crncy,
        v_intrm_copa_trans.ex_rt_typ,
        v_intrm_copa_trans.ex_rt,
        v_intrm_copa_trans.acct_hier_desc,
        v_intrm_copa_trans.acct_hier_shrt_desc,
        v_intrm_copa_trans.company_nm,
        v_intrm_copa_trans.ean_num,
        v_intrm_copa_trans.prod_hier_lvl1,
        v_intrm_copa_trans.prod_hier_lvl2,
        v_intrm_copa_trans.prod_hier_lvl3,
        v_intrm_copa_trans.prod_hier_lvl4,
        v_intrm_copa_trans.prod_hier_lvl5,
        v_intrm_copa_trans.prod_hier_lvl6,
        v_intrm_copa_trans.prod_hier_lvl7,
        v_intrm_copa_trans.prod_hier_lvl8,
        v_intrm_copa_trans.prod_hier_lvl9,
        v_intrm_copa_trans.store_type,
        (ciw.account_classification)::character varying AS account_classification,
        'NA'::character varying AS target_type,
        ((0)::numeric)::numeric(18, 0) AS tgt_value
    FROM 
        (
            v_intrm_copa_trans v_intrm_copa_trans
            JOIN (
                SELECT DISTINCT ach.acct_num,
                    ach.measure_code,
                    qp.account_classification,
                    ach.multiplication_factor
                FROM 
                    (
                        edw_acct_ciw_hier ach
                        JOIN (
                            SELECT DISTINCT itg_query_parameters_na.parameter_value AS acct_num,
                                split_part(
                                    (itg_query_parameters_na.parameter_type)::text,
                                    ('-'::character varying)::text,
                                    2
                                ) AS account_classification
                            FROM itg_query_parameters_na
                            WHERE (
                                    (
                                        upper((itg_query_parameters_na.country_code)::text) = ('KR'::character varying)::text
                                    )
                                    AND (
                                        upper((itg_query_parameters_na.parameter_name)::text) = ('TP'::character varying)::text
                                    )
                                )
                        ) qp ON (
                            (
                                ltrim(
                                    (ach.acct_num)::text,
                                    ((0)::character varying)::text
                                ) = ltrim(
                                    (qp.acct_num)::text,
                                    ((0)::character varying)::text
                                )
                            )
                        )
                    )
                WHERE (
                        upper((ach.measure_code)::text) = ('NTS'::character varying)::text
                    )
            ) ciw ON (
                (
                    (
                        (
                            (v_intrm_copa_trans.acct_num)::text = (ciw.acct_num)::text
                        )
                        AND (
                            upper((v_intrm_copa_trans.ctry_key)::text) = ('KR'::character varying)::text
                        )
                    )
                    AND (
                        upper((v_intrm_copa_trans.acct_hier_shrt_desc)::text) = ('NTS'::character varying)::text
                    )
                )
            )
        )
),
tp_target as 
(
    SELECT 
        'TP_TARGET'::character varying AS identifier,
        'NA'::character varying AS sls_org,
        'NA'::character varying AS matl_num,
        'NA'::character varying AS cust_num,
        'NA'::character varying AS acct_num,
        cust.sls_ofc,
        tgt.country_name AS ctry_nm,
        tgt.cntry_cd AS ctry_key,
        tgt.sales_group_cd AS sls_grp,
        tgt.tgt_date AS caln_day,
        cal.cal_mo_1 AS caln_yr_mo,
        cal.fisc_yr,
        cal.fisc_per AS fisc_yr_per,
        'NA'::character varying AS b3_base_prod,
        'NA'::character varying AS b4_var,
        'NA'::character varying AS b5_put_up,
        'NA'::character varying AS b1_mega_brnd,
        'NA'::character varying AS b2_brnd,
        'NA'::character varying AS prod_minor,
        'NA'::character varying AS prod_maj,
        'NA'::character varying AS prod_fran,
        'NA'::character varying AS fran,
        'NA'::character varying AS matl_sls,
        'NA'::character varying AS prod_hier,
        0 AS nts_actuals,
        0 AS tp_actuals,
        0 AS nts_qty,
        0 AS tp_qty,
        cust.sls_grp AS sls_grp_desc,
        cust.sls_ofc_desc,
        'NA'::character varying AS matl_desc,
        'NA'::character varying AS mega_brnd_desc,
        'NA'::character varying AS brnd_desc,
        'NA'::character varying AS varnt_desc,
        'NA'::character varying AS base_prod_desc,
        'NA'::character varying AS put_up_desc,
        tgt.channel,
        'NA'::character varying AS med_desc,
        'NA'::character varying AS edw_cust_nm,
        exrt.from_crncy,
        exrt.to_crncy,
        exrt.ex_rt_typ,
        exrt.ex_rt,
        'NA'::character varying AS acct_hier_desc,
        'NA'::character varying AS acct_hier_shrt_desc,
        'NA'::character varying AS company_nm,
        'NA'::character varying AS ean_num,
        'NA'::character varying AS prod_hier_lvl1,
        'NA'::character varying AS prod_hier_lvl2,
        'NA'::character varying AS prod_hier_lvl3,
        'NA'::character varying AS prod_hier_lvl4,
        'NA'::character varying AS prod_hier_lvl5,
        'NA'::character varying AS prod_hier_lvl6,
        'NA'::character varying AS prod_hier_lvl7,
        'NA'::character varying AS prod_hier_lvl8,
        'NA'::character varying AS prod_hier_lvl9,
        tgt.store_type,
        'NA'::character varying AS account_classification,
        tgt.target_type,
        tgt.tgt_value
    FROM 
        (
            (
                (
                    (
                        SELECT itg_kr_tp_tracker_target.cntry_cd,
                            itg_kr_tp_tracker_target.country_name,
                            itg_kr_tp_tracker_target.crncy_cd,
                            itg_kr_tp_tracker_target.channel,
                            itg_kr_tp_tracker_target.store_type,
                            itg_kr_tp_tracker_target.sales_group_cd,
                            itg_kr_tp_tracker_target.sales_group_name,
                            itg_kr_tp_tracker_target.target_type,
                            itg_kr_tp_tracker_target.year,
                            itg_kr_tp_tracker_target.tgt_date,
                            itg_kr_tp_tracker_target.ytd_target_fy,
                            itg_kr_tp_tracker_target.tgt_value,
                            itg_kr_tp_tracker_target.filename,
                            itg_kr_tp_tracker_target.run_id,
                            itg_kr_tp_tracker_target.crtd_dttm,
                            itg_kr_tp_tracker_target.brand,
                            itg_kr_tp_tracker_target.target_category_code,
                            itg_kr_tp_tracker_target.target_category,
                            itg_kr_tp_tracker_target.target_amt,
                            itg_kr_tp_tracker_target.ytd_target_amt
                        FROM itg_kr_tp_tracker_target
                        WHERE (
                                (itg_kr_tp_tracker_target.target_category_code)::text = 'TP'::text
                            )
                    ) tgt
                    LEFT JOIN (
                        SELECT DISTINCT edw_intrm_calendar.cal_day,
                            edw_intrm_calendar.cal_mo_1,
                            edw_intrm_calendar.fisc_yr,
                            edw_intrm_calendar.fisc_per,
                            edw_intrm_calendar.pstng_per
                        FROM edw_intrm_calendar
                    ) cal ON ((tgt.tgt_date = cal.cal_day))
                )
                LEFT JOIN (
                    SELECT DISTINCT v_intrm_reg_crncy_exch_fiscper.ex_rt_typ,
                        v_intrm_reg_crncy_exch_fiscper.from_crncy,
                        v_intrm_reg_crncy_exch_fiscper.fisc_per,
                        v_intrm_reg_crncy_exch_fiscper.to_crncy,
                        v_intrm_reg_crncy_exch_fiscper.ex_rt
                    FROM v_intrm_reg_crncy_exch_fiscper
                    WHERE (
                            (
                                upper(
                                    (v_intrm_reg_crncy_exch_fiscper.from_crncy)::text
                                ) = ('KRW'::character varying)::text
                            )
                            AND (
                                (
                                    (
                                        upper((v_intrm_reg_crncy_exch_fiscper.to_crncy)::text) = ('USD'::character varying)::text
                                    )
                                    OR (
                                        upper((v_intrm_reg_crncy_exch_fiscper.to_crncy)::text) = ('KRW'::character varying)::text
                                    )
                                )
                                OR (
                                    upper((v_intrm_reg_crncy_exch_fiscper.to_crncy)::text) = ('SGD'::character varying)::text
                                )
                            )
                        )
                ) exrt ON (
                    (
                        ((tgt.crncy_cd)::text = (exrt.from_crncy)::text)
                        AND (cal.fisc_per = exrt.fisc_per)
                    )
                )
            )
            LEFT JOIN (
                SELECT DISTINCT edw_customer_attr_flat_dim.sls_grp_cd,
                    edw_customer_attr_flat_dim.sls_ofc,
                    edw_customer_attr_flat_dim.sls_ofc_desc,
                    edw_customer_attr_flat_dim.store_typ,
                    edw_customer_attr_flat_dim.channel,
                    edw_customer_attr_flat_dim.sls_grp
                FROM edw_customer_attr_flat_dim
                WHERE (
                        upper((edw_customer_attr_flat_dim.county)::text) = ('KR'::character varying)::text
                    )
            ) cust ON (
                (
                    COALESCE(
                        upper(
                            trim(
                                (tgt.sales_group_cd)::text,
                                ('NA'::character varying)::text
                            )
                        )
                    ,'NA') = COALESCE(
                        upper(
                            trim(
                                (cust.sls_grp_cd)::text,
                                ('NA'::character varying)::text
                            )
                        )
                    ,'NA')
                )
            )
        )
),
tp_target_ytd as 
(
    SELECT 
        DISTINCT 'TP_TARGET_YTD'::character varying AS identifier,
        'NA'::character varying AS sls_org,
        'NA'::character varying AS matl_num,
        'NA'::character varying AS cust_num,
        'NA'::character varying AS acct_num,
        cust.sls_ofc,
        tgt.country_name AS ctry_nm,
        tgt.cntry_cd AS ctry_key,
        tgt.sales_group_cd AS sls_grp,
        '9999-12-31'::date AS caln_day,
        999999 AS caln_yr_mo,
        cal.fisc_yr,
        9999999 AS fisc_yr_per,
        'NA'::character varying AS b3_base_prod,
        'NA'::character varying AS b4_var,
        'NA'::character varying AS b5_put_up,
        'NA'::character varying AS b1_mega_brnd,
        'NA'::character varying AS b2_brnd,
        'NA'::character varying AS prod_minor,
        'NA'::character varying AS prod_maj,
        'NA'::character varying AS prod_fran,
        'NA'::character varying AS fran,
        'NA'::character varying AS matl_sls,
        'NA'::character varying AS prod_hier,
        0 AS nts_actuals,
        0 AS tp_actuals,
        0 AS nts_qty,
        0 AS tp_qty,
        cust.sls_grp AS sls_grp_desc,
        cust.sls_ofc_desc,
        'NA'::character varying AS matl_desc,
        'NA'::character varying AS mega_brnd_desc,
        'NA'::character varying AS brnd_desc,
        'NA'::character varying AS varnt_desc,
        'NA'::character varying AS base_prod_desc,
        'NA'::character varying AS put_up_desc,
        tgt.channel,
        'NA'::character varying AS med_desc,
        'NA'::character varying AS edw_cust_nm,
        exrt.from_crncy,
        exrt.to_crncy,
        exrt.ex_rt_typ,
        exrt.ex_rt,
        'NA'::character varying AS acct_hier_desc,
        'NA'::character varying AS acct_hier_shrt_desc,
        'NA'::character varying AS company_nm,
        'NA'::character varying AS ean_num,
        'NA'::character varying AS prod_hier_lvl1,
        'NA'::character varying AS prod_hier_lvl2,
        'NA'::character varying AS prod_hier_lvl3,
        'NA'::character varying AS prod_hier_lvl4,
        'NA'::character varying AS prod_hier_lvl5,
        'NA'::character varying AS prod_hier_lvl6,
        'NA'::character varying AS prod_hier_lvl7,
        'NA'::character varying AS prod_hier_lvl8,
        'NA'::character varying AS prod_hier_lvl9,
        tgt.store_type,
        'NA'::character varying AS account_classification,
        tgt.target_type,
        tgt.ytd_target_fy AS tgt_value
    FROM 
        (
            (
                (
                    (
                        SELECT itg_kr_tp_tracker_target.cntry_cd,
                            itg_kr_tp_tracker_target.country_name,
                            itg_kr_tp_tracker_target.crncy_cd,
                            itg_kr_tp_tracker_target.channel,
                            itg_kr_tp_tracker_target.store_type,
                            itg_kr_tp_tracker_target.sales_group_cd,
                            itg_kr_tp_tracker_target.sales_group_name,
                            itg_kr_tp_tracker_target.target_type,
                            itg_kr_tp_tracker_target.year,
                            itg_kr_tp_tracker_target.tgt_date,
                            itg_kr_tp_tracker_target.ytd_target_fy,
                            itg_kr_tp_tracker_target.tgt_value,
                            itg_kr_tp_tracker_target.filename,
                            itg_kr_tp_tracker_target.run_id,
                            itg_kr_tp_tracker_target.crtd_dttm,
                            itg_kr_tp_tracker_target.brand,
                            itg_kr_tp_tracker_target.target_category_code,
                            itg_kr_tp_tracker_target.target_category,
                            itg_kr_tp_tracker_target.target_amt,
                            itg_kr_tp_tracker_target.ytd_target_amt
                        FROM itg_kr_tp_tracker_target
                        WHERE (
                                (itg_kr_tp_tracker_target.target_category_code)::text = 'TP'::text
                            )
                    ) tgt
                    LEFT JOIN (
                        SELECT DISTINCT edw_intrm_calendar.cal_day,
                            edw_intrm_calendar.cal_mo_1,
                            edw_intrm_calendar.fisc_yr,
                            edw_intrm_calendar.fisc_per,
                            edw_intrm_calendar.pstng_per
                        FROM edw_intrm_calendar
                    ) cal ON ((tgt.tgt_date = cal.cal_day))
                )
                LEFT JOIN (
                    SELECT DISTINCT v_intrm_reg_crncy_exch_fiscper.ex_rt_typ,
                        v_intrm_reg_crncy_exch_fiscper.from_crncy,
                        "left"(
                            (
                                (v_intrm_reg_crncy_exch_fiscper.fisc_per)::character varying
                            )::text,
                            4
                        ) AS fisc_yr,
                        v_intrm_reg_crncy_exch_fiscper.to_crncy,
                        "max"(v_intrm_reg_crncy_exch_fiscper.ex_rt) AS ex_rt
                    FROM v_intrm_reg_crncy_exch_fiscper
                    WHERE (
                            (
                                upper(
                                    (v_intrm_reg_crncy_exch_fiscper.from_crncy)::text
                                ) = ('KRW'::character varying)::text
                            )
                            AND (
                                (
                                    (
                                        upper((v_intrm_reg_crncy_exch_fiscper.to_crncy)::text) = ('USD'::character varying)::text
                                    )
                                    OR (
                                        upper((v_intrm_reg_crncy_exch_fiscper.to_crncy)::text) = ('KRW'::character varying)::text
                                    )
                                )
                                OR (
                                    upper((v_intrm_reg_crncy_exch_fiscper.to_crncy)::text) = ('SGD'::character varying)::text
                                )
                            )
                        )
                    GROUP BY v_intrm_reg_crncy_exch_fiscper.ex_rt_typ,
                        v_intrm_reg_crncy_exch_fiscper.from_crncy,
                        "left"(
                            (
                                (v_intrm_reg_crncy_exch_fiscper.fisc_per)::character varying
                            )::text,
                            4
                        ),
                        v_intrm_reg_crncy_exch_fiscper.to_crncy
                ) exrt ON (
                    (
                        ((tgt.crncy_cd)::text = (exrt.from_crncy)::text)
                        AND (
                            ((cal.fisc_yr)::character varying)::text = exrt.fisc_yr
                        )
                    )
                )
            )
            LEFT JOIN (
                SELECT DISTINCT edw_customer_attr_flat_dim.sls_grp_cd,
                    edw_customer_attr_flat_dim.sls_ofc,
                    edw_customer_attr_flat_dim.sls_ofc_desc,
                    edw_customer_attr_flat_dim.store_typ,
                    edw_customer_attr_flat_dim.channel,
                    edw_customer_attr_flat_dim.sls_grp
                FROM edw_customer_attr_flat_dim
                WHERE (
                        upper((edw_customer_attr_flat_dim.county)::text) = ('KR'::character varying)::text
                    )
            ) cust ON (
                (
                    COALESCE(
                        upper(
                            trim(
                                (tgt.sales_group_cd)::text,
                                ('NA'::character varying)::text
                            )
                        )
                    ,'NA') = COALESCE(
                        upper(
                            trim(
                                (cust.sls_grp_cd)::text,
                                ('NA'::character varying)::text
                            )
                        )
                    ,'NA')
                )
            )
        )
),
final as 
(   
    SELECT 
        derived_table2.identifier,
        COALESCE(derived_table2.sls_org, 'NA'::character varying) AS sls_org,
        COALESCE(derived_table2.matl_num, 'NA'::character varying) AS matl_num,
        COALESCE(derived_table2.cust_num, 'NA'::character varying) AS cust_num,
        COALESCE(derived_table2.acct_num, 'NA'::character varying) AS acct_num,
        COALESCE(derived_table2.sls_ofc, 'NA'::character varying) AS sls_ofc,
        COALESCE(derived_table2.ctry_nm, 'NA'::character varying) AS ctry_nm,
        COALESCE(derived_table2.ctry_key, 'NA'::character varying) AS ctry_key,
        COALESCE(derived_table2.sls_grp, 'NA'::character varying) AS sls_grp,
        COALESCE(derived_table2.caln_day, '9999-12-31'::date) AS caln_day,
        COALESCE(derived_table2.caln_yr_mo, 999999) AS caln_yr_mo,
        COALESCE(derived_table2.fisc_yr, 9999) AS fisc_yr,
        COALESCE(derived_table2.fisc_yr_per, 9999999) AS fisc_yr_per,
        COALESCE(
            derived_table2.b3_base_prod,
            'NA'::character varying
        ) AS b3_base_prod,
        COALESCE(derived_table2.b4_var, 'NA'::character varying) AS b4_var,
        COALESCE(
            derived_table2.b5_put_up,
            'NA'::character varying
        ) AS b5_put_up,
        COALESCE(
            derived_table2.b1_mega_brnd,
            'NA'::character varying
        ) AS b1_mega_brnd,
        COALESCE(derived_table2.b2_brnd, 'NA'::character varying) AS b2_brnd,
        COALESCE(
            derived_table2.prod_minor,
            'NA'::character varying
        ) AS prod_minor,
        COALESCE(derived_table2.prod_maj, 'NA'::character varying) AS prod_maj,
        COALESCE(
            derived_table2.prod_fran,
            'NA'::character varying
        ) AS prod_fran,
        COALESCE(derived_table2.fran, 'NA'::character varying) AS fran,
        COALESCE(derived_table2.matl_sls, 'NA'::character varying) AS matl_sls,
        COALESCE(
            derived_table2.prod_hier,
            'NA'::character varying
        ) AS prod_hier,
        COALESCE(
            derived_table2.amt_obj_crncy,
            '0'::character varying
        ) AS amt_obj_crncy,
        COALESCE(
            derived_table2.nts_actuals,
            ((0)::numeric)::numeric(18, 0)
        ) AS nts_actuals,
        COALESCE(
            derived_table2.tp_actuals,
            ((0)::numeric)::numeric(18, 0)
        ) AS tp_actuals,
        COALESCE(
            derived_table2.nts_qty,
            ((0)::numeric)::numeric(18, 0)
        ) AS nts_qty,
        COALESCE(
            derived_table2.tp_qty,
            ((0)::numeric)::numeric(18, 0)
        ) AS tp_qty,
        COALESCE(
            derived_table2.sls_grp_desc,
            'NA'::character varying
        ) AS sls_grp_desc,
        COALESCE(
            derived_table2.sls_ofc_desc,
            'NA'::character varying
        ) AS sls_ofc_desc,
        COALESCE(
            derived_table2.matl_desc,
            'NA'::character varying
        ) AS matl_desc,
        COALESCE(
            derived_table2.mega_brnd_desc,
            'NA'::character varying
        ) AS mega_brnd_desc,
        COALESCE(
            derived_table2.brnd_desc,
            'NA'::character varying
        ) AS brnd_desc,
        COALESCE(
            derived_table2.varnt_desc,
            'NA'::character varying
        ) AS varnt_desc,
        COALESCE(
            derived_table2.base_prod_desc,
            'NA'::character varying
        ) AS base_prod_desc,
        COALESCE(
            derived_table2.put_up_desc,
            'NA'::character varying
        ) AS put_up_desc,
        COALESCE(derived_table2.channel, 'NA'::character varying) AS channel,
        COALESCE(derived_table2.med_desc, 'NA'::character varying) AS med_desc,
        COALESCE(
            derived_table2.edw_cust_nm,
            'NA'::character varying
        ) AS edw_cust_nm,
        COALESCE(
            derived_table2.from_crncy,
            'NA'::character varying
        ) AS from_crncy,
        COALESCE(derived_table2.to_crncy, 'NA'::character varying) AS to_crncy,
        COALESCE(
            derived_table2.ex_rt_typ,
            'NA'::character varying
        ) AS ex_rt_typ,
        COALESCE(
            derived_table2.ex_rt,
            ((0)::numeric)::numeric(18, 0)
        ) AS ex_rt,
        COALESCE(
            derived_table2.acct_hier_desc,
            'NA'::character varying
        ) AS acct_hier_desc,
        COALESCE(
            derived_table2.acct_hier_shrt_desc,
            'NA'::character varying
        ) AS acct_hier_shrt_desc,
        COALESCE(
            derived_table2.company_nm,
            'NA'::character varying
        ) AS company_nm,
        COALESCE(derived_table2.ean_num, 'NA'::character varying) AS ean_num,
        COALESCE(
            derived_table2.prod_hier_lvl1,
            'NA'::character varying
        ) AS prod_hier_lvl1,
        COALESCE(
            derived_table2.prod_hier_lvl2,
            'NA'::character varying
        ) AS prod_hier_lvl2,
        COALESCE(
            derived_table2.prod_hier_lvl3,
            'NA'::character varying
        ) AS prod_hier_lvl3,
        COALESCE(
            derived_table2.prod_hier_lvl4,
            'NA'::character varying
        ) AS prod_hier_lvl4,
        COALESCE(
            derived_table2.prod_hier_lvl5,
            'NA'::character varying
        ) AS prod_hier_lvl5,
        COALESCE(
            derived_table2.prod_hier_lvl6,
            'NA'::character varying
        ) AS prod_hier_lvl6,
        COALESCE(
            derived_table2.prod_hier_lvl7,
            'NA'::character varying
        ) AS prod_hier_lvl7,
        COALESCE(
            derived_table2.prod_hier_lvl8,
            'NA'::character varying
        ) AS prod_hier_lvl8,
        COALESCE(
            derived_table2.prod_hier_lvl9,
            'NA'::character varying
        ) AS prod_hier_lvl9,
        COALESCE(
            derived_table2.store_type,
            'NA'::character varying
        ) AS store_type,
        COALESCE(
            derived_table2.account_classification,
            'NA'::character varying
        ) AS account_classification,
        COALESCE(
            derived_table2.target_type,
            'NA'::character varying
        ) AS target_type,
        COALESCE(
            derived_table2.tgt_value,
            ((0)::numeric)::numeric(18, 0)
        ) AS tgt_value_per,
        CASE
            WHEN (
                (
                    (
                        (derived_table2.identifier)::text = ('TP_TARGET'::character varying)::text
                    )
                    AND (derived_table2.sls_grp IS NULL)
                )
                AND (derived_table2.store_type IS NULL)
            ) THEN (
                COALESCE(
                    sum(derived_table2.tgt_value) OVER(
                        PARTITION BY derived_table2.identifier,
                        derived_table2.fisc_yr_per,
                        derived_table2.target_type,
                        derived_table2.to_crncy,
                        derived_table2.channel,
                        derived_table2.store_type,
                        derived_table2.sls_grp order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ),
                    ((0)::numeric)::numeric(18, 0)
                ) * COALESCE(
                    sum(derived_table2.nts_actuals) OVER(
                        PARTITION BY derived_table2.fisc_yr_per,
                        derived_table2.to_crncy,
                        derived_table2.channel order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ),
                    ((0)::numeric)::numeric(18, 0)
                )
            )
            WHEN (
                (
                    (derived_table2.identifier)::text = ('TP_TARGET'::character varying)::text
                )
                AND (derived_table2.sls_grp IS NULL)
            ) THEN (
                COALESCE(
                    sum(derived_table2.tgt_value) OVER(
                        PARTITION BY derived_table2.identifier,
                        derived_table2.fisc_yr_per,
                        derived_table2.target_type,
                        derived_table2.to_crncy,
                        derived_table2.channel,
                        derived_table2.store_type,
                        derived_table2.sls_grp order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ),
                    ((0)::numeric)::numeric(18, 0)
                ) * COALESCE(
                    sum(derived_table2.nts_actuals) OVER(
                        PARTITION BY derived_table2.fisc_yr_per,
                        derived_table2.to_crncy,
                        derived_table2.channel,
                        derived_table2.store_type order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ),
                    ((0)::numeric)::numeric(18, 0)
                )
            )
            WHEN (
                (derived_table2.identifier)::text = ('TP_TARGET'::character varying)::text
            ) THEN (
                COALESCE(
                    sum(derived_table2.tgt_value) OVER(
                        PARTITION BY derived_table2.identifier,
                        derived_table2.fisc_yr_per,
                        derived_table2.target_type,
                        derived_table2.to_crncy,
                        derived_table2.channel,
                        derived_table2.store_type,
                        derived_table2.sls_grp order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ),
                    ((0)::numeric)::numeric(18, 0)
                ) * COALESCE(
                    sum(derived_table2.nts_actuals) OVER(
                        PARTITION BY derived_table2.fisc_yr_per,
                        derived_table2.to_crncy,
                        derived_table2.channel,
                        derived_table2.store_type,
                        derived_table2.sls_grp order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ),
                    ((0)::numeric)::numeric(18, 0)
                )
            )
            WHEN (
                (
                    (
                        (derived_table2.identifier)::text = ('TP_TARGET_YTD'::character varying)::text
                    )
                    AND (derived_table2.sls_grp IS NULL)
                )
                AND (derived_table2.store_type IS NULL)
            ) THEN (
                COALESCE(
                    sum(derived_table2.tgt_value) OVER(
                        PARTITION BY derived_table2.identifier,
                        derived_table2.fisc_yr,
                        derived_table2.target_type,
                        derived_table2.to_crncy,
                        derived_table2.channel,
                        derived_table2.store_type,
                        derived_table2.sls_grp order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ),
                    ((0)::numeric)::numeric(18, 0)
                ) * COALESCE(
                    sum(derived_table2.nts_actuals) OVER(
                        PARTITION BY derived_table2.fisc_yr,
                        derived_table2.to_crncy,
                        derived_table2.channel order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ),
                    ((0)::numeric)::numeric(18, 0)
                )
            )
            WHEN (
                (
                    (derived_table2.identifier)::text = ('TP_TARGET_YTD'::character varying)::text
                )
                AND (derived_table2.sls_grp IS NULL)
            ) THEN (
                COALESCE(
                    sum(derived_table2.tgt_value) OVER(
                        PARTITION BY derived_table2.identifier,
                        derived_table2.fisc_yr,
                        derived_table2.target_type,
                        derived_table2.to_crncy,
                        derived_table2.channel,
                        derived_table2.store_type,
                        derived_table2.sls_grp order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ),
                    ((0)::numeric)::numeric(18, 0)
                ) * COALESCE(
                    sum(derived_table2.nts_actuals) OVER(
                        PARTITION BY derived_table2.fisc_yr,
                        derived_table2.to_crncy,
                        derived_table2.channel,
                        derived_table2.store_type order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ),
                    ((0)::numeric)::numeric(18, 0)
                )
            )
            WHEN (
                (derived_table2.identifier)::text = ('TP_TARGET_YTD'::character varying)::text
            ) THEN (
                COALESCE(
                    sum(derived_table2.tgt_value) OVER(
                        PARTITION BY derived_table2.identifier,
                        derived_table2.fisc_yr,
                        derived_table2.target_type,
                        derived_table2.to_crncy,
                        derived_table2.channel,
                        derived_table2.store_type,
                        derived_table2.sls_grp order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ),
                    ((0)::numeric)::numeric(18, 0)
                ) * COALESCE(
                    sum(derived_table2.nts_actuals) OVER(
                        PARTITION BY derived_table2.fisc_yr,
                        derived_table2.to_crncy,
                        derived_table2.channel,
                        derived_table2.store_type,
                        derived_table2.sls_grp  order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ),
                    ((0)::numeric)::numeric(18, 0)
                )
            )
            ELSE ((0)::numeric)::numeric(18, 0)
        END AS tgt_value
    FROM 
        (
            SELECT 
                derived_table1.identifier,
                derived_table1.sls_org,
                derived_table1.matl_num,
                derived_table1.cust_num,
                derived_table1.acct_num,
                derived_table1.sls_ofc,
                derived_table1.ctry_nm,
                derived_table1.ctry_key,
                derived_table1.sls_grp,
                derived_table1.caln_day,
                derived_table1.caln_yr_mo,
                derived_table1.fisc_yr,
                derived_table1.fisc_yr_per,
                derived_table1.b3_base_prod,
                derived_table1.b4_var,
                derived_table1.b5_put_up,
                derived_table1.b1_mega_brnd,
                derived_table1.b2_brnd,
                derived_table1.prod_minor,
                derived_table1.prod_maj,
                derived_table1.prod_fran,
                derived_table1.fran,
                derived_table1.matl_sls,
                derived_table1.prod_hier,
                '0'::character varying AS amt_obj_crncy,
                sum(
                    COALESCE(
                        derived_table1.nts_actuals,
                        ((0)::numeric)::numeric(18, 0)
                    )
                ) AS nts_actuals,
                sum(
                    COALESCE(
                        derived_table1.tp_actuals,
                        ((0)::numeric)::numeric(18, 0)
                    )
                ) AS tp_actuals,
                sum(
                    COALESCE(
                        derived_table1.nts_qty,
                        ((0)::numeric)::numeric(18, 0)
                    )
                ) AS nts_qty,
                sum(
                    COALESCE(
                        derived_table1.tp_qty,
                        ((0)::numeric)::numeric(18, 0)
                    )
                ) AS tp_qty,
                derived_table1.sls_grp_desc,
                derived_table1.sls_ofc_desc,
                derived_table1.matl_desc,
                derived_table1.mega_brnd_desc,
                derived_table1.brnd_desc,
                derived_table1.varnt_desc,
                derived_table1.base_prod_desc,
                derived_table1.put_up_desc,
                derived_table1.channel,
                derived_table1.med_desc,
                derived_table1.edw_cust_nm,
                derived_table1.from_crncy,
                derived_table1.to_crncy,
                derived_table1.ex_rt_typ,
                derived_table1.ex_rt,
                derived_table1.acct_hier_desc,
                derived_table1.acct_hier_shrt_desc,
                derived_table1.company_nm,
                derived_table1.ean_num,
                derived_table1.prod_hier_lvl1,
                derived_table1.prod_hier_lvl2,
                derived_table1.prod_hier_lvl3,
                derived_table1.prod_hier_lvl4,
                derived_table1.prod_hier_lvl5,
                derived_table1.prod_hier_lvl6,
                derived_table1.prod_hier_lvl7,
                derived_table1.prod_hier_lvl8,
                derived_table1.prod_hier_lvl9,
                derived_table1.store_type,
                derived_table1.account_classification,
                derived_table1.target_type,
                sum(
                    COALESCE(
                        derived_table1.tgt_value,
                        ((0)::numeric)::numeric(18, 0)
                    )
                ) AS tgt_value
            FROM 
            (
                select * from nts_actuals
                union all
                select * from tp_actuals
                union all
                select * from tp_target
                union all
                select * from tp_target_ytd
            ) derived_table1
            WHERE (
                    (derived_table1.fisc_yr)::double precision >= 
                    (
                        date_part(
                            year,
                            current_timestamp::timestamp without time zone
                        ) - 
                        (
                            (
                                SELECT (itg_query_parameters_rg.parameter_value)::integer AS parameter_value
                                FROM itg_query_parameters_rg
                                WHERE (
                                        (
                                            upper((itg_query_parameters_rg.country_code)::text) = ('KR'::character varying)::text
                                        )
                                        AND (
                                            upper((itg_query_parameters_rg.parameter_name)::text) = (
                                                'KOREA_TP_TRACKER_TDE_DATA_RETENTION_YEARS'::character varying
                                            )::text
                                        )
                                    )
                            )
                        )::double precision
                    )
                )
            GROUP BY derived_table1.identifier,
                derived_table1.sls_org,
                derived_table1.matl_num,
                derived_table1.cust_num,
                derived_table1.acct_num,
                derived_table1.sls_ofc,
                derived_table1.ctry_nm,
                derived_table1.ctry_key,
                derived_table1.sls_grp,
                derived_table1.caln_day,
                derived_table1.caln_yr_mo,
                derived_table1.fisc_yr,
                derived_table1.fisc_yr_per,
                derived_table1.b3_base_prod,
                derived_table1.b4_var,
                derived_table1.b5_put_up,
                derived_table1.b1_mega_brnd,
                derived_table1.b2_brnd,
                derived_table1.prod_minor,
                derived_table1.prod_maj,
                derived_table1.prod_fran,
                derived_table1.fran,
                derived_table1.matl_sls,
                derived_table1.prod_hier,
                derived_table1.sls_grp_desc,
                derived_table1.sls_ofc_desc,
                derived_table1.matl_desc,
                derived_table1.mega_brnd_desc,
                derived_table1.brnd_desc,
                derived_table1.varnt_desc,
                derived_table1.base_prod_desc,
                derived_table1.put_up_desc,
                derived_table1.channel,
                derived_table1.med_desc,
                derived_table1.edw_cust_nm,
                derived_table1.from_crncy,
                derived_table1.to_crncy,
                derived_table1.ex_rt_typ,
                derived_table1.ex_rt,
                derived_table1.acct_hier_desc,
                derived_table1.acct_hier_shrt_desc,
                derived_table1.company_nm,
                derived_table1.ean_num,
                derived_table1.prod_hier_lvl1,
                derived_table1.prod_hier_lvl2,
                derived_table1.prod_hier_lvl3,
                derived_table1.prod_hier_lvl4,
                derived_table1.prod_hier_lvl5,
                derived_table1.prod_hier_lvl6,
                derived_table1.prod_hier_lvl7,
                derived_table1.prod_hier_lvl8,
                derived_table1.prod_hier_lvl9,
                derived_table1.store_type,
                derived_table1.account_classification,
                derived_table1.target_type
        ) derived_table2
)
select * from final