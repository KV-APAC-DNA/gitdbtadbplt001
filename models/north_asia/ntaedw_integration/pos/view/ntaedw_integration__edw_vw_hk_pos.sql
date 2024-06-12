with rpt_regional_scorecard as 
(
    select * from aspedw_integration.rpt_regional_scorecard
),
edw_vw_greenlight_skus as 
(
    select * from aspedw_integration.edw_vw_greenlight_skus
),
v_edw_customer_sales_dim as 
(
    select * from aspedw_integration.v_edw_customer_sales_dim
),
edw_gch_producthierarchy as 
(
    select * from aspedw_integration.edw_gch_producthierarchy
),
v_intrm_reg_crncy_exch_fiscper as 
(
    select * from aspedw_integration.v_intrm_reg_crncy_exch_fiscper
),
edw_copa_trans_fact as 
(
    select * from aspedw_integration.edw_copa_trans_fact
),
edw_vw_promo_calendar as 
(
    select * from ntaedw_integration.edw_vw_promo_calendar
),
edw_intrm_calendar as 
(
    select * from ntaedw_integration.edw_intrm_calendar
),
edw_pos_fact as 
(
    select * from ntaedw_integration.edw_pos_fact
),
itg_mds_hk_ref_pos_accounts as 
(
    select * from ntaitg_integration.itg_mds_hk_ref_pos_accounts
),
itg_mds_hk_pos_product_mapping as 
(
    select * from ntaitg_integration.itg_mds_hk_pos_product_mapping
),
itg_query_parameters as 
(
    select * from ntaitg_integration.itg_query_parameters
),
exch_rate as 
(
    SELECT 
        exch_rate.from_crncy,
        exch_rate.fisc_per,
        sum(
            CASE
                WHEN ((exch_rate.to_crncy)::text = ('USD'::character varying)::text) THEN exch_rate.ex_rt
                ELSE ((0)::numeric)::numeric(18, 0)
            END
        ) AS usd_rt,
        sum(
            CASE
                WHEN ((exch_rate.to_crncy)::text = ('HKD'::character varying)::text) THEN exch_rate.ex_rt
                ELSE ((0)::numeric)::numeric(18, 0)
            END
        ) AS lcl_rt
    FROM v_intrm_reg_crncy_exch_fiscper exch_rate
    WHERE 
        (
            (
                (exch_rate.from_crncy)::text = ('HKD'::character varying)::text
            )
            AND (
                (
                    (exch_rate.to_crncy)::text = ('USD'::character varying)::text
                )
                OR (
                    (exch_rate.to_crncy)::text = ('HKD'::character varying)::text
                )
            )
        )
    GROUP BY exch_rate.from_crncy,
        exch_rate.fisc_per
),
pos as 
(
    SELECT 
        'POS'::character varying AS subsource_type,
        a.src_sys_cd AS customer,
        a.vend_nm AS vendor_desc,
        a.vend_prod_cd AS customer_product_code,
        b.jnj_sku_code,
        CASE
            WHEN ((b.jnj_sku_code)::text = ('NA'::character varying)::text) THEN 'NA'::character varying
            ELSE gls.pka_product_key
        END AS pka_product_key,
        CASE
            WHEN ((b.jnj_sku_code)::text = ('NA'::character varying)::text) THEN 'NA'::character varying
            ELSE gls.pka_product_key_description
        END AS pka_product_key_description,
        CASE
            WHEN ((b.jnj_sku_code)::text = ('NA'::character varying)::text) THEN 'NA'::character varying
            ELSE gcph.gcph_franchise
        END AS stronghold,
        CASE
            WHEN ((b.jnj_sku_code)::text = ('NA'::character varying)::text) THEN 'NA'::character varying
            ELSE gcph.gcph_needstate
        END AS gcph_needstate,
        b.category_name,
        b.age_group_name,
        CASE
            WHEN ((b.jnj_sku_code)::text = ('NA'::character varying)::text) THEN 'NA'::character varying
            ELSE gcph.gcph_brand
        END AS gcph_brand,
        CASE
            WHEN ((b.jnj_sku_code)::text = ('NA'::character varying)::text) THEN 'NA'::character varying
            ELSE gls.mega_brnd_desc
        END AS mega_brnd_desc,
        CASE
            WHEN ((b.jnj_sku_code)::text = ('NA'::character varying)::text) THEN 'NA'::character varying
            ELSE gls.pka_package_desc
        END AS package,
        CASE
            WHEN ((b.jnj_sku_code)::text = ('NA'::character varying)::text) THEN 'NA'::character varying
            ELSE gls.pka_size_desc
        END AS size,
        COALESCE(gls.greenlight_sku_flag,'N/A'::character varying) AS greenlight_sku_flag,
        exch_rate.usd_rt,
        exch_rate.lcl_rt,
        cal.year,
        cal.fisc_per,
        cal.qrtr_no,
        (cal.qrtr)::character varying AS qrtr,
        (cal.mnth_id)::character varying AS mnth_id,
        (cal.mnth_desc)::character varying AS mnth_desc,
        cal.mnth_no,
        cal.mnth_shrt,
        cal.mnth_long,
        (cal.wk)::character varying AS wk,
        cal.mnth_wk_no,
        cal.fisc_week_day_num,
        cal.mnth_day,
        cal.cal_year,
        cal.cal_qrtr_no,
        cal.cal_mnth_id,
        cal.cal_mnth_no,
        cal.cal_mnth_nm,
        cal.cal_mnth_day,
        (cal.cal_mnth_wk_num)::character varying AS cal_mnth_wk_num,
        cal.cal_yr_wk_num,
        cal.cal_week_day_num,
        (cal.promo_week)::character varying AS promo_week,
        cal.promo_month_week_num,
        cal.promo_week_day_num,
        (cal.promo_month_shrt)::character varying AS promo_month_shrt,
        cal.promo_year,
        cal.cal_day,
        (cal.cal_date_id)::character varying AS cal_date_id,
        COALESCE(((a.sls_qty)::numeric)::numeric(18, 0),((0)::numeric)::numeric(18, 0)) AS pos_qty,
        COALESCE(a.sls_amt, ((0)::numeric)::numeric(18, 0)) AS pos_sales,
        ((0)::numeric)::numeric(18, 0) AS sellin_qty,
        ((0)::numeric)::numeric(18, 0) AS sellin_preww,
        ((0)::numeric)::numeric(18, 0) AS sellin_nts,
        a.prod_nm AS customer_description,
        ((0)::numeric)::numeric(18, 0) AS ciw_gts_lcy,
        ((0)::numeric)::numeric(18, 0) AS ciw_gts_usd,
        ((0)::numeric)::numeric(18, 0) AS ciw_lcy,
        ((0)::numeric)::numeric(18, 0) AS ciw_usd,
        ((0)::numeric)::numeric(18, 0) AS ciw_gts_prev_yr_mnth,
        ((0)::numeric)::numeric(18, 0) AS ciw_prev_yr_mnth,
        ((0)::numeric)::numeric(18, 0) AS ciw_gts_lcy_prev_yr_mnth,
        ((0)::numeric)::numeric(18, 0) AS ciw_lcy_prev_yr_mnth,
        ((0)::numeric)::numeric(18, 0) AS numerator,
        ((0)::numeric)::numeric(18, 0) AS denominator,
        ((0)::numeric)::numeric(18, 0) AS numerator_prev_yr,
        ((0)::numeric)::numeric(18, 0) AS denominator_prev_yr,
        ((0)::numeric)::numeric(18, 0) AS msl_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS msl_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS msl_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS osa_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS osa_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS osa_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS promo_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS promo_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS promo_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS display_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS display_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS display_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS planogram_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS planogram_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS planogram_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS sos_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS sos_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS sos_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS soa_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS soa_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS soa_complaince_denominator_wt
    FROM
        edw_pos_fact a
        LEFT JOIN itg_mds_hk_pos_product_mapping b ON 
        (
            (
                ((a.vend_prod_cd)::text = (b.prod_code)::text)
                AND ((a.src_sys_cd)::text = (b.customer_name)::text)
            )
        )
        LEFT JOIN 
        (
            SELECT DISTINCT ltrim(
                    (edw_gch_producthierarchy.materialnumber)::text,
                    ('0'::character varying)::text
                ) AS mat_num,
                edw_gch_producthierarchy.gcph_franchise,
                edw_gch_producthierarchy.gcph_needstate,
                edw_gch_producthierarchy.gcph_brand
            FROM edw_gch_producthierarchy
        ) gcph ON (((b.jnj_sku_code)::text = gcph.mat_num))
        LEFT JOIN (
            SELECT DISTINCT edw_vw_greenlight_skus.matl_num,
                edw_vw_greenlight_skus.matl_desc,
                edw_vw_greenlight_skus.pka_package_desc,
                edw_vw_greenlight_skus.pka_size_desc,
                edw_vw_greenlight_skus.mega_brnd_desc,
                edw_vw_greenlight_skus.greenlight_sku_flag,
                edw_vw_greenlight_skus.pka_product_key,
                edw_vw_greenlight_skus.pka_product_key_description
            FROM edw_vw_greenlight_skus
            WHERE (
                    (edw_vw_greenlight_skus.ctry_nm)::text = ('Hong Kong'::character varying)::text
                )
        ) gls ON (((b.jnj_sku_code)::text = gls.matl_num))
        LEFT JOIN edw_vw_promo_calendar cal ON ((a.pos_dt = cal.cal_day))
        LEFT JOIN 
        exch_rate ON 
        LEFT(cal.fisc_per::character varying::text, 4) || RIGHT(cal.fisc_per::character varying::text, 2) = 
        LEFT(exch_rate.fisc_per::character varying::text, 4) || RIGHT(exch_rate.fisc_per::character varying::text, 2)
    WHERE 
        (
            a.src_sys_cd IN (
                SELECT itg_mds_hk_ref_pos_accounts.name FROM itg_mds_hk_ref_pos_accounts
            )
        )
),
sellin_qty as 
(
    SELECT 
        'SELLIN_QTY'::character varying AS subsource_type,
        a.banner AS customer,
        NULL::character varying AS vendor_desc,
        b.prod_code AS customer_product_code,
        (
            ltrim(
                (a.matl_num)::text,
                ('0'::character varying)::text
            )
        )::character varying AS jnj_sku_code,
        gls.pka_product_key,
        gls.pka_product_key_description,
        gcph.gcph_franchise AS stronghold,
        gcph.gcph_needstate,
        b.category_name,
        b.age_group_name,
        gcph.gcph_brand,
        gls.mega_brnd_desc,
        gls.pka_package_desc AS package,
        gls.pka_size_desc AS size,
        COALESCE(
            gls.greenlight_sku_flag,
            'N/A'::character varying
        ) AS greenlight_sku_flag,
        exch_rate.usd_rt,
        exch_rate.lcl_rt,
        cal.year,
        cal.fisc_per,
        cal.qrtr_no,
        (cal.qrtr)::character varying AS qrtr,
        (cal.mnth_id)::character varying AS mnth_id,
        (cal.mnth_desc)::character varying AS mnth_desc,
        cal.mnth_no,
        cal.mnth_shrt,
        cal.mnth_long,
        (cal.wk)::character varying AS wk,
        cal.mnth_wk_no,
        cal.fisc_week_day_num,
        cal.mnth_day,
        cal.cal_year,
        cal.cal_qrtr_no,
        cal.cal_mnth_id,
        cal.cal_mnth_no,
        cal.cal_mnth_nm,
        cal.cal_mnth_day,
        (cal.cal_mnth_wk_num)::character varying AS cal_mnth_wk_num,
        cal.cal_yr_wk_num,
        cal.cal_week_day_num,
        (cal.promo_week)::character varying AS promo_week,
        cal.promo_month_week_num,
        cal.promo_week_day_num,
        (cal.promo_month_shrt)::character varying AS promo_month_shrt,
        cal.promo_year,
        cal.cal_day,
        (cal.cal_date_id)::character varying AS cal_date_id,
        ((0)::numeric)::numeric(18, 0) AS pos_qty,
        ((0)::numeric)::numeric(18, 0) AS pos_sales,
        (COALESCE(a.gts_qty, (0)::double precision))::numeric(18, 0) AS sellin_qty,
        ((0)::numeric)::numeric(18, 0) AS sellin_preww,
        ((0)::numeric)::numeric(18, 0) AS sellin_nts,
        null as customer_description,
        ((0)::numeric)::numeric(18, 0) AS ciw_gts_lcy,
        ((0)::numeric)::numeric(18, 0) AS ciw_gts_usd,
        ((0)::numeric)::numeric(18, 0) AS ciw_lcy,
        ((0)::numeric)::numeric(18, 0) AS ciw_usd,
        ((0)::numeric)::numeric(18, 0) AS ciw_gts_prev_yr_mnth,
        ((0)::numeric)::numeric(18, 0) AS ciw_prev_yr_mnth,
        ((0)::numeric)::numeric(18, 0) AS ciw_gts_lcy_prev_yr_mnth,
        ((0)::numeric)::numeric(18, 0) AS ciw_lcy_prev_yr_mnth,
        ((0)::numeric)::numeric(18, 0) AS numerator,
        ((0)::numeric)::numeric(18, 0) AS denominator,
        ((0)::numeric)::numeric(18, 0) AS numerator_prev_yr,
        ((0)::numeric)::numeric(18, 0) AS denominator_prev_yr,
        ((0)::numeric)::numeric(18, 0) AS msl_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS msl_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS msl_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS osa_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS osa_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS osa_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS promo_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS promo_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS promo_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS display_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS display_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS display_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS planogram_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS planogram_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS planogram_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS sos_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS sos_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS sos_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS soa_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS soa_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS soa_complaince_denominator_wt
    FROM 
        (
            SELECT 
                copa.fisc_yr_per,
                copa.matl_num,
                copa.sls_org,
                copa.div,
                copa.dstr_chnl,
                copa.cust_num,
                cust_sales.banner,
                copa.caln_day,
                sum(
                    CASE
                        WHEN (
                            (copa.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
                        ) THEN COALESCE(
                            (copa.qty)::double precision,
                            (0)::double precision
                        )
                        ELSE (((0)::numeric)::numeric(18, 0))::double precision
                    END
                ) AS gts_qty
            FROM edw_copa_trans_fact copa,
                (
                    SELECT 
                        DISTINCT v_edw_customer_sales_dim.cust_num,
                        v_edw_customer_sales_dim."banner format" AS banner
                    FROM v_edw_customer_sales_dim
                    WHERE 
                        v_edw_customer_sales_dim.sls_org IN 
                        (
                            SELECT itg_query_parameters.parameter_value
                            FROM itg_query_parameters
                            WHERE itg_query_parameters.country_code::text = 'HK'::text
                            AND itg_query_parameters.parameter_name::text = 'copa'::text
                            AND itg_query_parameters.parameter_type::text = 'sls_org'::text
                        )
                        AND v_edw_customer_sales_dim."banner format" IN 
                        (
                            SELECT itg_mds_hk_ref_pos_accounts.name FROM itg_mds_hk_ref_pos_accounts
                        )

                ) cust_sales
            WHERE 
            copa.sls_org IN 
            (
                SELECT itg_query_parameters.parameter_value
                FROM itg_query_parameters
                WHERE itg_query_parameters.country_code::text = 'HK'::text
                AND itg_query_parameters.parameter_name::text = 'copa'::text
                AND itg_query_parameters.parameter_type::text = 'sls_org'::text
            )
            AND copa.cust_num::text = cust_sales.cust_num::text
            AND copa.fisc_yr::numeric(18, 0) >= 
            (
                SELECT date_part(year, convert_timezone('UTC', current_timestamp())::timestamp without time zone)::numeric(18, 0) - itg_query_parameters.parameter_value::numeric(18, 0)
                FROM itg_query_parameters
                WHERE itg_query_parameters.country_code::text = 'HK'::text
                AND itg_query_parameters.parameter_name::text = 'HK_POS_DataMart_Data_Retention_Years'::text
            )
            AND copa.caln_day IS NOT NULL
            GROUP BY copa.fisc_yr_per,
                copa.matl_num,
                copa.sls_org,
                copa.div,
                copa.dstr_chnl,
                copa.cust_num,
                cust_sales.banner,
                copa.caln_day
        ) a
        LEFT JOIN itg_mds_hk_pos_product_mapping b ON 
        (
            (
                ltrim(
                    (a.matl_num)::text,
                    ('0'::character varying)::text
                ) = (b.jnj_sku_code)::text
            )
        )
        LEFT JOIN 
        (
            SELECT DISTINCT ltrim(
                    (edw_gch_producthierarchy.materialnumber)::text,
                    ('0'::character varying)::text
                ) AS mat_num,
                edw_gch_producthierarchy.gcph_franchise,
                edw_gch_producthierarchy.gcph_needstate,
                edw_gch_producthierarchy.gcph_brand
            FROM edw_gch_producthierarchy
        ) gcph ON 
        (
            (
                ltrim(
                    (a.matl_num)::text,
                    ('0'::character varying)::text
                ) = gcph.mat_num
            )
        )
        LEFT JOIN 
        (
            SELECT DISTINCT edw_vw_greenlight_skus.matl_num,
                edw_vw_greenlight_skus.matl_desc,
                edw_vw_greenlight_skus.pka_package_desc,
                edw_vw_greenlight_skus.pka_size_desc,
                edw_vw_greenlight_skus.mega_brnd_desc,
                edw_vw_greenlight_skus.greenlight_sku_flag,
                edw_vw_greenlight_skus.pka_product_key,
                edw_vw_greenlight_skus.pka_product_key_description
            FROM edw_vw_greenlight_skus
            WHERE (
                    (edw_vw_greenlight_skus.ctry_nm)::text = ('Hong Kong'::character varying)::text
                )
        ) gls ON 
        (
            (
                ltrim(
                    (a.matl_num)::text,
                    ('0'::character varying)::text
                ) = gls.matl_num
            )
        )
        LEFT JOIN edw_vw_promo_calendar cal ON 
        (
            (
                to_date(
                    (a.caln_day)::text,
                    ('yyyymmdd'::character varying)::text
                ) = to_date(
                    ((cal.cal_day)::character varying)::text,
                    ('yyyy-mm-dd'::character varying)::text
                )
            )
        )
        LEFT JOIN 
        exch_rate ON
        LEFT(cal.fisc_per::character varying::text, 4) || RIGHT(cal.fisc_per::character varying::text, 2) = 
        LEFT(exch_rate.fisc_per::character varying::text, 4) || RIGHT(exch_rate.fisc_per::character varying::text, 2)

),
preww_nts as 
(
    SELECT 
        'PREWW_NTS'::character varying AS subsource_type,
        base.banner AS customer,
        NULL::character varying AS vendor_desc,
        base.prod_code AS customer_product_code,
        (
            ltrim(
                (base.matl_num)::text,
                ('0'::character varying)::text
            )
        )::character varying AS jnj_sku_code,
        base.pka_product_key,
        base.pka_product_key_description,
        base.gcph_franchise AS stronghold,
        base.gcph_needstate,
        base.category_name,
        base.age_group_name,
        base.gcph_brand,
        base.mega_brnd_desc,
        base.pka_package_desc AS package,
        base.pka_size_desc AS size,
        COALESCE(
            base.greenlight_sku_flag,
            'N/A'::character varying
        ) AS greenlight_sku_flag,
        exch_rate.usd_rt,
        exch_rate.lcl_rt,
        cal.year,
        cal.fisc_per,
        cal.qrtr_no,
        (cal.qrtr)::character varying AS qrtr,
        (cal.mnth_id)::character varying AS mnth_id,
        (cal.mnth_desc)::character varying AS mnth_desc,
        cal.mnth_no,
        cal.mnth_shrt,
        cal.mnth_long,
        NULL::character varying AS wk,
        NULL::integer AS mnth_wk_no,
        NULL::integer AS fisc_week_day_num,
        NULL::integer AS mnth_day,
        NULL::integer AS cal_year,
        NULL::integer AS cal_qrtr_no,
        NULL::integer AS cal_mnth_id,
        NULL::integer AS cal_mnth_no,
        NULL::character varying AS cal_mnth_nm,
        NULL::integer AS cal_mnth_day,
        NULL::character varying AS cal_mnth_wk_num,
        NULL::character varying AS cal_yr_wk_num,
        NULL::integer AS cal_week_day_num,
        NULL::character varying AS promo_week,
        NULL::integer AS promo_month_week_num,
        NULL::integer AS promo_week_day_num,
        NULL::character varying AS promo_month_shrt,
        NULL::integer AS promo_year,
        NULL::date AS cal_day,
        NULL::character varying AS cal_date_id,
        ((0)::numeric)::numeric(18, 0) AS pos_qty,
        ((0)::numeric)::numeric(18, 0) AS pos_sales,
        ((0)::numeric)::numeric(18, 0) AS sellin_qty,
        (base.preww7_act)::numeric(18, 0) AS sellin_preww,
        (
            COALESCE(base.preww7_act, (0)::double precision) - (
                (COALESCE(base.tp_act, (0)::double precision))::numeric(18, 0)
            )::double precision
        ) AS sellin_nts,
        null as customer_description,
        ((0)::numeric)::numeric(18, 0) AS ciw_gts_lcy,
        ((0)::numeric)::numeric(18, 0) AS ciw_gts_usd,
        ((0)::numeric)::numeric(18, 0) AS ciw_lcy,
        ((0)::numeric)::numeric(18, 0) AS ciw_usd,
        ((0)::numeric)::numeric(18, 0) AS ciw_gts_prev_yr_mnth,
        ((0)::numeric)::numeric(18, 0) AS ciw_prev_yr_mnth,
        ((0)::numeric)::numeric(18, 0) AS ciw_gts_lcy_prev_yr_mnth,
        ((0)::numeric)::numeric(18, 0) AS ciw_lcy_prev_yr_mnth,
        ((0)::numeric)::numeric(18, 0) AS numerator,
        ((0)::numeric)::numeric(18, 0) AS denominator,
        ((0)::numeric)::numeric(18, 0) AS numerator_prev_yr,
        ((0)::numeric)::numeric(18, 0) AS denominator_prev_yr,
        ((0)::numeric)::numeric(18, 0) AS msl_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS msl_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS msl_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS osa_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS osa_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS osa_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS promo_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS promo_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS promo_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS display_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS display_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS display_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS planogram_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS planogram_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS planogram_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS sos_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS sos_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS sos_complaince_denominator_wt,
        ((0)::numeric)::numeric(18, 0) AS soa_complaince_numerator,
        ((0)::numeric)::numeric(18, 0) AS soa_complaince_denominator,
        ((0)::numeric)::numeric(18, 0) AS soa_complaince_denominator_wt
    FROM 
        (
            SELECT copa.fisc_yr_per,
                copa.cust_num,
                copa.banner,
                copa.matl_num,
                pos_prod_master.prod_code,
                gls.pka_product_key,
                gls.pka_product_key_description,
                gcph.gcph_franchise,
                gcph.gcph_needstate,
                pos_prod_master.category_name,
                pos_prod_master.age_group_name,
                gcph.gcph_brand,
                gls.mega_brnd_desc,
                gls.pka_package_desc,
                gls.pka_size_desc,
                gls.greenlight_sku_flag,
                (
                    round(sum(copa.gts_act), ((2)::numeric)::numeric(18, 0)) + round(
                        COALESCE(sum(copa.preww_d),null),
                        ((2)::numeric)::numeric(18, 0)
                    )
                ) AS preww7_act,
                (sum(copa.tp_act) * (- (1)::double precision)) AS tp_act
            FROM
                (
                    SELECT 
                        copa.fisc_yr_per,
                        copa.matl_num,
                        copa.sls_org,
                        copa.div,
                        copa.dstr_chnl,
                        copa.cust_num,
                        cust_sales.banner,
                        copa.caln_day,
                        sum
                        (
                            CASE
                                WHEN ((copa.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
                                ) THEN COALESCE((copa.amt_obj_crncy)::double precision,(0)::double precision)
                                ELSE (((0)::numeric)::numeric(18, 0))::double precision
                            END
                        ) AS gts_act,
                        sum
                        (
                            CASE
                                WHEN copa.acct_hier_shrt_desc::text = 'NTS'::text
                                AND ltrim(copa.acct_num::text, '0'::text)::character varying(300) IN (
                                    SELECT itg_query_parameters.parameter_value
                                    FROM itg_query_parameters
                                    WHERE itg_query_parameters.country_code::text = 'HK'::text
                                    AND itg_query_parameters.parameter_name::text = 'PreWW'::text
                                    AND itg_query_parameters.parameter_type::text = 'GL_Account'::text
                                )
                                THEN COALESCE(copa.amt_obj_crncy::double precision, 0::double precision)
                                ELSE 0::double precision
                            END
                        ) AS preww_d,
                        sum
                        (
                            CASE
                                WHEN copa.acct_hier_shrt_desc::text = 'NTS'::text
                                AND ltrim(copa.acct_num::text, '0'::text)::character varying(300) IN (
                                    SELECT itg_query_parameters.parameter_value
                                    FROM itg_query_parameters
                                    WHERE itg_query_parameters.country_code::text = 'HK'::text
                                    AND itg_query_parameters.parameter_name::text = 'TP'::text
                                    AND itg_query_parameters.parameter_type::text = 'GL_Account'::text
                                )
                                THEN COALESCE(copa.amt_obj_crncy::double precision, 0::double precision)
                                ELSE 0::double precision
                            END
                        ) AS tp_act
                    FROM edw_copa_trans_fact copa,
                        (
                            SELECT DISTINCT v_edw_customer_sales_dim.cust_num,
                                v_edw_customer_sales_dim."banner format" AS banner
                            FROM v_edw_customer_sales_dim
                            WHERE 
                                (
                                    (
                                        v_edw_customer_sales_dim.sls_org IN (
                                            SELECT itg_query_parameters.parameter_value
                                            FROM itg_query_parameters
                                            WHERE (
                                                    (
                                                        (
                                                            (itg_query_parameters.country_code)::text = ('HK'::character varying)::text
                                                        )
                                                        AND (
                                                            (itg_query_parameters.parameter_name)::text = ('copa'::character varying)::text
                                                        )
                                                    )
                                                    AND (
                                                        (itg_query_parameters.parameter_type)::text = ('sls_org'::character varying)::text
                                                    )
                                                )
                                        )
                                    )
                                    AND (
                                        v_edw_customer_sales_dim."banner format" IN (
                                            SELECT itg_mds_hk_ref_pos_accounts.name
                                            FROM itg_mds_hk_ref_pos_accounts
                                        )
                                    )
                                )
                        ) cust_sales
                    WHERE 
                        (
                            (
                                (
                                    (
                                        (
                                            copa.sls_org IN (
                                                SELECT itg_query_parameters.parameter_value
                                                FROM itg_query_parameters
                                                WHERE (
                                                        (
                                                            (
                                                                (itg_query_parameters.country_code)::text = ('HK'::character varying)::text
                                                            )
                                                            AND (
                                                                (itg_query_parameters.parameter_name)::text = ('copa'::character varying)::text
                                                            )
                                                        )
                                                        AND (
                                                            (itg_query_parameters.parameter_type)::text = ('sls_org'::character varying)::text
                                                        )
                                                    )
                                            )
                                        )
                                        AND (
                                            (copa.cust_num)::text = (cust_sales.cust_num)::text
                                        )
                                    )
                                    AND (
                                        ((copa.fisc_yr)::numeric)::numeric(18, 0) >= 
                                        (
                                            SELECT (
                                                    (
                                                        (
                                                            "date_part"(
                                                                year,
                                                                (convert_timezone('UTC', current_timestamp())::character varying)::timestamp without time zone
                                                            )
                                                        )::numeric
                                                    )::numeric(18, 0) - (itg_query_parameters.parameter_value)::numeric(18, 0)
                                                )
                                            FROM itg_query_parameters
                                            WHERE (
                                                    (
                                                        (itg_query_parameters.country_code)::text = ('HK'::character varying)::text
                                                    )
                                                    AND (
                                                        (itg_query_parameters.parameter_name)::text = (
                                                            'HK_POS_DataMart_Data_Retention_Years'::character varying
                                                        )::text
                                                    )
                                                )
                                        )
                                    )
                                )
                                AND (
                                    copa.fisc_yr_per < 
                                    (
                                        SELECT DISTINCT edw_intrm_calendar.fisc_per FROM edw_intrm_calendar
                                        WHERE (
                                                edw_intrm_calendar.cal_day = to_date(
                                                    (convert_timezone('UTC', current_timestamp())::character varying)::timestamp without time zone
                                                )
                                            )
                                    )
                                )
                            )
                            AND (copa.caln_day IS NOT NULL)
                        )
                    GROUP BY copa.fisc_yr_per,
                        copa.matl_num,
                        copa.sls_org,
                        copa.div,
                        copa.dstr_chnl,
                        copa.cust_num,
                        cust_sales.banner,
                        copa.caln_day
                    HAVING 
                    sum
                    (
                        CASE
                            WHEN copa.acct_hier_shrt_desc::text = 'GTS'::text THEN 
                                COALESCE(copa.amt_obj_crncy::double precision, 0::double precision)
                            ELSE 
                                0::double precision
                        END
                    ) + 
                    sum
                    (
                        CASE
                            WHEN copa.acct_hier_shrt_desc::text = 'NTS'::text AND 
                                ltrim(copa.acct_num::text, '0'::text)::character varying(300) IN (
                                    SELECT itg_query_parameters.parameter_value
                                    FROM itg_query_parameters
                                    WHERE itg_query_parameters.country_code::text = 'HK'::text
                                    AND itg_query_parameters.parameter_name::text = 'PreWW'::text
                                    AND itg_query_parameters.parameter_type::text = 'GL_Account'::text
                                ) THEN 
                                COALESCE(copa.amt_obj_crncy::double precision, 0::double precision)
                            ELSE 
                                0::double precision
                        END
                    ) + 
                    sum
                    (
                        CASE
                            WHEN copa.acct_hier_shrt_desc::text = 'NTS'::text AND 
                                ltrim(copa.acct_num::text, '0'::text)::character varying(300) IN (
                                    SELECT itg_query_parameters.parameter_value
                                    FROM itg_query_parameters
                                    WHERE itg_query_parameters.country_code::text = 'HK'::text
                                    AND itg_query_parameters.parameter_name::text = 'TP'::text
                                    AND itg_query_parameters.parameter_type::text = 'GL_Account'::text
                                ) THEN 
                                COALESCE(copa.amt_obj_crncy::double precision, 0::double precision)
                            ELSE 
                                0::double precision
                        END
                    ) <> 0::double precision

                ) copa
                LEFT JOIN 
                (
                    SELECT 
                        DISTINCT ltrim((edw_gch_producthierarchy.materialnumber)::text,('0'::character varying)::text) AS mat_num,
                        edw_gch_producthierarchy.gcph_franchise,
                        edw_gch_producthierarchy.gcph_needstate,
                        edw_gch_producthierarchy.gcph_brand
                    FROM edw_gch_producthierarchy
                ) gcph ON 
                (
                    (
                        ltrim(
                            (copa.matl_num)::text,
                            ((0)::character varying)::text
                        ) = ((gcph.mat_num)::character varying)::text
                    )
                )
                LEFT JOIN 
                (
                    SELECT DISTINCT edw_vw_greenlight_skus.matl_num,
                        edw_vw_greenlight_skus.matl_desc,
                        edw_vw_greenlight_skus.pka_package_desc,
                        edw_vw_greenlight_skus.pka_size_desc,
                        edw_vw_greenlight_skus.mega_brnd_desc,
                        edw_vw_greenlight_skus.greenlight_sku_flag,
                        edw_vw_greenlight_skus.pka_product_key,
                        edw_vw_greenlight_skus.pka_product_key_description
                    FROM edw_vw_greenlight_skus
                    WHERE (
                            (edw_vw_greenlight_skus.ctry_nm)::text = ('Hong Kong'::character varying)::text
                        )
                ) gls ON 
                (
                    (
                        ltrim(
                            (copa.matl_num)::text,
                            ((0)::character varying)::text
                        ) = ltrim(
                            ((gls.matl_num)::character varying)::text,
                            ((0)::character varying)::text
                        )
                    )
                )
                LEFT JOIN itg_mds_hk_pos_product_mapping pos_prod_master ON 
                (
                    (
                        ltrim(
                            (copa.matl_num)::text,
                            ((0)::character varying)::text
                        ) = (pos_prod_master.jnj_sku_code)::text
                    )
                )
            GROUP BY 
                copa.fisc_yr_per,
                copa.cust_num,
                copa.banner,
                copa.matl_num,
                pos_prod_master.prod_code,
                gls.pka_product_key,
                gls.pka_product_key_description,
                gcph.gcph_franchise,
                gcph.gcph_needstate,
                pos_prod_master.category_name,
                pos_prod_master.age_group_name,
                gcph.gcph_brand,
                gls.mega_brnd_desc,
                gls.pka_package_desc,
                gls.pka_size_desc,
                gls.greenlight_sku_flag
        ) base
        JOIN exch_rate ON ((base.fisc_yr_per = exch_rate.fisc_per))
        LEFT JOIN 
        (
            SELECT DISTINCT EDW_VW_PROMO_CALENDAR.year,
                edw_vw_promo_calendar.fisc_per,
                edw_vw_promo_calendar.qrtr_no,
                edw_vw_promo_calendar.qrtr,
                edw_vw_promo_calendar.mnth_id,
                edw_vw_promo_calendar.mnth_desc,
                edw_vw_promo_calendar.mnth_no,
                edw_vw_promo_calendar.mnth_shrt,
                edw_vw_promo_calendar.mnth_long
            FROM edw_vw_promo_calendar
        ) cal ON ((base.fisc_yr_per = cal.fisc_per))
),
otif_d as 
(
    SELECT 
        'OTIF-D'::character varying AS subsource_type,
        null as customer,
        NULL::character varying AS vendor_desc,
        null as customer_product_code,
        null as jnj_sku_code,
        null as pka_product_key,
        null as pka_product_key_description,
        null as stronghold,
        null as gcph_needstate,
        null as category_name,
        null as age_group_name,
        null as gcph_brand,
        null as mega_brnd_desc,
        null as package,
        null as size,
        null as greenlight_sku_flag,
        exch_rate.usd_rt,
        exch_rate.lcl_rt,
        cal.year,
        cal.fisc_per,
        cal.qrtr_no,
        (cal.qrtr)::character varying AS qrtr,
        (cal.mnth_id)::character varying AS mnth_id,
        (cal.mnth_desc)::character varying AS mnth_desc,
        cal.mnth_no,
        cal.mnth_shrt,
        cal.mnth_long,
        NULL::character varying AS wk,
        NULL::bigint AS mnth_wk_no,
        NULL::bigint AS fisc_week_day_num,
        NULL::bigint AS mnth_day,
        cal.year AS cal_year,
        NULL::integer AS cal_qrtr_no,
        NULL::integer AS cal_mnth_id,
        cal.mnth_no AS cal_mnth_no,
        NULL::character varying AS cal_mnth_nm,
        NULL::bigint AS cal_mnth_day,
        NULL::character varying AS cal_mnth_wk_num,
        NULL::character varying AS cal_yr_wk_num,
        NULL::bigint AS cal_week_day_num,
        NULL::character varying AS promo_week,
        (NULL::numeric)::numeric(18, 0) AS promo_month_week_num,
        (NULL::numeric)::numeric(18, 0) AS promo_week_day_num,
        (cal.mnth_no)::character varying AS promo_month_shrt,
        cal.year AS promo_year,
        NULL::date AS cal_day,
        NULL::character varying AS cal_date_id,
        ((0)::numeric)::numeric(18, 0) AS pos_qty,
        ((0)::numeric)::numeric(18, 0) AS pos_sales,
        ((0)::numeric)::numeric(18, 0) AS sellin_qty,
        ((0)::numeric)::numeric(18, 0) AS sellin_preww,
        ((0)::numeric)::numeric(18, 0) AS sellin_nts,
        null as customer_description,
        a.ciw_gts_lcy,
        a.ciw_gts_usd,
        a.ciw_lcy,
        a.ciw_usd,
        a.ciw_gts_prev_yr_mnth,
        a.ciw_prev_yr_mnth,
        a.ciw_gts_lcy_prev_yr_mnth,
        a.ciw_lcy_prev_yr_mnth,
        a.numerator,
        a.denominator,
        a.numerator_prev_yr,
        a.denominator_prev_yr,
        a.msl_complaince_numerator,
        a.msl_complaince_denominator,
        a.msl_complaince_denominator_wt,
        a.osa_complaince_numerator,
        a.osa_complaince_denominator,
        a.osa_complaince_denominator_wt,
        a.promo_complaince_numerator,
        a.promo_complaince_denominator,
        a.promo_complaince_denominator_wt,
        a.display_complaince_numerator,
        a.display_complaince_denominator,
        a.display_complaince_denominator_wt,
        a.planogram_complaince_numerator,
        a.planogram_complaince_denominator,
        a.planogram_complaince_denominator_wt,
        a.sos_complaince_numerator,
        a.sos_complaince_denominator,
        a.sos_complaince_denominator_wt,
        a.soa_complaince_numerator,
        a.soa_complaince_denominator,
        a.soa_complaince_denominator_wt
    FROM 
        (
            SELECT 
                rpt_regional_scorecard.datasource,
                rpt_regional_scorecard.ctry_nm,
                rpt_regional_scorecard.cluster,
                rpt_regional_scorecard.fisc_yr_per,
                rpt_regional_scorecard.parent_customer,
                rpt_regional_scorecard.copa_nts_usd,
                rpt_regional_scorecard.copa_nts_lcy,
                rpt_regional_scorecard.copa_top30_nts_usd,
                rpt_regional_scorecard.copa_top30_nts_lcy,
                rpt_regional_scorecard.ecomm_nts_usd,
                rpt_regional_scorecard.ecomm_nts_lcy,
                rpt_regional_scorecard.ciw_gts_lcy,
                rpt_regional_scorecard.ciw_gts_usd,
                rpt_regional_scorecard.ciw_lcy,
                rpt_regional_scorecard.ciw_usd,
                rpt_regional_scorecard.prev_yr_mnth,
                rpt_regional_scorecard.nts_prev_yr_mnth,
                rpt_regional_scorecard.nts_lcy_prev_yr_mnth,
                rpt_regional_scorecard.top30_growth,
                rpt_regional_scorecard.ecomm_nts_prev_yr_mnth,
                rpt_regional_scorecard.ecomm_nts_lcy_prev_yr_mnth,
                rpt_regional_scorecard.ecomm_nts_growth,
                rpt_regional_scorecard.ciw_gts_prev_yr_mnth,
                rpt_regional_scorecard.ciw_prev_yr_mnth,
                rpt_regional_scorecard.ciw_gts_lcy_prev_yr_mnth,
                rpt_regional_scorecard.ciw_lcy_prev_yr_mnth,
                rpt_regional_scorecard.growth_ciw_gts,
                rpt_regional_scorecard.growth_ciw,
                rpt_regional_scorecard.kpi,
                rpt_regional_scorecard.msl_complaince_numerator,
                rpt_regional_scorecard.msl_complaince_denominator,
                rpt_regional_scorecard.msl_complaince_denominator_wt,
                rpt_regional_scorecard.osa_complaince_numerator,
                rpt_regional_scorecard.osa_complaince_denominator,
                rpt_regional_scorecard.osa_complaince_denominator_wt,
                rpt_regional_scorecard.promo_complaince_numerator,
                rpt_regional_scorecard.promo_complaince_denominator,
                rpt_regional_scorecard.promo_complaince_denominator_wt,
                rpt_regional_scorecard.display_complaince_numerator,
                rpt_regional_scorecard.display_complaince_denominator,
                rpt_regional_scorecard.display_complaince_denominator_wt,
                rpt_regional_scorecard.planogram_complaince_numerator,
                rpt_regional_scorecard.planogram_complaince_denominator,
                rpt_regional_scorecard.planogram_complaince_denominator_wt,
                rpt_regional_scorecard.sos_complaince_numerator,
                rpt_regional_scorecard.sos_complaince_denominator,
                rpt_regional_scorecard.sos_complaince_denominator_wt,
                rpt_regional_scorecard.soa_complaince_numerator,
                rpt_regional_scorecard.soa_complaince_denominator,
                rpt_regional_scorecard.soa_complaince_denominator_wt,
                rpt_regional_scorecard.healthy_inventory_usd,
                rpt_regional_scorecard.total_inventory_usd,
                rpt_regional_scorecard.last_12_months_so_value_usd,
                rpt_regional_scorecard.weeks_cover,
                rpt_regional_scorecard.perfectstore_latestdate,
                rpt_regional_scorecard.dso_gts,
                rpt_regional_scorecard.dso_gross_account_receivable,
                rpt_regional_scorecard.dso_jnj_days,
                rpt_regional_scorecard.market_share_period_type,
                rpt_regional_scorecard.market_share_total,
                rpt_regional_scorecard.market_share_jnj,
                rpt_regional_scorecard.gross_profit,
                rpt_regional_scorecard.finance_nts,
                rpt_regional_scorecard.market_share_total_prev_yr,
                rpt_regional_scorecard.market_share_jnj_prev_yr,
                rpt_regional_scorecard.dso_gts_prev_yr,
                rpt_regional_scorecard.dso_gross_account_receivable_prev_yr,
                rpt_regional_scorecard.dso_jnj_days_prev_yr,
                rpt_regional_scorecard.gross_profit_prev_yr,
                rpt_regional_scorecard.finance_nts_prev_yr,
                rpt_regional_scorecard.copa_nts_greenlight_sku_usd,
                rpt_regional_scorecard.copa_nts_greenlight_sku_lcy,
                rpt_regional_scorecard.copa_nts_greenlight_sku_usd_prev_yr_mnth,
                rpt_regional_scorecard.copa_nts_greenlight_sku_lcy_prev_yr_mnth,
                rpt_regional_scorecard.numerator,
                rpt_regional_scorecard.denominator,
                rpt_regional_scorecard.numerator_prev_yr,
                rpt_regional_scorecard.denominator_prev_yr,
                rpt_regional_scorecard.customer_segment,
                rpt_regional_scorecard.cy_segment_nts_usd,
                rpt_regional_scorecard.py_segment_nts_usd,
                rpt_regional_scorecard.cy_segment_nts_lcy,
                rpt_regional_scorecard.py_segment_nts_lcy
            FROM rpt_regional_scorecard
            WHERE (
                    (
                        (rpt_regional_scorecard.ctry_nm)::text = ('Hong Kong'::character varying)::text
                    )
                    AND (
                        (rpt_regional_scorecard.datasource)::text = ('OTIF-D'::character varying)::text
                    )
                )
        ) a
        LEFT JOIN 
        (
            SELECT 
                derived_table1.year,
                derived_table1.fisc_per,
                derived_table1.qrtr_no,
                derived_table1.qrtr,
                derived_table1.mnth_id,
                derived_table1.mnth_desc,
                derived_table1.mnth_no,
                derived_table1.mnth_shrt,
                derived_table1.mnth_long,
                derived_table1.wk,
                derived_table1.mnth_wk_no,
                derived_table1.fisc_week_day_num,
                derived_table1.mnth_day,
                derived_table1.cal_year,
                derived_table1.cal_qrtr_no,
                derived_table1.cal_mnth_id,
                derived_table1.cal_mnth_no,
                derived_table1.cal_mnth_nm,
                derived_table1.cal_mnth_day,
                derived_table1.cal_mnth_wk_num,
                derived_table1.cal_yr_wk_num,
                derived_table1.cal_week_day_num,
                derived_table1.promo_week,
                derived_table1.promo_month_week_num,
                derived_table1.promo_week_day_num,
                derived_table1.promo_month_shrt,
                derived_table1.promo_year,
                derived_table1.cal_day,
                derived_table1.cal_date_id,
                derived_table1.rno
            FROM 
                (
                    SELECT 
                        a.year,
                        a.fisc_per,
                        a.qrtr_no,
                        a.qrtr,
                        a.mnth_id,
                        a.mnth_desc,
                        a.mnth_no,
                        a.mnth_shrt,
                        a.mnth_long,
                        a.wk,
                        a.mnth_wk_no,
                        a.fisc_week_day_num,
                        a.mnth_day,
                        a.cal_year,
                        a.cal_qrtr_no,
                        a.cal_mnth_id,
                        a.cal_mnth_no,
                        a.cal_mnth_nm,
                        a.cal_mnth_day,
                        a.cal_mnth_wk_num,
                        a.cal_yr_wk_num,
                        a.cal_week_day_num,
                        a.promo_week,
                        a.promo_month_week_num,
                        a.promo_week_day_num,
                        a.promo_month_shrt,
                        a.promo_year,
                        a.cal_day,
                        a.cal_date_id,
                        row_number() OVER(PARTITION BY a.fisc_per order by null) AS rno
                    FROM edw_vw_promo_calendar a
                ) derived_table1
            WHERE (derived_table1.rno = 1)
        ) cal ON ((a.fisc_yr_per = cal.fisc_per))
        LEFT JOIN 
        (
            SELECT exch_rate.from_crncy,
                exch_rate.fisc_per,
                sum(
                    CASE
                        WHEN (
                            (exch_rate.to_crncy)::text = ('USD'::character varying)::text
                        ) THEN exch_rate.ex_rt
                        ELSE ((0)::numeric)::numeric(18, 0)
                    END
                ) AS usd_rt,
                sum(
                    CASE
                        WHEN (
                            (exch_rate.to_crncy)::text = ('HKD'::character varying)::text
                        ) THEN exch_rate.ex_rt
                        ELSE ((0)::numeric)::numeric(18, 0)
                    END
                ) AS lcl_rt
            FROM v_intrm_reg_crncy_exch_fiscper exch_rate
            WHERE (
                    (
                        (exch_rate.from_crncy)::text = ('HKD'::character varying)::text
                    )
                    AND (
                        (
                            (exch_rate.to_crncy)::text = ('USD'::character varying)::text
                        )
                        OR (
                            (exch_rate.to_crncy)::text = ('HKD'::character varying)::text
                        )
                    )
                )
            GROUP BY exch_rate.from_crncy,
                exch_rate.fisc_per
        ) exch_rate ON ((a.fisc_yr_per = exch_rate.fisc_per))
    WHERE 
        a.fisc_yr_per <= 
        (
            SELECT edw_vw_promo_calendar.fisc_per
            FROM edw_vw_promo_calendar
            WHERE to_date(convert_timezone('UTC', current_timestamp())::character varying::timestamp without time zone) = edw_vw_promo_calendar.cal_day
        )
        AND substring(a.fisc_yr_per::character varying::text, 1, 4)::numeric(18, 0) >= 
        (
            SELECT date_part(year, convert_timezone('UTC', current_timestamp())::character varying::timestamp without time zone)::numeric(18, 0) - itg_query_parameters.parameter_value::numeric(18, 0)
            FROM itg_query_parameters
            WHERE itg_query_parameters.country_code::text = 'HK'::text
            AND itg_query_parameters.parameter_name::text = 'HK_POS_DataMart_Data_Retention_Years'::text
        )

),
ciw as 
(
    SELECT 
        'CIW'::character varying AS subsource_type,
        null as customer,
        NULL::character varying AS vendor_desc,
        null as customer_product_code,
        null as jnj_sku_code,
        null as pka_product_key,
        null as pka_product_key_description,
        null as stronghold,
        null as gcph_needstate,
        null as category_name,
        null as age_group_name,
        null as gcph_brand,
        null as mega_brnd_desc,
        null as package,
        null as size,
        null as greenlight_sku_flag,
        exch_rate.usd_rt,
        exch_rate.lcl_rt,
        cal.year,
        cal.fisc_per,
        cal.qrtr_no,
        (cal.qrtr)::character varying AS qrtr,
        (cal.mnth_id)::character varying AS mnth_id,
        (cal.mnth_desc)::character varying AS mnth_desc,
        cal.mnth_no,
        cal.mnth_shrt,
        cal.mnth_long,
        NULL::character varying AS wk,
        NULL::bigint AS mnth_wk_no,
        NULL::bigint AS fisc_week_day_num,
        NULL::bigint AS mnth_day,
        cal.year AS cal_year,
        NULL::integer AS cal_qrtr_no,
        NULL::integer AS cal_mnth_id,
        cal.mnth_no AS cal_mnth_no,
        NULL::character varying AS cal_mnth_nm,
        NULL::bigint AS cal_mnth_day,
        NULL::character varying AS cal_mnth_wk_num,
        NULL::character varying AS cal_yr_wk_num,
        NULL::bigint AS cal_week_day_num,
        NULL::character varying AS promo_week,
        (NULL::numeric)::numeric(18, 0) AS promo_month_week_num,
        (NULL::numeric)::numeric(18, 0) AS promo_week_day_num,
        (cal.mnth_no)::character varying AS promo_month_shrt,
        cal.year AS promo_year,
        NULL::date AS cal_day,
        NULL::character varying AS cal_date_id,
        ((0)::numeric)::numeric(18, 0) AS pos_qty,
        ((0)::numeric)::numeric(18, 0) AS pos_sales,
        ((0)::numeric)::numeric(18, 0) AS sellin_qty,
        ((0)::numeric)::numeric(18, 0) AS sellin_preww,
        ((0)::numeric)::numeric(18, 0) AS sellin_nts,
        null as customer_description,
        a.ciw_gts_lcy,
        a.ciw_gts_usd,
        a.ciw_lcy,
        a.ciw_usd,
        a.ciw_gts_prev_yr_mnth,
        a.ciw_prev_yr_mnth,
        a.ciw_gts_lcy_prev_yr_mnth,
        a.ciw_lcy_prev_yr_mnth,
        a.numerator,
        a.denominator,
        a.numerator_prev_yr,
        a.denominator_prev_yr,
        a.msl_complaince_numerator,
        a.msl_complaince_denominator,
        a.msl_complaince_denominator_wt,
        a.osa_complaince_numerator,
        a.osa_complaince_denominator,
        a.osa_complaince_denominator_wt,
        a.promo_complaince_numerator,
        a.promo_complaince_denominator,
        a.promo_complaince_denominator_wt,
        a.display_complaince_numerator,
        a.display_complaince_denominator,
        a.display_complaince_denominator_wt,
        a.planogram_complaince_numerator,
        a.planogram_complaince_denominator,
        a.planogram_complaince_denominator_wt,
        a.sos_complaince_numerator,
        a.sos_complaince_denominator,
        a.sos_complaince_denominator_wt,
        a.soa_complaince_numerator,
        a.soa_complaince_denominator,
        a.soa_complaince_denominator_wt
    FROM 
        (
                SELECT 
                    rpt_regional_scorecard.datasource,
                    rpt_regional_scorecard.ctry_nm,
                    rpt_regional_scorecard.cluster,
                    rpt_regional_scorecard.fisc_yr_per,
                    rpt_regional_scorecard.parent_customer,
                    rpt_regional_scorecard.copa_nts_usd,
                    rpt_regional_scorecard.copa_nts_lcy,
                    rpt_regional_scorecard.copa_top30_nts_usd,
                    rpt_regional_scorecard.copa_top30_nts_lcy,
                    rpt_regional_scorecard.ecomm_nts_usd,
                    rpt_regional_scorecard.ecomm_nts_lcy,
                    rpt_regional_scorecard.ciw_gts_lcy,
                    rpt_regional_scorecard.ciw_gts_usd,
                    rpt_regional_scorecard.ciw_lcy,
                    rpt_regional_scorecard.ciw_usd,
                    rpt_regional_scorecard.prev_yr_mnth,
                    rpt_regional_scorecard.nts_prev_yr_mnth,
                    rpt_regional_scorecard.nts_lcy_prev_yr_mnth,
                    rpt_regional_scorecard.top30_growth,
                    rpt_regional_scorecard.ecomm_nts_prev_yr_mnth,
                    rpt_regional_scorecard.ecomm_nts_lcy_prev_yr_mnth,
                    rpt_regional_scorecard.ecomm_nts_growth,
                    rpt_regional_scorecard.ciw_gts_prev_yr_mnth,
                    rpt_regional_scorecard.ciw_prev_yr_mnth,
                    rpt_regional_scorecard.ciw_gts_lcy_prev_yr_mnth,
                    rpt_regional_scorecard.ciw_lcy_prev_yr_mnth,
                    rpt_regional_scorecard.growth_ciw_gts,
                    rpt_regional_scorecard.growth_ciw,
                    rpt_regional_scorecard.kpi,
                    rpt_regional_scorecard.msl_complaince_numerator,
                    rpt_regional_scorecard.msl_complaince_denominator,
                    rpt_regional_scorecard.msl_complaince_denominator_wt,
                    rpt_regional_scorecard.osa_complaince_numerator,
                    rpt_regional_scorecard.osa_complaince_denominator,
                    rpt_regional_scorecard.osa_complaince_denominator_wt,
                    rpt_regional_scorecard.promo_complaince_numerator,
                    rpt_regional_scorecard.promo_complaince_denominator,
                    rpt_regional_scorecard.promo_complaince_denominator_wt,
                    rpt_regional_scorecard.display_complaince_numerator,
                    rpt_regional_scorecard.display_complaince_denominator,
                    rpt_regional_scorecard.display_complaince_denominator_wt,
                    rpt_regional_scorecard.planogram_complaince_numerator,
                    rpt_regional_scorecard.planogram_complaince_denominator,
                    rpt_regional_scorecard.planogram_complaince_denominator_wt,
                    rpt_regional_scorecard.sos_complaince_numerator,
                    rpt_regional_scorecard.sos_complaince_denominator,
                    rpt_regional_scorecard.sos_complaince_denominator_wt,
                    rpt_regional_scorecard.soa_complaince_numerator,
                    rpt_regional_scorecard.soa_complaince_denominator,
                    rpt_regional_scorecard.soa_complaince_denominator_wt,
                    rpt_regional_scorecard.healthy_inventory_usd,
                    rpt_regional_scorecard.total_inventory_usd,
                    rpt_regional_scorecard.last_12_months_so_value_usd,
                    rpt_regional_scorecard.weeks_cover,
                    rpt_regional_scorecard.perfectstore_latestdate,
                    rpt_regional_scorecard.dso_gts,
                    rpt_regional_scorecard.dso_gross_account_receivable,
                    rpt_regional_scorecard.dso_jnj_days,
                    rpt_regional_scorecard.market_share_period_type,
                    rpt_regional_scorecard.market_share_total,
                    rpt_regional_scorecard.market_share_jnj,
                    rpt_regional_scorecard.gross_profit,
                    rpt_regional_scorecard.finance_nts,
                    rpt_regional_scorecard.market_share_total_prev_yr,
                    rpt_regional_scorecard.market_share_jnj_prev_yr,
                    rpt_regional_scorecard.dso_gts_prev_yr,
                    rpt_regional_scorecard.dso_gross_account_receivable_prev_yr,
                    rpt_regional_scorecard.dso_jnj_days_prev_yr,
                    rpt_regional_scorecard.gross_profit_prev_yr,
                    rpt_regional_scorecard.finance_nts_prev_yr,
                    rpt_regional_scorecard.copa_nts_greenlight_sku_usd,
                    rpt_regional_scorecard.copa_nts_greenlight_sku_lcy,
                    rpt_regional_scorecard.copa_nts_greenlight_sku_usd_prev_yr_mnth,
                    rpt_regional_scorecard.copa_nts_greenlight_sku_lcy_prev_yr_mnth,
                    rpt_regional_scorecard.numerator,
                    rpt_regional_scorecard.denominator,
                    rpt_regional_scorecard.numerator_prev_yr,
                    rpt_regional_scorecard.denominator_prev_yr,
                    rpt_regional_scorecard.customer_segment,
                    rpt_regional_scorecard.cy_segment_nts_usd,
                    rpt_regional_scorecard.py_segment_nts_usd,
                    rpt_regional_scorecard.cy_segment_nts_lcy,
                    rpt_regional_scorecard.py_segment_nts_lcy
                FROM rpt_regional_scorecard
                WHERE (
                        (
                            (rpt_regional_scorecard.ctry_nm)::text = ('Hong Kong'::character varying)::text
                        )
                        AND (
                            (rpt_regional_scorecard.datasource)::text = ('CIW'::character varying)::text
                        )
                    )
        ) a
        LEFT JOIN 
        (
            SELECT 
                derived_table2.year,
                derived_table2.fisc_per,
                derived_table2.qrtr_no,
                derived_table2.qrtr,
                derived_table2.mnth_id,
                derived_table2.mnth_desc,
                derived_table2.mnth_no,
                derived_table2.mnth_shrt,
                derived_table2.mnth_long,
                derived_table2.wk,
                derived_table2.mnth_wk_no,
                derived_table2.fisc_week_day_num,
                derived_table2.mnth_day,
                derived_table2.cal_year,
                derived_table2.cal_qrtr_no,
                derived_table2.cal_mnth_id,
                derived_table2.cal_mnth_no,
                derived_table2.cal_mnth_nm,
                derived_table2.cal_mnth_day,
                derived_table2.cal_mnth_wk_num,
                derived_table2.cal_yr_wk_num,
                derived_table2.cal_week_day_num,
                derived_table2.promo_week,
                derived_table2.promo_month_week_num,
                derived_table2.promo_week_day_num,
                derived_table2.promo_month_shrt,
                derived_table2.promo_year,
                derived_table2.cal_day,
                derived_table2.cal_date_id,
                derived_table2.rno
            FROM 
                (
                    SELECT 
                        a.year,
                        a.fisc_per,
                        a.qrtr_no,
                        a.qrtr,
                        a.mnth_id,
                        a.mnth_desc,
                        a.mnth_no,
                        a.mnth_shrt,
                        a.mnth_long,
                        a.wk,
                        a.mnth_wk_no,
                        a.fisc_week_day_num,
                        a.mnth_day,
                        a.cal_year,
                        a.cal_qrtr_no,
                        a.cal_mnth_id,
                        a.cal_mnth_no,
                        a.cal_mnth_nm,
                        a.cal_mnth_day,
                        a.cal_mnth_wk_num,
                        a.cal_yr_wk_num,
                        a.cal_week_day_num,
                        a.promo_week,
                        a.promo_month_week_num,
                        a.promo_week_day_num,
                        a.promo_month_shrt,
                        a.promo_year,
                        a.cal_day,
                        a.cal_date_id,
                        row_number() OVER(PARTITION BY a.fisc_per order by null) AS rno
                    FROM edw_vw_promo_calendar a
                ) derived_table2
            WHERE (derived_table2.rno = 1)
        ) cal ON ((a.fisc_yr_per = cal.fisc_per))
        LEFT JOIN exch_rate ON ((a.fisc_yr_per = exch_rate.fisc_per))
    WHERE 
    a.fisc_yr_per <= 
    (
        SELECT edw_vw_promo_calendar.fisc_per
        FROM edw_vw_promo_calendar
        WHERE to_date(convert_timezone('UTC', current_timestamp())::character varying::timestamp without time zone) = edw_vw_promo_calendar.cal_day
    )
    AND substring(a.fisc_yr_per::character varying::text, 1, 4)::numeric(18, 0) >= 
    (
        SELECT date_part(year, convert_timezone('UTC', current_timestamp())::character varying::timestamp without time zone)::numeric(18, 0) - itg_query_parameters.parameter_value::numeric(18, 0)
        FROM itg_query_parameters
        WHERE itg_query_parameters.country_code::text = 'HK'::text
        AND itg_query_parameters.parameter_name::text = 'HK_POS_DataMart_Data_Retention_Years'::text
    )
),
perfectstore as 
(
    SELECT 
        'PERFECTSTORE'::character varying AS subsource_type,
        null as customer,
        NULL::character varying AS vendor_desc,
        null as customer_product_code,
        null as jnj_sku_code,
        null as pka_product_key,
        null as pka_product_key_description,
        null as stronghold,
        null as gcph_needstate,
        null as category_name,
        null as age_group_name,
        null as gcph_brand,
        null as mega_brnd_desc,
        null as package,
        null as size,
        null as greenlight_sku_flag,
        exch_rate.usd_rt,
        exch_rate.lcl_rt,
        cal.year,
        cal.fisc_per,
        cal.qrtr_no,
        (cal.qrtr)::character varying AS qrtr,
        (cal.mnth_id)::character varying AS mnth_id,
        (cal.mnth_desc)::character varying AS mnth_desc,
        cal.mnth_no,
        cal.mnth_shrt,
        cal.mnth_long,
        NULL::character varying AS wk,
        NULL::bigint AS mnth_wk_no,
        NULL::bigint AS fisc_week_day_num,
        NULL::bigint AS mnth_day,
        cal.year AS cal_year,
        NULL::integer AS cal_qrtr_no,
        NULL::integer AS cal_mnth_id,
        cal.mnth_no AS cal_mnth_no,
        NULL::character varying AS cal_mnth_nm,
        NULL::bigint AS cal_mnth_day,
        NULL::character varying AS cal_mnth_wk_num,
        NULL::character varying AS cal_yr_wk_num,
        NULL::bigint AS cal_week_day_num,
        NULL::character varying AS promo_week,
        (NULL::numeric)::numeric(18, 0) AS promo_month_week_num,
        (NULL::numeric)::numeric(18, 0) AS promo_week_day_num,
        (cal.mnth_no)::character varying AS promo_month_shrt,
        cal.year AS promo_year,
        NULL::date AS cal_day,
        NULL::character varying AS cal_date_id,
        ((0)::numeric)::numeric(18, 0) AS pos_qty,
        ((0)::numeric)::numeric(18, 0) AS pos_sales,
        ((0)::numeric)::numeric(18, 0) AS sellin_qty,
        ((0)::numeric)::numeric(18, 0) AS sellin_preww,
        ((0)::numeric)::numeric(18, 0) AS sellin_nts,
        null as customer_description,
        a.ciw_gts_lcy,
        a.ciw_gts_usd,
        a.ciw_lcy,
        a.ciw_usd,
        a.ciw_gts_prev_yr_mnth,
        a.ciw_prev_yr_mnth,
        a.ciw_gts_lcy_prev_yr_mnth,
        a.ciw_lcy_prev_yr_mnth,
        a.numerator,
        a.denominator,
        a.numerator_prev_yr,
        a.denominator_prev_yr,
        a.msl_complaince_numerator,
        a.msl_complaince_denominator,
        a.msl_complaince_denominator_wt,
        a.osa_complaince_numerator,
        a.osa_complaince_denominator,
        a.osa_complaince_denominator_wt,
        a.promo_complaince_numerator,
        a.promo_complaince_denominator,
        a.promo_complaince_denominator_wt,
        a.display_complaince_numerator,
        a.display_complaince_denominator,
        a.display_complaince_denominator_wt,
        a.planogram_complaince_numerator,
        a.planogram_complaince_denominator,
        a.planogram_complaince_denominator_wt,
        a.sos_complaince_numerator,
        a.sos_complaince_denominator,
        a.sos_complaince_denominator_wt,
        a.soa_complaince_numerator,
        a.soa_complaince_denominator,
        a.soa_complaince_denominator_wt
    FROM 
        (
            SELECT 
                rpt_regional_scorecard.datasource,
                rpt_regional_scorecard.ctry_nm,
                rpt_regional_scorecard.cluster,
                rpt_regional_scorecard.fisc_yr_per,
                rpt_regional_scorecard.parent_customer,
                rpt_regional_scorecard.copa_nts_usd,
                rpt_regional_scorecard.copa_nts_lcy,
                rpt_regional_scorecard.copa_top30_nts_usd,
                rpt_regional_scorecard.copa_top30_nts_lcy,
                rpt_regional_scorecard.ecomm_nts_usd,
                rpt_regional_scorecard.ecomm_nts_lcy,
                rpt_regional_scorecard.ciw_gts_lcy,
                rpt_regional_scorecard.ciw_gts_usd,
                rpt_regional_scorecard.ciw_lcy,
                rpt_regional_scorecard.ciw_usd,
                rpt_regional_scorecard.prev_yr_mnth,
                rpt_regional_scorecard.nts_prev_yr_mnth,
                rpt_regional_scorecard.nts_lcy_prev_yr_mnth,
                rpt_regional_scorecard.top30_growth,
                rpt_regional_scorecard.ecomm_nts_prev_yr_mnth,
                rpt_regional_scorecard.ecomm_nts_lcy_prev_yr_mnth,
                rpt_regional_scorecard.ecomm_nts_growth,
                rpt_regional_scorecard.ciw_gts_prev_yr_mnth,
                rpt_regional_scorecard.ciw_prev_yr_mnth,
                rpt_regional_scorecard.ciw_gts_lcy_prev_yr_mnth,
                rpt_regional_scorecard.ciw_lcy_prev_yr_mnth,
                rpt_regional_scorecard.growth_ciw_gts,
                rpt_regional_scorecard.growth_ciw,
                rpt_regional_scorecard.kpi,
                rpt_regional_scorecard.msl_complaince_numerator,
                rpt_regional_scorecard.msl_complaince_denominator,
                rpt_regional_scorecard.msl_complaince_denominator_wt,
                rpt_regional_scorecard.osa_complaince_numerator,
                rpt_regional_scorecard.osa_complaince_denominator,
                rpt_regional_scorecard.osa_complaince_denominator_wt,
                rpt_regional_scorecard.promo_complaince_numerator,
                rpt_regional_scorecard.promo_complaince_denominator,
                rpt_regional_scorecard.promo_complaince_denominator_wt,
                rpt_regional_scorecard.display_complaince_numerator,
                rpt_regional_scorecard.display_complaince_denominator,
                rpt_regional_scorecard.display_complaince_denominator_wt,
                rpt_regional_scorecard.planogram_complaince_numerator,
                rpt_regional_scorecard.planogram_complaince_denominator,
                rpt_regional_scorecard.planogram_complaince_denominator_wt,
                rpt_regional_scorecard.sos_complaince_numerator,
                rpt_regional_scorecard.sos_complaince_denominator,
                rpt_regional_scorecard.sos_complaince_denominator_wt,
                rpt_regional_scorecard.soa_complaince_numerator,
                rpt_regional_scorecard.soa_complaince_denominator,
                rpt_regional_scorecard.soa_complaince_denominator_wt,
                rpt_regional_scorecard.healthy_inventory_usd,
                rpt_regional_scorecard.total_inventory_usd,
                rpt_regional_scorecard.last_12_months_so_value_usd,
                rpt_regional_scorecard.weeks_cover,
                rpt_regional_scorecard.perfectstore_latestdate,
                rpt_regional_scorecard.dso_gts,
                rpt_regional_scorecard.dso_gross_account_receivable,
                rpt_regional_scorecard.dso_jnj_days,
                rpt_regional_scorecard.market_share_period_type,
                rpt_regional_scorecard.market_share_total,
                rpt_regional_scorecard.market_share_jnj,
                rpt_regional_scorecard.gross_profit,
                rpt_regional_scorecard.finance_nts,
                rpt_regional_scorecard.market_share_total_prev_yr,
                rpt_regional_scorecard.market_share_jnj_prev_yr,
                rpt_regional_scorecard.dso_gts_prev_yr,
                rpt_regional_scorecard.dso_gross_account_receivable_prev_yr,
                rpt_regional_scorecard.dso_jnj_days_prev_yr,
                rpt_regional_scorecard.gross_profit_prev_yr,
                rpt_regional_scorecard.finance_nts_prev_yr,
                rpt_regional_scorecard.copa_nts_greenlight_sku_usd,
                rpt_regional_scorecard.copa_nts_greenlight_sku_lcy,
                rpt_regional_scorecard.copa_nts_greenlight_sku_usd_prev_yr_mnth,
                rpt_regional_scorecard.copa_nts_greenlight_sku_lcy_prev_yr_mnth,
                rpt_regional_scorecard.numerator,
                rpt_regional_scorecard.denominator,
                rpt_regional_scorecard.numerator_prev_yr,
                rpt_regional_scorecard.denominator_prev_yr,
                rpt_regional_scorecard.customer_segment,
                rpt_regional_scorecard.cy_segment_nts_usd,
                rpt_regional_scorecard.py_segment_nts_usd,
                rpt_regional_scorecard.cy_segment_nts_lcy,
                rpt_regional_scorecard.py_segment_nts_lcy
            FROM rpt_regional_scorecard
            WHERE (
                    (
                        (rpt_regional_scorecard.ctry_nm)::text = ('Hong Kong'::character varying)::text
                    )
                    AND (
                        (rpt_regional_scorecard.datasource)::text = ('PERFECTSTORE'::character varying)::text
                    )
                )
        ) a
        LEFT JOIN 
        (
            SELECT 
                derived_table3.year,
                derived_table3.fisc_per,
                derived_table3.qrtr_no,
                derived_table3.qrtr,
                derived_table3.mnth_id,
                derived_table3.mnth_desc,
                derived_table3.mnth_no,
                derived_table3.mnth_shrt,
                derived_table3.mnth_long,
                derived_table3.wk,
                derived_table3.mnth_wk_no,
                derived_table3.fisc_week_day_num,
                derived_table3.mnth_day,
                derived_table3.cal_year,
                derived_table3.cal_qrtr_no,
                derived_table3.cal_mnth_id,
                derived_table3.cal_mnth_no,
                derived_table3.cal_mnth_nm,
                derived_table3.cal_mnth_day,
                derived_table3.cal_mnth_wk_num,
                derived_table3.cal_yr_wk_num,
                derived_table3.cal_week_day_num,
                derived_table3.promo_week,
                derived_table3.promo_month_week_num,
                derived_table3.promo_week_day_num,
                derived_table3.promo_month_shrt,
                derived_table3.promo_year,
                derived_table3.cal_day,
                derived_table3.cal_date_id,
                derived_table3.rno
            FROM (
                    SELECT
                        a.year,
                        a.fisc_per,
                        a.qrtr_no,
                        a.qrtr,
                        a.mnth_id,
                        a.mnth_desc,
                        a.mnth_no,
                        a.mnth_shrt,
                        a.mnth_long,
                        a.wk,
                        a.mnth_wk_no,
                        a.fisc_week_day_num,
                        a.mnth_day,
                        a.cal_year,
                        a.cal_qrtr_no,
                        a.cal_mnth_id,
                        a.cal_mnth_no,
                        a.cal_mnth_nm,
                        a.cal_mnth_day,
                        a.cal_mnth_wk_num,
                        a.cal_yr_wk_num,
                        a.cal_week_day_num,
                        a.promo_week,
                        a.promo_month_week_num,
                        a.promo_week_day_num,
                        a.promo_month_shrt,
                        a.promo_year,
                        a.cal_day,
                        a.cal_date_id,
                        row_number() OVER(PARTITION BY a.fisc_per order by null) AS rno
                    FROM edw_vw_promo_calendar a
                ) derived_table3
            WHERE (derived_table3.rno = 1)
        ) cal ON ((a.fisc_yr_per = cal.fisc_per))
        LEFT JOIN  exch_rate ON ((a.fisc_yr_per = exch_rate.fisc_per))
    WHERE 
        (
            (
                a.fisc_yr_per <= (
                    SELECT edw_vw_promo_calendar.fisc_per
                    FROM edw_vw_promo_calendar
                    WHERE (
                            to_date(
                                (convert_timezone('UTC', current_timestamp())::character varying)::timestamp without time zone
                            ) = edw_vw_promo_calendar.cal_day
                        )
                )
            )
            AND (
                (
                    "substring"(((a.fisc_yr_per)::character varying)::text, 1, 4)
                )::numeric(18, 0) >= (
                    SELECT (
                            (
                                (
                                    "date_part"(
                                        year,
                                        (convert_timezone('UTC', current_timestamp())::character varying)::timestamp without time zone
                                    )
                                )::numeric
                            )::numeric(18, 0) - (itg_query_parameters.parameter_value)::numeric(18, 0)
                        )
                    FROM itg_query_parameters
                    WHERE (
                            (
                                (itg_query_parameters.country_code)::text = ('HK'::character varying)::text
                            )
                            AND (
                                (itg_query_parameters.parameter_name)::text = (
                                    'HK_POS_DataMart_Data_Retention_Years'::character varying
                                )::text
                            )
                        )
                )
            )
        )
),
final as
(
    select * from pos
    UNION ALL
    select * from sellin_qty
    UNION ALL
    select * from preww_nts
    UNION ALL
    select * from otif_d
    UNION ALL
    select * from ciw
    UNION ALL
    select * from perfectstore
)
select * from final