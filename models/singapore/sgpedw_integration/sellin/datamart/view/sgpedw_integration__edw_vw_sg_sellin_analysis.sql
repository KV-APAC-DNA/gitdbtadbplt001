with itg_sg_constant_key_value as
(
    select * from {{ source('sgpitg_integration', 'itg_sg_constant_key_value') }}
),
edw_vw_sg_material_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_sg_material_dim') }}
),
edw_vw_sg_customer_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_sg_customer_dim') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_vw_sg_curr_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_sg_curr_dim') }}
),
itg_sg_zuellig_sellout as
(
    select * from {{ ref('sgpitg_integration__itg_sg_zuellig_sellout') }}
),
itg_sg_tp_closed_month as
(
    select * from {{ ref('sgpitg_integration__itg_sg_tp_closed_month') }}
),
itg_sg_tp_closed_year_bal as
(
    select * from {{ ref('sgpitg_integration__itg_sg_tp_closed_year_bal') }}
),
itg_sg_zuellig_customer_mapping as
(
    select * from {{ ref('sgpitg_integration__itg_sg_zuellig_customer_mapping') }}
),
itg_sg_ciw_mapping as
(
    select * from {{ ref('sgpitg_integration__itg_sg_ciw_mapping') }}
),
itg_sg_zuellig_product_mapping as
(
    select * from {{ ref('sgpitg_integration__itg_sg_zuellig_product_mapping') }}
),
itg_sg_zuellig_product_mapping as
(
    select * from {{ ref('sgpitg_integration__itg_sg_zuellig_product_mapping') }}
),
edw_vw_sg_sellin_sales_fact as(
    select * from {{ ref('sgpedw_integration__edw_vw_sg_sellin_sales_fact') }}
),
divest as(
    SELECT DISTINCT
        ITG_SG_CONSTANT_KEY_VALUE.key,
        ITG_SG_CONSTANT_KEY_VALUE.effective_period_from,
        CASE
        WHEN 
        (
        (
        CAST((ITG_SG_CONSTANT_KEY_VALUE.effective_period_to) AS TEXT) = CAST(NULL AS TEXT)
        )
        OR 
        (
        (ITG_SG_CONSTANT_KEY_VALUE.effective_period_to IS NULL)
        AND (NULL IS NULL)
        )
        )
        THEN CAST('209912' AS TEXT)
        WHEN 
        (
        (
        CAST((ITG_SG_CONSTANT_KEY_VALUE.effective_period_to) AS TEXT) = CAST('' AS TEXT)
        )
        OR 
        (
        (ITG_SG_CONSTANT_KEY_VALUE.effective_period_to IS NULL)
        AND ('' IS NULL)
        )
        )
        THEN CAST('209912' AS TEXT)

        ELSE CAST(NULL AS TEXT)
        END AS effective_period_to
    FROM snaposeitg_integration.ITG_SG_CONSTANT_KEY_VALUE AS ITG_SG_CONSTANT_KEY_VALUE
    WHERE   
    (ITG_SG_CONSTANT_KEY_VALUE.data_category_cd = 2::DECIMAL)
    AND (CAST((ITG_SG_CONSTANT_KEY_VALUE.value) AS TEXT) = CAST('Y' AS TEXT))
),
evomd as (
    SELECT
        edw_vw_sg_material_dim.cntry_key,
        edw_vw_sg_material_dim.sap_matl_num,
        edw_vw_sg_material_dim.sap_mat_desc,
        edw_vw_sg_material_dim.ean_num,
        edw_vw_sg_material_dim.sap_mat_type_cd,
        edw_vw_sg_material_dim.sap_mat_type_desc,
        edw_vw_sg_material_dim.sap_base_uom_cd,
        edw_vw_sg_material_dim.sap_prchse_uom_cd,
        edw_vw_sg_material_dim.sap_prod_sgmt_cd,
        edw_vw_sg_material_dim.sap_prod_sgmt_desc,
        edw_vw_sg_material_dim.sap_base_prod_cd,
        edw_vw_sg_material_dim.sap_base_prod_desc,
        edw_vw_sg_material_dim.sap_mega_brnd_cd,
        edw_vw_sg_material_dim.sap_mega_brnd_desc,
        edw_vw_sg_material_dim.sap_brnd_cd,
        edw_vw_sg_material_dim.sap_brnd_desc,
        edw_vw_sg_material_dim.sap_vrnt_cd,
        edw_vw_sg_material_dim.sap_vrnt_desc,
        edw_vw_sg_material_dim.sap_put_up_cd,
        edw_vw_sg_material_dim.sap_put_up_desc,
        edw_vw_sg_material_dim.sap_grp_frnchse_cd,
        edw_vw_sg_material_dim.sap_grp_frnchse_desc,
        edw_vw_sg_material_dim.sap_frnchse_cd,
        edw_vw_sg_material_dim.sap_frnchse_desc,
        edw_vw_sg_material_dim.sap_prod_frnchse_cd,
        edw_vw_sg_material_dim.sap_prod_frnchse_desc,
        edw_vw_sg_material_dim.sap_prod_mjr_cd,
        edw_vw_sg_material_dim.sap_prod_mjr_desc,
        edw_vw_sg_material_dim.sap_prod_mnr_cd,
        edw_vw_sg_material_dim.sap_prod_mnr_desc,
        edw_vw_sg_material_dim.sap_prod_hier_cd,
        edw_vw_sg_material_dim.sap_prod_hier_desc,
        edw_vw_sg_material_dim.gph_region,
        edw_vw_sg_material_dim.gph_reg_frnchse,
        edw_vw_sg_material_dim.gph_reg_frnchse_grp,
        edw_vw_sg_material_dim.gph_prod_frnchse,
        edw_vw_sg_material_dim.gph_prod_brnd,
        edw_vw_sg_material_dim.gph_prod_sub_brnd,
        edw_vw_sg_material_dim.gph_prod_vrnt,
        edw_vw_sg_material_dim.gph_prod_needstate,
        edw_vw_sg_material_dim.gph_prod_ctgry,
        edw_vw_sg_material_dim.gph_prod_subctgry,
        edw_vw_sg_material_dim.gph_prod_sgmnt,
        edw_vw_sg_material_dim.gph_prod_subsgmnt,
        edw_vw_sg_material_dim.gph_prod_put_up_cd,
        edw_vw_sg_material_dim.gph_prod_put_up_desc,
        edw_vw_sg_material_dim.gph_prod_size,
        edw_vw_sg_material_dim.gph_prod_size_uom,
        edw_vw_sg_material_dim.launch_dt,
        edw_vw_sg_material_dim.qty_shipper_pc,
        edw_vw_sg_material_dim.prft_ctr,
        edw_vw_sg_material_dim.shlf_life
    FROM edw_vw_sg_material_dim
),

evocd as 
(
    SELECT DISTINCT
        MIN(CAST((EDW_VW_sg_CUSTOMER_DIM.sap_cust_nm) AS TEXT)) AS sap_cust_nm,
        EDW_VW_sg_CUSTOMER_DIM.sap_sls_org,
        EDW_VW_sg_CUSTOMER_DIM.sap_cmp_id,
        EDW_VW_sg_CUSTOMER_DIM.sap_cntry_cd,
        EDW_VW_sg_CUSTOMER_DIM.sap_cntry_nm,
        EDW_VW_sg_CUSTOMER_DIM.sap_prnt_cust_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_prnt_cust_desc,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_desc,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_frmt_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_frmt_desc,
        EDW_VW_sg_CUSTOMER_DIM.retail_env
    FROM EDW_VW_sg_CUSTOMER_DIM
    GROUP BY
        EDW_VW_sg_CUSTOMER_DIM.sap_sls_org,
        EDW_VW_sg_CUSTOMER_DIM.sap_cmp_id,
        EDW_VW_sg_CUSTOMER_DIM.sap_cntry_cd,
        EDW_VW_sg_CUSTOMER_DIM.sap_cntry_nm,
        EDW_VW_sg_CUSTOMER_DIM.sap_prnt_cust_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_prnt_cust_desc,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_desc,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_frmt_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_frmt_desc,
        EDW_VW_sg_CUSTOMER_DIM.retail_env
)
,
main as
(
    SELECT
        CAST('Zuellig Sellout' AS TEXT) AS data_source,
        evocd.sap_sls_org,
        LTRIM(CAST((evocd.sap_cmp_id) AS TEXT), CAST('0' AS TEXT)) AS sap_cmp_id,
        evocd.sap_cntry_nm,
        SUBSTRING(CAST((iszs.period) AS TEXT), 1, 4) AS "year",
        iszs.period AS mnth_id,
        iszs.sg_banner,
        evocd.sap_bnr_frmt_desc,
        evocd.sap_cust_nm,
        evocd.sap_prnt_cust_desc,
        evocd.retail_env,
        iszs.merchandizing_flg,
        iszs.sg_brand,
        evomd.gph_reg_frnchse_grp,
        evomd.gph_prod_frnchse,
        evomd.gph_prod_brnd,
        SUM(iszs.sales_value) AS sales_value,
        SUM(iszs.sales_units) AS sales_units,
        SUM(iszs.bonus_units) AS bonus_units,
        SUM(iszs.returns_units) AS returns_units,
        SUM(iszs.returns_value) AS returns_value,
        SUM(iszs.returns_bonus_units) AS returns_bonus_units,
        SUM(iszs.gts) AS jj_net_sales_value
    FROM 
    (
        itg_sg_zuellig_sellout AS iszs
        LEFT JOIN divest
        ON 
        (
            CAST(( iszs.period) AS TEXT) >= CAST((divest.effective_period_from) AS TEXT)
        )
        AND
        (
            CAST(( iszs.period) AS TEXT) <= CAST((divest.effective_period_to) AS TEXT)
            AND ( CAST((iszs.sg_brand) AS TEXT) = CAST((divest.key) AS TEXT))
        )  
        LEFT JOIN evomd
        ON evomd.sap_matl_num::TEXT = iszs.sap_item_code::TEXT
    )
    LEFT JOIN evocd 
    ON upper(evocd.sap_bnr_frmt_desc::text) = upper(iszs.sg_banner::text)
    WHERE divest.key IS NULL
        GROUP BY
        1,
        evocd.sap_sls_org,LTRIM(CAST((evocd.sap_cmp_id) AS TEXT), CAST('0' AS TEXT)),
        evocd.sap_cntry_nm,
        SUBSTRING(CAST((iszs.period) AS TEXT), 1, 4),
        iszs.period,
        iszs.sg_banner,
        evocd.sap_bnr_frmt_desc,
        evocd.sap_cust_nm,
        evocd.sap_prnt_cust_desc,
        evocd.retail_env,
        iszs.merchandizing_flg,
        iszs.sg_brand,
        evomd.gph_reg_frnchse_grp,
        evomd.gph_prod_frnchse,
        evomd.gph_prod_brnd
),
zsellout as
(
    SELECT
        main.data_source,
        main.sap_sls_org,
        main.sap_cmp_id,
        main.sap_cntry_nm,
        main."year",
        main.mnth_id,
        main.sg_banner,
        main.sap_bnr_frmt_desc,
        main.sap_cust_nm,
        main.sap_prnt_cust_desc,
        main.retail_env,
        main.merchandizing_flg,
        main.sg_brand,
        main.gph_reg_frnchse_grp,
        main.gph_prod_frnchse,
        main.gph_prod_brnd,
        main.sales_value,
        main.sales_units,
        main.bonus_units,
        main.returns_units,
        main.returns_value,
        main.returns_bonus_units,
        main.jj_net_sales_value,
        CASE
            WHEN (CAST((main.merchandizing_flg) AS TEXT) = CAST('Y' AS TEXT))
            THEN main.jj_net_sales_value
            ELSE 0::DECIMAL
        END AS jj_net_sales_value_for_merchndsng,
        SUM(main.jj_net_sales_value) OVER (PARTITION BY main.mnth_id order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS jj_tot_sls_val,
        SUM(main.jj_net_sales_value) OVER (PARTITION BY main.mnth_id, main.sg_brand order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS jj_prd_tot_sls_val,
        SUM(main.jj_net_sales_value) OVER (PARTITION BY main.mnth_id, main.sg_banner order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS jj_cust_tot_sls_val,
        SUM(CASE
                WHEN (CAST((main.merchandizing_flg) AS TEXT) = CAST('Y' AS TEXT))
                THEN main.jj_net_sales_value
                ELSE 0::DECIMAL
            END
            )   OVER (PARTITION BY main.mnth_id order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS jj_tot_sls_val_for_merchndsng,
        SUM(CASE
                WHEN (CAST((main.merchandizing_flg) AS TEXT) = CAST('Y' AS TEXT))
                THEN main.jj_net_sales_value
                ELSE 0::DECIMAL
            END
            )   OVER (PARTITION BY main.mnth_id, main.sg_brand order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS jj_prd_tot_sls_val_for_merchndsng,
        SUM(CASE
                WHEN (CAST((main.merchandizing_flg) AS TEXT) = CAST('Y' AS TEXT))
                THEN main.jj_net_sales_value
                ELSE 0::DECIMAL
            END
            )   OVER (PARTITION BY main.mnth_id, main.sg_banner order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS jj_cust_tot_sls_val_for_merchndsng,

        COALESCE
        (
            (
                (
                SUM(main.jj_net_sales_value) OVER (PARTITION BY main.mnth_id, main.sg_banner order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) * CAST((1000000) AS DECIMAL)
                ) / CASE
                        WHEN 
                        (
                            SUM(main.jj_net_sales_value) OVER (PARTITION BY main.mnth_id order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = CAST((0) AS DECIMAL)
                        )
                        THEN CAST(NULL AS DECIMAL)
                        ELSE SUM(main.jj_net_sales_value) OVER (PARTITION BY main.mnth_id order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                    END
            ),
            CAST((0) AS DECIMAL)
        ) AS jj_cust_sls_contrb_per_m,
        COALESCE
        (
            (
            (
                main.jj_net_sales_value * CAST((1000000) AS DECIMAL)
            ) / CASE
                    WHEN 
                    (
                    SUM(main.jj_net_sales_value) OVER (PARTITION BY main.mnth_id, main.sg_brand order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = CAST((0) AS DECIMAL)
                    )
                    THEN CAST(NULL AS DECIMAL)
                    ELSE SUM(main.jj_net_sales_value) OVER (PARTITION BY main.mnth_id, main.sg_brand order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                END
            ),
            CAST((0) AS DECIMAL)
        ) AS jj_prod_cust_sls_contrb_per_m,
        COALESCE
        (
            (
                (
                    main.jj_net_sales_value * CAST((1000000) AS DECIMAL)
                ) / CASE
                        WHEN 
                        (
                        SUM(main.jj_net_sales_value) OVER (PARTITION BY main.mnth_id, main.sg_banner order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = CAST((0) AS DECIMAL)
                        )
                        THEN CAST(NULL AS DECIMAL)
                        ELSE SUM(main.jj_net_sales_value) OVER (PARTITION BY main.mnth_id, main.sg_banner order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                    END
                ),
            CAST((0) AS DECIMAL)
        ) AS jj_cust_prod_sls_contrb_per_m,
        COALESCE
        (
            (
                (
                    SUM
                    (
                        CASE
                            WHEN (CAST((main.merchandizing_flg) AS TEXT) = CAST('Y' AS TEXT))
                            THEN main.jj_net_sales_value
                            ELSE 0::DECIMAL
                        END
                    ) OVER (PARTITION BY main.mnth_id, main.sg_banner order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) *1000000::DECIMAL 
                ) / CASE
                    WHEN 
                    (
                        SUM
                        (
                            CASE
                                WHEN (CAST((main.merchandizing_flg) AS TEXT) = CAST('Y' AS TEXT))
                                THEN main.jj_net_sales_value
                                ELSE 0::DECIMAL
                            END
                        ) OVER (PARTITION BY main.mnth_id order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 0::DECIMAL
                    )
                    THEN CAST(NULL AS DECIMAL)
                    ELSE 
                    SUM
                    (
                        CASE
                            WHEN (CAST((main.merchandizing_flg) AS TEXT) = CAST('Y' AS TEXT))
                            THEN main.jj_net_sales_value
                            ELSE 0::DECIMAL
                        END
                    ) OVER (PARTITION BY main.mnth_id order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                END
            ),0::DECIMAL
        ) AS jj_cust_sls_contrb_for_merchndsng_per_m,
        COALESCE
        (
            (
                (
                    CASE
                        WHEN (CAST((main.merchandizing_flg) AS TEXT) = CAST('Y' AS TEXT))
                        THEN main.jj_net_sales_value
                        ELSE 0::DECIMAL
                    END * 1000000::DECIMAL
                ) / CASE
                    WHEN 
                    (
                        SUM
                        (
                            CASE
                                WHEN (CAST((main.merchandizing_flg) AS TEXT) = CAST('Y' AS TEXT))
                                THEN main.jj_net_sales_value
                                ELSE 0::DECIMAL
                            END
                        ) OVER (PARTITION BY main.mnth_id, main.sg_brand order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 0::DECIMAL
                    )
                    THEN CAST(NULL AS DECIMAL)
                    ELSE SUM
                    (
                    CASE
                        WHEN (CAST((main.merchandizing_flg) AS TEXT) = CAST('Y' AS TEXT))
                        THEN main.jj_net_sales_value
                        ELSE 0::DECIMAL
                    END
                    ) OVER (PARTITION BY main.mnth_id, main.sg_brand order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                    END
            ),0::DECIMAL
        ) AS jj_prod_cust_sls_contrb_for_merchndsng_per_m,
        COALESCE
        (
            (
                (
                    CASE
                        WHEN (CAST((main.merchandizing_flg) AS TEXT) = CAST('Y' AS TEXT))
                        THEN main.jj_net_sales_value
                        ELSE 0::DECIMAL
                    END * 1000000::DECIMAL
                ) / CASE
                        WHEN 
                        (
                            SUM
                            (
                                CASE
                                WHEN (CAST((main.merchandizing_flg) AS TEXT) = CAST('Y' AS TEXT))
                                THEN main.jj_net_sales_value
                                ELSE 0::DECIMAL
                                END
                            ) OVER (PARTITION BY main.mnth_id, main.sg_banner order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = 0::DECIMAL
                        )
                        THEN CAST(NULL AS DECIMAL)
                        ELSE SUM
                        (
                            CASE
                                WHEN (CAST((main.merchandizing_flg) AS TEXT) = CAST('Y' AS TEXT))
                                THEN main.jj_net_sales_value
                                ELSE 0::DECIMAL
                            END
                        ) OVER (PARTITION BY main.mnth_id, main.sg_banner order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                    END
            ),0::DECIMAL
        ) AS jj_cust_prod_sls_contrb_for_merchndsng_per_m
        FROM  main
),
derived_table1 as
(
    SELECT
        TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM')AS file_period,
        CAST
        (
            (TO_CHAR(CAST(CAST((TO_DATE((SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) 
            || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)),CAST('MONYY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
            CAST('YYYYMM' AS TEXT))) AS INT
        ) AS sheet_period,
        UPPER(CAST((t1.month_of_activity) AS TEXT)) AS month_of_activity,
        t1.customer,
        t1.brand,
        t1.gl_account,
        SUM(t1.jnj_total_committed_w_o_gst) AS tp_val
        FROM itg_sg_tp_closed_month AS t1
        WHERE

        (
            (
                (UPPER(CAST((t1.customer_l1) AS TEXT)) = CAST('ZUELLIG' AS TEXT))
                AND 
                (
                    CAST((CAST((TO_CHAR(CAST(CAST((TO_DATE((SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)),
    CAST('MONYY' AS TEXT) )) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),CAST('YYYYMM' AS TEXT))) AS INT)) AS TEXT)
                        = TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM')
                )
                )
                AND ((NOT t1.supporting IS NULL)
                AND((CAST((t1.supporting) AS TEXT) <> CAST('' AS TEXT)))
                )
                AND ( UPPER(CAST((t1.month_of_activity) AS TEXT)) 
                = UPPER(CAST((t1.month_number) AS TEXT)))
        )
        GROUP BY
        TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM'),
        
        CAST
        (
            (TO_CHAR(CAST(CAST((TO_DATE((SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) 
            || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)),CAST('MONYY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
            CAST('YYYYMM' AS TEXT))) AS INT
        ),
        t1.month_of_activity,
        t1.customer,
        t1.brand,
        t1.gl_account

)
,
derived_table2_temp1 as
(
    SELECT
        TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM')AS file_period,
        CAST
        (
            (TO_CHAR(CAST(CAST((TO_DATE((SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) 
            || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)),CAST('MONYY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
            CAST('YYYYMM' AS TEXT))) AS INT
        ) AS sheet_period,
        UPPER(CAST((t1.month_of_activity) AS TEXT))::varchar AS month_of_activity,
        t1.customer,
        t1.brand,
        t1.gl_account,
        SUM(t1.jnj_total_committed_w_o_gst) AS tp_val
    FROM itg_sg_tp_closed_month AS t1
    WHERE
        (
        (
            (
            (
                (
                (
                    (UPPER(CAST((t1.customer_l1) AS TEXT)) = CAST('ZUELLIG' AS TEXT))
                    AND 
                    (
                        CAST((CAST((TO_CHAR(CAST(CAST((TO_DATE((SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)),
                CAST('MONYY' AS TEXT) )) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),CAST('YYYYMM' AS TEXT))) AS INT)) AS TEXT)
                            = TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM')
                    )
                )
                AND 
                (
                    TO_CHAR
                    (
                    CAST(CAST((TO_DATE(UPPER(CAST((t1.month_number) AS TEXT)), 
                    CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                    CAST('MM' AS TEXT)
                    ) <> CAST('01' AS TEXT)
                )
                )
                AND 
                (
                    (NOT t1.supporting IS NULL)
                    AND((CAST((t1.supporting) AS TEXT) <> CAST('' AS TEXT)))
                )
            )
            AND
            (
                (t1.committed_accrual_w_o_gst = CAST((0) AS DECIMAL(17, 3)))
                OR ( t1.committed_accrual_w_o_gst IS NULL)
            )
            )
            AND 
            (
            TO_CHAR
            (CAST(CAST((TO_DATE(UPPER(CAST((t1.month_of_activity) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                CAST('YYYY' AS TEXT)
            ) = 
            TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM')
            )
        )
        AND
        (
            TO_CHAR
            (
            CAST(CAST((TO_DATE(UPPER(CAST((t1.month_of_activity) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
            CAST('YYYYMM' AS TEXT)
            ) < 
            TO_CHAR
            (
            CAST(CAST((TO_DATE(UPPER(CAST((t1.month_number) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
            CAST('YYYYMM' AS TEXT)
            )
        )
        )
    GROUP BY
     TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM'),
    CAST
    ( 
        (
            TO_CHAR
            (
                CAST(CAST((TO_DATE(
                    (
                        SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) 
                        || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)
                    ),
                    CAST('MONYY' AS TEXT)
                    )
                ) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                CAST('YYYYMM' AS TEXT)
            )
        ) 
        AS INT
    ),
    t1.month_of_activity,
    t1.customer,
    t1.brand,
    t1.gl_account
),
derived_table2_temp2 as
(
    SELECT
        TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM')AS file_period,
        CAST
        (
            (TO_CHAR(CAST(CAST((TO_DATE((SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) 
            || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)),CAST('MONYY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
            CAST('YYYYMM' AS TEXT))) AS INT
        ) AS sheet_period,
        UPPER(CAST((t1.month_of_activity) AS TEXT))::varchar AS month_of_activity,
        t1.customer,
        t1.brand,
        t1.gl_account,
        SUM((CAST((-1) AS DECIMAL) * t1.jnj_actuals_w_o_gst)) AS tp_val
    FROM itg_sg_tp_closed_month AS t1
    WHERE
        (
        (
            (
            (
                (
                (
                    (UPPER(CAST((t1.customer_l1) AS TEXT)) = CAST('ZUELLIG' AS TEXT))
                    AND 
                    (
                        CAST((CAST((TO_CHAR(CAST(CAST((TO_DATE((SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)),
            CAST('MONYY' AS TEXT) )) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),CAST('YYYYMM' AS TEXT))) AS INT)) AS TEXT)
                            <> TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM')
                    )
                )
                AND 
                (
                    TO_CHAR
                    (
                    CAST(CAST((TO_DATE(UPPER(CAST((t1.month_number) AS TEXT)), 
                    CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                    CAST('MM' AS TEXT)
                    ) <> CAST('01' AS TEXT)
                )
                )
                AND 
                (
                    (NOT t1.supporting IS NULL)
                    AND((CAST((t1.supporting) AS TEXT) <> CAST('' AS TEXT)))
                )
            )
            AND
            (
                (t1.committed_accrual_w_o_gst = CAST((0) AS DECIMAL(17, 3)))
                OR ( t1.committed_accrual_w_o_gst IS NULL)
            )
            )
            AND 
            (
            TO_CHAR
            (CAST(CAST((TO_DATE(UPPER(CAST((t1.month_of_activity) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                CAST('YYYY' AS TEXT)
            ) = 
            TO_CHAR
            (
                CAST(CAST((TO_DATE(UPPER(CAST((t1.month_number) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                CAST('YYYY' AS TEXT)
            )
            )
        )
        AND
        (
            TO_CHAR
            (
            CAST(CAST((TO_DATE(UPPER(CAST((t1.month_of_activity) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
            CAST('YYYYMM' AS TEXT)
            ) < 
            TO_CHAR
            (
            CAST(CAST((TO_DATE(UPPER(CAST((t1.month_number) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
            CAST('YYYYMM' AS TEXT)
            )
        )
        )
    GROUP BY
    TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM'),
    CAST
    ( 
        (
            TO_CHAR
            (
                CAST(CAST((TO_DATE(
                    (
                        SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) 
                        || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)
                    ),
                    CAST('MONYY' AS TEXT)
                    )
                ) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                CAST('YYYYMM' AS TEXT)
            )
        ) 
        AS INT
    ),
    t1.month_of_activity,
    t1.customer,
    t1.brand,
    t1.gl_account
),
derived_table3_temp1 as(

    SELECT
        TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM')AS file_period,
        CAST
        (
            (TO_CHAR(CAST(CAST((TO_DATE((SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) 
            || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)),CAST('MONYY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
            CAST('YYYYMM' AS TEXT))) AS INT
        ) AS sheet_period,
        UPPER(CAST((t1.month_of_activity) AS TEXT))::varchar AS month_of_activity,
        t1.customer,
        t1.brand,
        t1.gl_account,
        SUM(t1.jnj_total_committed_w_o_gst) AS tp_val
    FROM itg_sg_tp_closed_month AS t1
    WHERE
        (
        (
            (
            (
                (
                (
                    (UPPER(CAST((t1.customer_l1) AS TEXT)) = CAST('ZUELLIG' AS TEXT))
                    AND 
                    (
                        CAST((CAST((TO_CHAR(CAST(CAST((TO_DATE((SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)),
        CAST('MONYY' AS TEXT) )) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),CAST('YYYYMM' AS TEXT))) AS INT)) AS TEXT)
                            = TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM')
                    )
                )
                AND 
                (
                    TO_CHAR
                    (
                    CAST(CAST((TO_DATE(UPPER(CAST((t1.month_number) AS TEXT)), 
                    CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                    CAST('MM' AS TEXT)
                    ) <> CAST('01' AS TEXT)
                )
                )
                AND 
                (
                    (NOT t1.supporting IS NULL)
                    AND((CAST((t1.supporting) AS TEXT) <> CAST('' AS TEXT)))
                )
            )
            AND
            (
                (t1.committed_accrual_w_o_gst <> CAST((0) AS DECIMAL(17, 3)))
                AND (NOT t1.committed_accrual_w_o_gst IS NULL)
            )
            )
            AND 
            (
            TO_CHAR
            (CAST(CAST((TO_DATE(UPPER(CAST((t1.month_of_activity) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                CAST('YYYY' AS TEXT)
            ) = 
            TO_CHAR
            (
                CAST(CAST((TO_DATE(UPPER(CAST((t1.month_number) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                CAST('YYYY' AS TEXT)
            )
            )
        )
        AND
        (
            TO_CHAR
            (
            CAST(CAST((TO_DATE(UPPER(CAST((t1.month_of_activity) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
            CAST('YYYYMM' AS TEXT)
            ) < 
            TO_CHAR
            (
            CAST(CAST((TO_DATE(UPPER(CAST((t1.month_number) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
            CAST('YYYYMM' AS TEXT)
            )
        )
        )
    GROUP BY
    TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM'),
    CAST
    ( 
        (
            TO_CHAR
            (
                CAST(CAST((TO_DATE(
                    (
                        SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) 
                        || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)
                    ),
                    CAST('MONYY' AS TEXT)
                    )
                ) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                CAST('YYYYMM' AS TEXT)
            )
        ) 
        AS INT
    ),
    t1.month_of_activity,
    t1.customer,
    t1.brand,
    t1.gl_account
),
derived_table3_temp2 as
(
     SELECT
        TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM')AS file_period,
        CAST
        (
            (TO_CHAR(CAST(CAST((TO_DATE((SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) 
            || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)),CAST('MONYY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
            CAST('YYYYMM' AS TEXT))) AS INT
        ) AS sheet_period,
        UPPER(CAST((t1.month_of_activity) AS TEXT))::varchar AS month_of_activity,
        t1.customer,
        t1.brand,
        t1.gl_account,
        SUM((CAST((-1) AS DECIMAL) * t1.jnj_actuals_w_o_gst)) AS tp_val
    FROM itg_sg_tp_closed_month AS t1
    WHERE
        (
        (
            (
            (
                (
                (
                    (UPPER(CAST((t1.customer_l1) AS TEXT)) = CAST('ZUELLIG' AS TEXT))
                    AND 
                    (
                        CAST((CAST((TO_CHAR(CAST(CAST((TO_DATE((SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)),
                CAST('MONYY' AS TEXT) )) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),CAST('YYYYMM' AS TEXT))) AS INT)) AS TEXT)
                            <> TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM')
                    )
                )
                AND 
                (
                    TO_CHAR
                    (
                    CAST(CAST((TO_DATE(UPPER(CAST((t1.month_number) AS TEXT)), 
                    CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                    CAST('MM' AS TEXT)
                    ) <> CAST('01' AS TEXT)
                )
                )
                AND 
                (
                    (NOT t1.supporting IS NULL)
                    AND((CAST((t1.supporting) AS TEXT) <> CAST('' AS TEXT)))
                )
            )
            AND
            (
                (t1.committed_accrual_w_o_gst <> CAST((0) AS DECIMAL(17, 3)))
                AND ( NOT t1.committed_accrual_w_o_gst IS NULL)
            )
            )
            AND 
            (
            TO_CHAR
            (CAST(CAST((TO_DATE(UPPER(CAST((t1.month_of_activity) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                CAST('YYYY' AS TEXT)
            ) = 
            TO_CHAR
            (
                CAST(CAST((TO_DATE(UPPER(CAST((t1.month_number) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                CAST('YYYY' AS TEXT)
            )
            )
        )
        AND
        (
            TO_CHAR
            (
            CAST(CAST((TO_DATE(UPPER(CAST((t1.month_of_activity) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
            CAST('YYYYMM' AS TEXT)
            ) < 
            TO_CHAR
            (
            CAST(CAST((TO_DATE(UPPER(CAST((t1.month_number) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
            CAST('YYYYMM' AS TEXT)
            )
        )
        )
    GROUP BY
     TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM'),
    CAST
    ( 
        (
            TO_CHAR
            (
                CAST(CAST((TO_DATE(
                    (
                        SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) 
                        || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)
                    ),
                    CAST('MONYY' AS TEXT)
                    )
                ) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                CAST('YYYYMM' AS TEXT)
            )
        ) 
        AS INT
    ),
    t1.month_of_activity,
    t1.customer,
    t1.brand,
    t1.gl_account   
),
derived_table4_temp1 as
(
   SELECT
    TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM')AS file_period,
        CAST((TO_CHAR( CAST(CAST((TO_DATE(SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 4), CAST('YYYY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
        CAST('YYYY' AS TEXT))) AS INT) AS sheet_year,
        UPPER(CAST((t1.month_of_actual) AS TEXT))::varchar AS monthof_actual,
        t1.customer,
        t1.brand,
        t1.gl_account,
        SUM(t1.tp_impact) AS tp_val
    FROM itg_sg_tp_closed_year_bal AS t1
    WHERE
        (
        (UPPER(CAST((t1.customer_l1) AS TEXT)) = CAST('ZUELLIG' AS TEXT))
        AND 
        (
            UPPER(CAST((t1.month_of_actual) AS TEXT)) = UPPER(CAST((t1.month_number) AS TEXT))
        )
        )
    GROUP BY
    TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM'),
    CAST((TO_CHAR( CAST(CAST((TO_DATE(SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 4), CAST('YYYY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
    CAST('YYYY' AS TEXT))) AS INT),
    UPPER(CAST((t1.month_of_actual) AS TEXT))::varchar,
    t1.customer,
    t1.brand,
    t1.gl_account
     
),
derived_table4_temp2 as
(
   SELECT
    TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM')AS file_period,
        CAST((TO_CHAR( CAST(CAST((TO_DATE(SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 4), CAST('YYYY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
        CAST('YYYY' AS TEXT))) AS INT) AS sheet_year,
        UPPER(CAST((t1.month_of_reversal) AS TEXT))::varchar AS monthof_actual,
        t1.customer,
        t1.brand,
        t1.gl_account,
        SUM((CAST((-1) AS DECIMAL) * t1.comments_or_reversed_accrued_amt)) AS tp_val
    FROM itg_sg_tp_closed_year_bal AS t1
    WHERE
        (
        (UPPER(CAST((t1.customer_l1) AS TEXT)) = CAST('ZUELLIG' AS TEXT))
        AND 
        (
            UPPER(CAST((t1.month_of_reversal) AS TEXT)) = UPPER(CAST((t1.month_number) AS TEXT))
        )
        )
    GROUP BY
    TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM'),
    CAST((TO_CHAR( CAST(CAST((TO_DATE(SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 4), CAST('YYYY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
    CAST('YYYY' AS TEXT))) AS INT),
    UPPER(CAST((t1.month_of_reversal) AS TEXT))::varchar,
    t1.customer,
    t1.brand,
    t1.gl_account
     
),
derived_table5_temp1 as
(
    SELECT
    TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM')AS file_period,
    CAST
    (
        (TO_CHAR(CAST(CAST((TO_DATE((SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) 
        || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)),CAST('MONYY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
        CAST('YYYYMM' AS TEXT))) AS INT
    ) AS sheet_period,
    UPPER(CAST((t1.month_of_activity) AS TEXT)) AS month_of_activity,
    t1.customer,
    t1.brand,
    t1.gl_account,
    SUM(t1.jnj_total_committed_w_o_gst) AS tp_val
    FROM itg_sg_tp_closed_month AS t1
    WHERE

    (
        (
            (UPPER(CAST((t1.customer_l1) AS TEXT)) = CAST('ZUELLIG' AS TEXT))
            AND 
            (
                CAST((CAST((TO_CHAR(CAST(CAST((TO_DATE((SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)),
                CAST('MONYY' AS TEXT) )) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),CAST('YYYYMM' AS TEXT))) AS INT)) AS TEXT)
                    = TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM')
            )
            )
            AND 
            (
                TO_CHAR(CAST(CAST((TO_DATE(UPPER(CAST((t1.month_of_activity ) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                    CAST('YYYY' AS TEXT)
                ) < TO_CHAR(
                    CAST(CAST((TO_DATE(UPPER(CAST((t1.month_number) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                    CAST('YYYY' AS TEXT)
                )
            )
    )
    GROUP BY
    TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM'),
    CAST
    (
        (TO_CHAR(CAST(CAST((TO_DATE((SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) 
        || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)),CAST('MONYY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
        CAST('YYYYMM' AS TEXT))) AS INT
    ),
    t1.month_of_activity,
    t1.customer,
    t1.brand,
    t1.gl_account

),
derived_table5_temp2 as
(
     SELECT
        TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM')AS file_period,
        CAST
        (
            (TO_CHAR(CAST(CAST((TO_DATE((SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) 
            || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)),CAST('MONYY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
            CAST('YYYYMM' AS TEXT))) AS INT
        ) AS sheet_period,
        UPPER(CAST((t1.month_of_activity) AS TEXT))::varchar AS month_of_activity,
        t1.customer,
        t1.brand,
        t1.gl_account,
        SUM((CAST((-1) AS DECIMAL) * t1.jnj_actuals_w_o_gst)) AS tp_val
    FROM itg_sg_tp_closed_month AS t1
    WHERE
        (
            (
                (
                    (
                        (UPPER(CAST((t1.customer_l1) AS TEXT)) = CAST('ZUELLIG' AS TEXT))
                        AND 
                        (
                            CAST((CAST((TO_CHAR(CAST(CAST((TO_DATE((SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)),
                            CAST('MONYY' AS TEXT) )) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),CAST('YYYYMM' AS TEXT))) AS INT)) AS TEXT)
                                <> TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM')
                        )
                    )
                    AND 
                    (
                        TO_CHAR
                        (
                        CAST(CAST((TO_DATE(UPPER(CAST((t1.month_number) AS TEXT)), 
                        CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                        CAST('MM' AS TEXT)
                        ) <> CAST('01' AS TEXT)
                    )
                )
            )
            AND
            (
                TO_CHAR
                (
                CAST(CAST((TO_DATE(UPPER(CAST((t1.month_of_activity) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                CAST('YYYYMM' AS TEXT)
                ) < 
                TO_CHAR
                (
                CAST(CAST((TO_DATE(UPPER(CAST((t1.month_number) AS TEXT)), CAST('MON-YY' AS TEXT))) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                CAST('YYYYMM' AS TEXT)
                )
            )
        )
    GROUP BY
     TO_CHAR(TO_DATE(t1.month_number, 'MON-YY'), 'YYYYMM'),
    CAST
    ( 
        (
            TO_CHAR
            (
                CAST(CAST((TO_DATE(
                    (
                        SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 4, 3) 
                        || SUBSTRING(UPPER(CAST((t1.sheet_name) AS TEXT)), 8, 2)
                    ),
                    CAST('MONYY' AS TEXT)
                    )
                ) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                CAST('YYYYMM' AS TEXT)
            )
        ) 
        AS INT
    ),
    t1.month_of_activity,
    t1.customer,
    t1.brand,
    t1.gl_account   
),
tp_brand_map as
(
    SELECT DISTINCT
        ITG_SG_CONSTANT_KEY_VALUE.key,
        ITG_SG_CONSTANT_KEY_VALUE.value
    FROM snaposeitg_integration.ITG_SG_CONSTANT_KEY_VALUE AS ITG_SG_CONSTANT_KEY_VALUE
    WHERE
        (ITG_SG_CONSTANT_KEY_VALUE.data_category_cd = CAST((7) AS DECIMAL)) 
),
tp_brand_exclusion as(
    SELECT DISTINCT
        ITG_SG_CONSTANT_KEY_VALUE.key
    FROM snaposeitg_integration.ITG_SG_CONSTANT_KEY_VALUE AS ITG_SG_CONSTANT_KEY_VALUE
    WHERE
    (
        (
        ITG_SG_CONSTANT_KEY_VALUE.data_category_cd = CAST((8) AS DECIMAL)
        )
        AND (CAST((ITG_SG_CONSTANT_KEY_VALUE.value) AS TEXT) = CAST('Y' AS TEXT))
    )
),
derived_table2 as 
(
    select * from derived_table2_temp1
    UNION ALL
    select * from derived_table2_temp2 
),
derived_table3 as(
    select * from derived_table3_temp1
    UNION ALL
    select * from derived_table3_temp2
),
derived_table4 as(
    select * from derived_table4_temp1
    UNION ALL
    select * from derived_table4_temp2
),
derived_table5 as(
    select * from derived_table5_temp1
    UNION ALL
    select * from derived_table5_temp2
),
main2 as
(
  (
    (
        (
                SELECT
                CAST((derived_table1.file_period) AS VARCHAR) AS file_period,
                derived_table1.customer,
                derived_table1.brand,
                derived_table1.gl_account,
                derived_table1.tp_val
            FROM derived_table1
            UNION ALL
            SELECT
                derived_table2.file_period,
                derived_table2.customer,
                derived_table2.brand,
                derived_table2.gl_account,
                SUM(derived_table2.tp_val) AS tp_val
            FROM 
            derived_table2
            GROUP BY
                derived_table2.file_period,
                derived_table2.customer,
                derived_table2.brand,
                derived_table2.gl_account
        )
        UNION ALL
        SELECT
        derived_table3.file_period,
        derived_table3.customer,
        derived_table3.brand,
        derived_table3.gl_account,
        SUM(derived_table3.tp_val) AS tp_val
        FROM derived_table3
        GROUP BY
        derived_table3.file_period,
        derived_table3.customer,
        derived_table3.brand,
        derived_table3.gl_account
    )
    UNION ALL
    SELECT
        derived_table4.file_period,
        derived_table4.customer,
        derived_table4.brand,
        derived_table4.gl_account,
        SUM(derived_table4.tp_val) AS sum
    FROM 
    derived_table4
    GROUP BY
        derived_table4.file_period,
        derived_table4.customer,
        derived_table4.brand,
        derived_table4.gl_account
    )
    UNION ALL
    SELECT
    derived_table5.file_period,
    derived_table5.customer,
    derived_table5.brand,
    derived_table5.gl_account,
    SUM(derived_table5.tp_val) AS sum
    FROM  derived_table5
    GROUP BY
    derived_table5.file_period,
    derived_table5.customer,
    derived_table5.brand,
    derived_table5.gl_account

)
,
customer_mapping as
(
    SELECT DISTINCT
    itg_sg_zuellig_customer_mapping.zuellig_customer,
    itg_sg_zuellig_customer_mapping.regional_banner,
    itg_sg_zuellig_customer_mapping.merchandizing,
    itg_sg_zuellig_customer_mapping.cdl_dttm,
    itg_sg_zuellig_customer_mapping.crtd_dttm,
    itg_sg_zuellig_customer_mapping.updt_dttm
    FROM itg_sg_zuellig_customer_mapping
),
gl_mapping as 
(
    SELECT
        itg_sg_ciw_mapping.gl,
        itg_sg_ciw_mapping.gl_description,
        itg_sg_ciw_mapping.posted_where
    FROM itg_sg_ciw_mapping
)
,
main_fact as
(
    SELECT
        main.file_period,
        COALESCE(customer_mapping.regional_banner, main.customer) AS customer,
        COALESCE(tp_brand_map.value, main.brand) AS sg_brand,
        main.gl_account,
        gl_mapping.gl_description,
        gl_mapping.posted_where,
        main.tp_val
    FROM (
        (
          ( main2 as main
    
      
            LEFT JOIN tp_brand_map
            ON UPPER(CAST((tp_brand_map.key) AS TEXT)) = UPPER(CAST((main.brand) AS TEXT))   
            LEFT JOIN divest
            ON
            (
            (CAST(( main.file_period) AS TEXT) >= CAST((divest.effective_period_from) AS TEXT))
            AND
                (
                    CAST(( main.file_period) AS TEXT) <= CAST((divest.effective_period_to) AS TEXT)
                    AND ( CAST((main.brand) AS TEXT) = CAST((divest.key) AS TEXT))
                )
            )

          )
          LEFT JOIN tp_brand_exclusion
          ON 
          (
              UPPER(CAST((main.brand) AS TEXT)) LIKE 
              (
              (
                  CAST('%' AS TEXT) || UPPER(CAST((tp_brand_exclusion.key) AS TEXT))
              ) || CAST('%' AS TEXT)
              )
          )
            
        )
        LEFT JOIN  customer_mapping
        ON
        (
            UPPER(CAST((customer_mapping.zuellig_customer) AS TEXT))
            = UPPER(CAST((main.customer) AS TEXT))
        )
    )
    LEFT JOIN gl_mapping
    ON
    (
    CAST((main.gl_account) AS TEXT) = CAST((gl_mapping.gl) AS TEXT)
    )

    WHERE divest.key IS NULL
    AND (
        tp_brand_exclusion.key IS NULL
        )

)
,
sg_brand_jj_code_map as
(
     SELECT DISTINCT
        MIN(CAST((itg_sg_zuellig_product_mapping.jj_code) AS TEXT)) AS jj_code,
        itg_sg_zuellig_product_mapping.brand
    FROM itg_sg_zuellig_product_mapping
    GROUP BY
    itg_sg_zuellig_product_mapping.brand   
),
prodmstr1 as
(
    SELECT DISTINCT
        matl.gph_reg_frnchse_grp,
        matl.gph_prod_frnchse,
        matl.gph_prod_brnd,
        sg_brand_jj_code_map.brand AS sg_brand
    FROM
    sg_brand_jj_code_map
    LEFT JOIN edw_vw_sg_material_dim AS matl
    ON LTRIM(CAST((matl.sap_matl_num) AS TEXT), CAST('0' AS TEXT)) = LTRIM(sg_brand_jj_code_map.jj_code, CAST('0' AS TEXT))
    
),
custmstr_groupby as
(
    SELECT DISTINCT
        MIN(CAST((EDW_VW_sg_CUSTOMER_DIM.sap_cust_nm) AS TEXT)) AS sap_cust_nm,
        EDW_VW_sg_CUSTOMER_DIM.sap_sls_org,
        EDW_VW_sg_CUSTOMER_DIM.sap_cmp_id,
        EDW_VW_sg_CUSTOMER_DIM.sap_cntry_cd,
        EDW_VW_sg_CUSTOMER_DIM.sap_cntry_nm,
        EDW_VW_sg_CUSTOMER_DIM.sap_prnt_cust_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_prnt_cust_desc,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_desc,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_frmt_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_frmt_desc,
        EDW_VW_sg_CUSTOMER_DIM.retail_env
    FROM EDW_VW_sg_CUSTOMER_DIM
    GROUP BY
        EDW_VW_sg_CUSTOMER_DIM.sap_sls_org,
        EDW_VW_sg_CUSTOMER_DIM.sap_cmp_id,
        EDW_VW_sg_CUSTOMER_DIM.sap_cntry_cd,
        EDW_VW_sg_CUSTOMER_DIM.sap_cntry_nm,
        EDW_VW_sg_CUSTOMER_DIM.sap_prnt_cust_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_prnt_cust_desc,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_desc,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_frmt_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_frmt_desc,
        EDW_VW_sg_CUSTOMER_DIM.retail_env
),
custmstr as
(
    SELECT DISTINCT
        edw_vw_sg_customer_dim.sap_cust_id,
        edw_vw_sg_customer_dim.sap_cust_nm,
        EDW_VW_sg_CUSTOMER_DIM.sap_sls_org,
        LTRIM(
                CAST(
                    (
                        edw_vw_sg_customer_dim.sap_cmp_id
                    ) AS TEXT
                ),
                CAST('0' AS TEXT)
            ) AS sap_cmp_id,
        EDW_VW_sg_CUSTOMER_DIM.sap_cmp_id,
        EDW_VW_sg_CUSTOMER_DIM.sap_cntry_cd,
        EDW_VW_sg_CUSTOMER_DIM.sap_cntry_nm,
        EDW_VW_sg_CUSTOMER_DIM.sap_prnt_cust_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_prnt_cust_desc,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_desc,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_frmt_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_frmt_desc,
        EDW_VW_sg_CUSTOMER_DIM.retail_env
    FROM EDW_VW_sg_CUSTOMER_DIM
),
tpoffinvoice as
(
    SELECT
        custmstr.sap_cmp_id AS co_cd,
        custmstr.sap_cntry_cd AS cntry_key,
        SUBSTRING(CAST((main_fact.file_period) AS TEXT), 1, 4) AS jj_yr,
        main_fact.file_period AS jj_mnth_id,
        main_fact.gl_account AS acct_no,
        main_fact.gl_description,
        main_fact.posted_where,
        main_fact.customer AS sg_banner,
        custmstr.sap_cust_nm,
        custmstr.sap_prnt_cust_key,
        custmstr.sap_prnt_cust_desc,
        custmstr.sap_bnr_key,
        custmstr.sap_bnr_desc,
        custmstr.sap_bnr_frmt_key,
        custmstr.sap_bnr_frmt_desc,
        custmstr.retail_env,
        main_fact.sg_brand,
        prodmstr.gph_reg_frnchse_grp,
        prodmstr.gph_prod_frnchse,
        prodmstr.gph_prod_brnd,
        main_fact.tp_val
    FROM
    (
    main_fact
    LEFT JOIN prodmstr1 as prodmstr
    ON UPPER(CAST((main_fact.sg_brand) AS TEXT)) = UPPER(CAST((prodmstr.sg_brand) AS TEXT)) 

    )
    LEFT JOIN custmstr_groupby as custmstr
    ON UPPER(CAST((custmstr.sap_bnr_frmt_desc) AS TEXT)) = UPPER(CAST((main_fact.customer) AS TEXT))

              
),
custmstr_2_groupby as
(
    SELECT DISTINCT
        MIN(CAST((
            EDW_VW_sg_CUSTOMER_DIM.sap_cust_nm
        ) AS TEXT)) AS sap_cust_nm,
        EDW_VW_sg_CUSTOMER_DIM.sap_sls_org,
        EDW_VW_sg_CUSTOMER_DIM.sap_cmp_id,
        EDW_VW_sg_CUSTOMER_DIM.sap_cntry_cd,
        EDW_VW_sg_CUSTOMER_DIM.sap_cntry_nm,
        EDW_VW_sg_CUSTOMER_DIM.sap_prnt_cust_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_prnt_cust_desc,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_desc,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_frmt_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_frmt_desc,
        EDW_VW_sg_CUSTOMER_DIM.retail_env
    FROM EDW_VW_sg_CUSTOMER_DIM
    WHERE
    (
         
        (
            CAST((LTRIM(CAST((EDW_VW_sg_CUSTOMER_DIM.sap_cust_id) AS TEXT), CAST('0' AS TEXT))) AS VARCHAR(50)) 
            IN
            (
                SELECT DISTINCT
                ITG_SG_CONSTANT_KEY_VALUE.key
                FROM snaposeitg_integration.ITG_SG_CONSTANT_KEY_VALUE AS ITG_SG_CONSTANT_KEY_VALUE
                WHERE
                (
                    (
                    ITG_SG_CONSTANT_KEY_VALUE.data_category_cd = CAST((4) AS DECIMAL)
                    )
                    AND (
                    CAST((ITG_SG_CONSTANT_KEY_VALUE.value) AS TEXT) = CAST('Y' AS TEXT)
                    )
                )
            )
        )
    )
    GROUP BY
    EDW_VW_sg_CUSTOMER_DIM.sap_sls_org,
    EDW_VW_sg_CUSTOMER_DIM.sap_cmp_id,
    EDW_VW_sg_CUSTOMER_DIM.sap_cntry_cd,
    EDW_VW_sg_CUSTOMER_DIM.sap_cntry_nm,
    EDW_VW_sg_CUSTOMER_DIM.sap_prnt_cust_key,
    EDW_VW_sg_CUSTOMER_DIM.sap_prnt_cust_desc,
    EDW_VW_sg_CUSTOMER_DIM.sap_bnr_key,
    EDW_VW_sg_CUSTOMER_DIM.sap_bnr_desc,
    EDW_VW_sg_CUSTOMER_DIM.sap_bnr_frmt_key,
    EDW_VW_sg_CUSTOMER_DIM.sap_bnr_frmt_desc,
    EDW_VW_sg_CUSTOMER_DIM.retail_env

),
custmstr_2 as
(
    SELECT DISTINCT
        EDW_VW_sg_CUSTOMER_DIM.sap_cust_nm AS sap_cust_nm,
        EDW_VW_sg_CUSTOMER_DIM.sap_sls_org,
        EDW_VW_sg_CUSTOMER_DIM.sap_cmp_id,
        EDW_VW_sg_CUSTOMER_DIM.sap_cntry_cd,
        EDW_VW_sg_CUSTOMER_DIM.sap_cntry_nm,
        EDW_VW_sg_CUSTOMER_DIM.sap_prnt_cust_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_prnt_cust_desc,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_desc,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_frmt_key,
        EDW_VW_sg_CUSTOMER_DIM.sap_bnr_frmt_desc,
        EDW_VW_sg_CUSTOMER_DIM.retail_env
    FROM EDW_VW_sg_CUSTOMER_DIM
    WHERE
    (
         
        
            CAST((LTRIM(CAST((EDW_VW_sg_CUSTOMER_DIM.sap_cust_id) AS TEXT), CAST('0' AS TEXT))) AS VARCHAR(50)) 
            IN
            (
                SELECT DISTINCT
                ITG_SG_CONSTANT_KEY_VALUE.key
                FROM snaposeitg_integration.ITG_SG_CONSTANT_KEY_VALUE AS ITG_SG_CONSTANT_KEY_VALUE
                WHERE
                (
                    (
                    ITG_SG_CONSTANT_KEY_VALUE.data_category_cd = CAST((4) AS DECIMAL)
                    )
                    AND (
                    CAST((ITG_SG_CONSTANT_KEY_VALUE.value) AS TEXT) = CAST('Y' AS TEXT)
                    )
                )
            )
        
    )
    
),
sellin as
(
    SELECT
        edw_vw_sg_sellin_sales_fact.co_cd,
        edw_vw_sg_sellin_sales_fact.cntry_nm AS cntry_key,
        edw_vw_sg_sellin_sales_fact.fisc_yr AS jj_yr,
        edw_vw_sg_sellin_sales_fact.jj_mnth_id,
        LTRIM(CAST((edw_vw_sg_sellin_sales_fact.item_cd) AS TEXT), CAST('0' AS TEXT)) AS item_cd,
        edw_vw_sg_sellin_sales_fact.acct_no,
        SUM(edw_vw_sg_sellin_sales_fact.base_val) AS base_val,
        SUM(edw_vw_sg_sellin_sales_fact.sls_qty) AS sls_qty,
        SUM(edw_vw_sg_sellin_sales_fact.ret_qty) AS ret_qty
    FROM edw_vw_sg_sellin_sales_fact
    WHERE(
    
            CAST( edw_vw_sg_sellin_sales_fact.cntry_nm AS TEXT) = CAST('SG' AS TEXT)
            AND 
            ( CAST((LTRIM(CAST((edw_vw_sg_sellin_sales_fact.cust_id) AS TEXT), CAST((0) AS TEXT))) AS VARCHAR(50)) 
              IN 
                (
                    SELECT DISTINCT
                    ITG_SG_CONSTANT_KEY_VALUE.key
                    FROM snaposeitg_integration.ITG_SG_CONSTANT_KEY_VALUE AS ITG_SG_CONSTANT_KEY_VALUE
                    WHERE
                    (
                        (
                        ITG_SG_CONSTANT_KEY_VALUE.data_category_cd = CAST((4) AS DECIMAL)
                        )
                        AND (
                        CAST((ITG_SG_CONSTANT_KEY_VALUE.value) AS TEXT) = CAST('Y' AS TEXT)
                        )
                    )
                )
            )
            )
        
    GROUP BY
        edw_vw_sg_sellin_sales_fact.co_cd,
        edw_vw_sg_sellin_sales_fact.cntry_nm,
        edw_vw_sg_sellin_sales_fact.fisc_yr,
        edw_vw_sg_sellin_sales_fact.jj_mnth_id,
        LTRIM(CAST((edw_vw_sg_sellin_sales_fact.item_cd) AS TEXT), CAST('0' AS TEXT)),
        edw_vw_sg_sellin_sales_fact.acct_no
),
prodmap as
(
    SELECT DISTINCT
        LTRIM(CAST((t1.jj_code) AS TEXT), CAST('0' AS TEXT)) AS sap_item_code,
        t1.brand AS sg_brand
    FROM (
        SELECT DISTINCT
        itg_sg_zuellig_product_mapping.jj_code,
        itg_sg_zuellig_product_mapping.brand
        FROM itg_sg_zuellig_product_mapping
    ) AS t1,
    (
        SELECT DISTINCT
        edw_vw_sg_material_dim.sap_matl_num,
        edw_vw_sg_material_dim.sap_brnd_desc,
        edw_vw_sg_material_dim.gph_reg_frnchse_grp,
        edw_vw_sg_material_dim.gph_prod_frnchse,
        edw_vw_sg_material_dim.gph_prod_brnd
        FROM edw_vw_sg_material_dim
    ) AS t2
    WHERE
        (
        LTRIM(CAST((t1.jj_code) AS TEXT), CAST('0' AS TEXT)) 
        = LTRIM(CAST((t2.sap_matl_num) AS TEXT), CAST('0' AS TEXT))
        )
),
prodmstr as
(
    SELECT
        edw_vw_sg_material_dim.cntry_key,
        edw_vw_sg_material_dim.sap_matl_num,
        edw_vw_sg_material_dim.sap_mat_desc,
        edw_vw_sg_material_dim.ean_num,
        edw_vw_sg_material_dim.sap_mat_type_cd,
        edw_vw_sg_material_dim.sap_mat_type_desc,
        edw_vw_sg_material_dim.sap_base_uom_cd,
        edw_vw_sg_material_dim.sap_prchse_uom_cd,
        edw_vw_sg_material_dim.sap_prod_sgmt_cd,
        edw_vw_sg_material_dim.sap_prod_sgmt_desc,
        edw_vw_sg_material_dim.sap_base_prod_cd,
        edw_vw_sg_material_dim.sap_base_prod_desc,
        edw_vw_sg_material_dim.sap_mega_brnd_cd,
        edw_vw_sg_material_dim.sap_mega_brnd_desc,
        edw_vw_sg_material_dim.sap_brnd_cd,
        edw_vw_sg_material_dim.sap_brnd_desc,
        edw_vw_sg_material_dim.sap_vrnt_cd,
        edw_vw_sg_material_dim.sap_vrnt_desc,
        edw_vw_sg_material_dim.sap_put_up_cd,
        edw_vw_sg_material_dim.sap_put_up_desc,
        edw_vw_sg_material_dim.sap_grp_frnchse_cd,
        edw_vw_sg_material_dim.sap_grp_frnchse_desc,
        edw_vw_sg_material_dim.sap_frnchse_cd,
        edw_vw_sg_material_dim.sap_frnchse_desc,
        edw_vw_sg_material_dim.sap_prod_frnchse_cd,
        edw_vw_sg_material_dim.sap_prod_frnchse_desc,
        edw_vw_sg_material_dim.sap_prod_mjr_cd,
        edw_vw_sg_material_dim.sap_prod_mjr_desc,
        edw_vw_sg_material_dim.sap_prod_mnr_cd,
        edw_vw_sg_material_dim.sap_prod_mnr_desc,
        edw_vw_sg_material_dim.sap_prod_hier_cd,
        edw_vw_sg_material_dim.sap_prod_hier_desc,
        edw_vw_sg_material_dim.gph_region,
        edw_vw_sg_material_dim.gph_reg_frnchse,
        edw_vw_sg_material_dim.gph_reg_frnchse_grp,
        edw_vw_sg_material_dim.gph_prod_frnchse,
        edw_vw_sg_material_dim.gph_prod_brnd,
        edw_vw_sg_material_dim.gph_prod_sub_brnd,
        edw_vw_sg_material_dim.gph_prod_vrnt,
        edw_vw_sg_material_dim.gph_prod_needstate,
        edw_vw_sg_material_dim.gph_prod_ctgry,
        edw_vw_sg_material_dim.gph_prod_subctgry,
        edw_vw_sg_material_dim.gph_prod_sgmnt,
        edw_vw_sg_material_dim.gph_prod_subsgmnt,
        edw_vw_sg_material_dim.gph_prod_put_up_cd,
        edw_vw_sg_material_dim.gph_prod_put_up_desc,
        edw_vw_sg_material_dim.gph_prod_size,
        edw_vw_sg_material_dim.gph_prod_size_uom,
        edw_vw_sg_material_dim.launch_dt,
        edw_vw_sg_material_dim.qty_shipper_pc,
        edw_vw_sg_material_dim.prft_ctr,
        edw_vw_sg_material_dim.shlf_life
    FROM edw_vw_sg_material_dim
),
a as
(
    SELECT
        sellin.co_cd,
        sellin.cntry_key,
        sellin.jj_yr,
        sellin.jj_mnth_id,
        sellin.item_cd,
        sellin.acct_no,
        glmap.gl_description,
        glmap.posted_where,
        prodmap.sg_brand,
        prodmstr.gph_reg_frnchse_grp,
        prodmstr.gph_prod_frnchse,
        prodmstr.gph_prod_brnd,
        prodmstr.sap_brnd_desc,
        custmstr.sap_cust_nm,
        custmstr.sap_prnt_cust_key,
        custmstr.sap_prnt_cust_desc,
        custmstr.sap_bnr_key,
        custmstr.sap_bnr_desc,
        custmstr.sap_bnr_frmt_key,
        custmstr.sap_bnr_frmt_desc,
        custmstr.retail_env,
        sellin.base_val,
        sellin.sls_qty,
        sellin.ret_qty
    FROM custmstr_2_groupby as custmstr,
    (
        (
            ( 
                sellin
                LEFT JOIN prodmap
                ON sellin.item_cd = prodmap.sap_item_code  
            )
            LEFT JOIN prodmstr
            ON sellin.item_cd = CAST((prodmstr.sap_matl_num) AS TEXT)
        )
        LEFT JOIN itg_sg_ciw_mapping AS glmap
        ON CAST((sellin.acct_no) AS TEXT) = CAST((glmap.gl) AS TEXT)
    )
    WHERE sellin.jj_mnth_id IN 
    (
        SELECT DISTINCT itg_sg_zuellig_sellout.period FROM itg_sg_zuellig_sellout
    )

),
brand_gap_fill as
(
    SELECT DISTINCT
        MIN(CAST((t1.brand) AS TEXT)) AS sg_brand,
        t2.sap_brnd_desc,
        MAX(CAST((t2.gph_reg_frnchse_grp) AS TEXT)) AS gph_reg_frnchse_grp,
        MAX(CAST((t2.gph_prod_frnchse
        ) AS TEXT)) AS gph_prod_frnchse,
        MAX(CAST((t2.gph_prod_brnd) AS TEXT)) AS gph_prod_brnd
    FROM
    (
        SELECT DISTINCT
        itg_sg_zuellig_product_mapping.jj_code,
        itg_sg_zuellig_product_mapping.brand
        FROM itg_sg_zuellig_product_mapping
    ) AS t1,
    (
        SELECT DISTINCT
        edw_vw_sg_material_dim.sap_matl_num,
        edw_vw_sg_material_dim.sap_brnd_desc,
        edw_vw_sg_material_dim.gph_reg_frnchse_grp,
        edw_vw_sg_material_dim.gph_prod_frnchse,
        edw_vw_sg_material_dim.gph_prod_brnd
        FROM edw_vw_sg_material_dim
    ) AS t2
    WHERE
        (
        LTRIM(CAST((t1.jj_code) AS TEXT), CAST('0' AS TEXT)) =
         LTRIM(CAST((t2.sap_matl_num) AS TEXT), CAST('0' AS TEXT))
        )
    GROUP BY
        t2.sap_brnd_desc
),
zsellin as
(
    SELECT
        a.co_cd,
        a.cntry_key,
        a.jj_yr,
        a.jj_mnth_id,
        a.acct_no,
        a.gl_description,
        a.posted_where,
        COALESCE(a.sg_brand, CAST((brand_gap_fill.sg_brand) AS VARCHAR)) AS sg_brand,
        COALESCE(a.gph_reg_frnchse_grp, CAST(( brand_gap_fill.gph_reg_frnchse_grp) AS VARCHAR)) AS gph_reg_frnchse_grp,
        COALESCE(a.gph_prod_frnchse, CAST((brand_gap_fill.gph_prod_frnchse) AS VARCHAR)) AS gph_prod_frnchse,
        COALESCE(a.gph_prod_brnd, CAST((brand_gap_fill.gph_prod_brnd) AS VARCHAR)) AS gph_prod_brnd,
        a.sap_cust_nm,
        a.sap_prnt_cust_key,
        a.sap_prnt_cust_desc,
        a.sap_bnr_key,
        a.sap_bnr_desc,
        a.sap_bnr_frmt_key,
        a.sap_bnr_frmt_desc,
        a.retail_env,
        SUM(a.base_val) AS base_val,
        SUM(a.sls_qty) AS sls_qty,
        SUM(a.ret_qty) AS ret_qty
    FROM 
        a
        LEFT JOIN brand_gap_fill
        ON CAST((a.sap_brnd_desc) AS TEXT) = CAST((brand_gap_fill.sap_brnd_desc ) AS TEXT)
    GROUP BY
        a.co_cd,
        a.cntry_key,
        a.jj_yr,
        a.jj_mnth_id,
        a.acct_no,
        a.gl_description,
        a.posted_where,
        COALESCE(a.sg_brand, CAST((brand_gap_fill.sg_brand) AS VARCHAR)),
        COALESCE(a.gph_reg_frnchse_grp, CAST((brand_gap_fill.gph_reg_frnchse_grp) AS VARCHAR)),
        COALESCE(a.gph_prod_frnchse, CAST((brand_gap_fill.gph_prod_frnchse) AS VARCHAR)),
        COALESCE(a.gph_prod_brnd, CAST((brand_gap_fill.gph_prod_brnd) AS VARCHAR)),
        a.sap_cust_nm,
        a.sap_prnt_cust_key,
        a.sap_prnt_cust_desc,
        a.sap_bnr_key,
        a.sap_bnr_desc,
        a.sap_bnr_frmt_key,
        a.sap_bnr_frmt_desc,
        a.retail_env
),
zsellin_1 as
(
    SELECT
        zsellin.co_cd,
        zsellin.cntry_key,
        zsellin.jj_yr,
        zsellin.jj_mnth_id,
        zsellin.acct_no,
        zsellin.gl_description,
        zsellin.posted_where,
        zsellin.sg_brand,
        zsellin.gph_reg_frnchse_grp,
        zsellin.gph_prod_frnchse,
        zsellin.gph_prod_brnd,
        zsellin.sap_cust_nm,
        zsellin.sap_prnt_cust_key,
        zsellin.sap_prnt_cust_desc,
        zsellin.sap_bnr_key,
        zsellin.sap_bnr_desc,
        zsellin.sap_bnr_frmt_key,
        zsellin.sap_bnr_frmt_desc,
        zsellin.retail_env,
        zsellin.base_val,
        zsellin.sls_qty,
        zsellin.ret_qty
    FROM zsellin
    LEFT JOIN divest
    ON 
    (
            CAST((zsellin.jj_mnth_id) AS TEXT) >= CAST((divest.effective_period_from) AS TEXT)
        )
        AND (CAST((zsellin.jj_mnth_id) AS TEXT) <= divest.effective_period_to)
        AND (CAST((zsellin.sg_brand) AS TEXT) = CAST((divest.key) AS TEXT))
        WHERE divest.key IS NULL
),
zsellin_zsellout as
(
    SELECT
        zsellin.co_cd,
        zsellin.cntry_key,
        zsellin.jj_yr,
        zsellin.jj_mnth_id,
        zsellin.acct_no,
        zsellin.gl_description,
        zsellin.posted_where,
        zsellout.sg_banner,
        CAST((
        zsellout.sap_cust_nm
        ) AS VARCHAR) AS sap_cust_nm,
        zsellin.sap_prnt_cust_key,
        zsellin.sap_prnt_cust_desc,
        zsellin.sap_bnr_key,
        zsellin.sap_bnr_desc,
        zsellin.sap_bnr_frmt_key,
        zsellin.sap_bnr_frmt_desc,
        zsellin.retail_env,
        zsellin.sg_brand,
        zsellin.gph_reg_frnchse_grp,
        zsellin.gph_prod_frnchse,
        zsellin.gph_prod_brnd,
        (
            CAST
            (
            (( CAST((zsellin.base_val) AS DOUBLE) * CAST((zsellout.jj_prod_cust_sls_contrb_per_m) AS FLOAT))) AS FLOAT
            ) / CAST((1000000) AS DOUBLE)
        ) AS base_val
    FROM 
    zsellin_1 as zsellin,
    zsellout
    WHERE
    (
    (
        (
        CAST((zsellin.jj_mnth_id) AS TEXT) = CAST((zsellout.mnth_id) AS TEXT)
        )
        AND
        (
        CAST((zsellin.sg_brand) AS TEXT) = CAST((zsellout.sg_brand) AS TEXT)
        )
    )
    AND 
    (
        (
        UPPER(CAST((zsellin.posted_where) AS TEXT)) = CAST('TP ON-INVOICE' AS TEXT)
        )
        OR (
        UPPER(CAST((zsellin.posted_where) AS TEXT)) = CAST('COGS' AS TEXT)
        )
    )
    )
),
appr_by_cust as
(
    SELECT
        zsellin.co_cd,
        zsellin.cntry_key,
        zsellin.jj_yr,
        zsellin.jj_mnth_id,
        zsellin.acct_no,
        zsellin.gl_description,
        zsellin.posted_where,
        zsellout.sg_banner,
        zsellout.sap_cust_nm,
        zsellin.sap_prnt_cust_key,
        zsellin.sap_prnt_cust_desc,
        zsellin.sap_bnr_key,
        zsellin.sap_bnr_desc,
        zsellin.sap_bnr_frmt_key,
        zsellin.sap_bnr_frmt_desc,
        zsellin.retail_env,
        (
            (
            zsellin.base_val * 
            CASE WHEN (zsellin.acct_no IN (SELECT DISTINCT ITG_SG_CONSTANT_KEY_VALUE.key FROM snaposeitg_integration.ITG_SG_CONSTANT_KEY_VALUE AS ITG_SG_CONSTANT_KEY_VALUE 
                        WHERE
                        (               
                        ITG_SG_CONSTANT_KEY_VALUE.data_category_cd = CAST((6) AS DECIMAL)   
                        AND 
                        CAST((ITG_SG_CONSTANT_KEY_VALUE.value) AS TEXT) = CAST('Y' AS TEXT)
                        
                    )))
                THEN zsellout.jj_cust_sls_contrb_for_merchndsng_per_m
                ELSE zsellout.jj_cust_sls_contrb_per_m
            END
            ) / CAST((1000000) AS DECIMAL)
        ) AS base_val
        FROM 
        zsellin, 
        (
        SELECT
            zsellout.mnth_id,
            zsellout.sg_banner,
            zsellout.sap_cust_nm,
            MAX(zsellout.jj_cust_sls_contrb_per_m) AS jj_cust_sls_contrb_per_m,
            MAX(zsellout.jj_cust_sls_contrb_for_merchndsng_per_m) AS jj_cust_sls_contrb_for_merchndsng_per_m
        FROM zsellout
        GROUP BY
            zsellout.mnth_id,
            zsellout.sg_banner,
            zsellout.sap_cust_nm
        ) AS zsellout
        WHERE
        (
            (
            (
                CAST((
                zsellout.mnth_id) AS TEXT) = CAST((zsellin.jj_mnth_id) AS TEXT)
            )
            AND (
                (
                UPPER(CAST((zsellin.posted_where) AS TEXT)) = CAST('TT' AS TEXT)
                )
                OR (
                UPPER(CAST((zsellin.posted_where) AS TEXT)) = CAST('RETURN' AS TEXT)
                )
            )
            )
            AND (
            NOT (
                zsellin.acct_no IN (
                SELECT DISTINCT
                    ITG_SG_CONSTANT_KEY_VALUE.key
                FROM snaposeitg_integration.ITG_SG_CONSTANT_KEY_VALUE AS ITG_SG_CONSTANT_KEY_VALUE
                WHERE
                    (
                    (
                        ITG_SG_CONSTANT_KEY_VALUE.data_category_cd = CAST((5) AS DECIMAL)
                    )
                    AND (CAST((ITG_SG_CONSTANT_KEY_VALUE.value) AS TEXT) = CAST('Y' AS TEXT)
                    )
                    )
                )
            )
            )
                    )
),
cust_prod_contr as
(
    SELECT
        zsellout.mnth_id,
        zsellout.sg_banner,
        zsellout.sg_brand,
        zsellout.gph_reg_frnchse_grp,
        zsellout.gph_prod_frnchse,
        zsellout.gph_prod_brnd,
        zsellout.sap_cust_nm,
        MAX(zsellout.jj_cust_prod_sls_contrb_per_m) AS jj_cust_prod_sls_contrb_per_m,
        MAX(zsellout.jj_cust_prod_sls_contrb_for_merchndsng_per_m) AS jj_cust_prod_sls_contrb_for_merchndsng_per_m
    FROM zsellout
    GROUP BY
    zsellout.mnth_id,
    zsellout.sg_banner,
    zsellout.sg_brand,
    zsellout.gph_reg_frnchse_grp,
    zsellout.gph_prod_frnchse,
    zsellout.gph_prod_brnd,
    zsellout.sap_cust_nm
),
zuellig_appr_temp as
(
    SELECT
    appr_by_cust.co_cd,
    appr_by_cust.cntry_key,
    appr_by_cust.jj_yr,
    appr_by_cust.jj_mnth_id,
    appr_by_cust.acct_no,
    appr_by_cust.gl_description,
    appr_by_cust.posted_where,
    appr_by_cust.sg_banner,
    CAST((
    appr_by_cust.sap_cust_nm
    ) AS VARCHAR) AS sap_cust_nm,
    appr_by_cust.sap_prnt_cust_key,
    appr_by_cust.sap_prnt_cust_desc,
    appr_by_cust.sap_bnr_key,
    appr_by_cust.sap_bnr_desc,
    appr_by_cust.sap_bnr_frmt_key,
    appr_by_cust.sap_bnr_frmt_desc,
    appr_by_cust.retail_env,
    cust_prod_contr.sg_brand,
    cust_prod_contr.gph_reg_frnchse_grp,
    cust_prod_contr.gph_prod_frnchse,
    cust_prod_contr.gph_prod_brnd,
    (
    (
    appr_by_cust.base_val * CASE
    WHEN (
        appr_by_cust.acct_no IN (
        SELECT DISTINCT
            ITG_SG_CONSTANT_KEY_VALUE.key
        FROM snaposeitg_integration.ITG_SG_CONSTANT_KEY_VALUE AS ITG_SG_CONSTANT_KEY_VALUE
        WHERE
            (
            (
                ITG_SG_CONSTANT_KEY_VALUE.data_category_cd = CAST((
                6
                ) AS DECIMAL)
            )
            AND (
                CAST((
                ITG_SG_CONSTANT_KEY_VALUE.value
                ) AS TEXT) = CAST('Y' AS TEXT)
            )
            )
        )
    )
    THEN cust_prod_contr.jj_cust_prod_sls_contrb_for_merchndsng_per_m
    ELSE cust_prod_contr.jj_cust_prod_sls_contrb_per_m
    END
    ) / CAST((
        1000000
    ) AS DECIMAL)
    ) AS base_val
    FROM appr_by_cust,cust_prod_contr
    WHERE
        (
            CAST((appr_by_cust.jj_mnth_id) AS TEXT) = CAST((cust_prod_contr.mnth_id) AS TEXT)
        AND 
            CAST((appr_by_cust.sg_banner) AS TEXT) = CAST((cust_prod_contr.sg_banner) AS TEXT)
        )
)
,
zuellig_appr as
(
        select * from zsellin_zsellout   
        UNION ALL
        select * from zuellig_appr_temp

),

zapportionment as
(
  SELECT
    CAST((LTRIM(CAST((tpoffinvoice.co_cd) AS TEXT), CAST('0' AS TEXT))) AS VARCHAR) AS co_cd,
    tpoffinvoice.cntry_key,
    CAST((
    tpoffinvoice.jj_yr
    ) AS INT) AS jj_yr,
    tpoffinvoice.jj_mnth_id,
    tpoffinvoice.acct_no,
    tpoffinvoice.gl_description,
    tpoffinvoice.posted_where,
    tpoffinvoice.sg_banner,
    CAST((tpoffinvoice.sap_cust_nm) AS VARCHAR) AS sap_cust_nm,
    tpoffinvoice.sap_prnt_cust_key,
    tpoffinvoice.sap_prnt_cust_desc,
    tpoffinvoice.sap_bnr_key,
    tpoffinvoice.sap_bnr_desc,
    tpoffinvoice.sap_bnr_frmt_key,
    tpoffinvoice.sap_bnr_frmt_desc,
    tpoffinvoice.retail_env,
    tpoffinvoice.sg_brand,
    tpoffinvoice.gph_reg_frnchse_grp,
    tpoffinvoice.gph_prod_frnchse,
    tpoffinvoice.gph_prod_brnd,
    tpoffinvoice.tp_val AS base_val
    FROM tpoffinvoice
    UNION ALL
    SELECT
    zuellig_appr.co_cd,
    zuellig_appr.cntry_key,
    zuellig_appr.jj_yr,
    zuellig_appr.jj_mnth_id,
    zuellig_appr.acct_no,
    zuellig_appr.gl_description,
    zuellig_appr.posted_where,
    zuellig_appr.sg_banner,
    CAST((
    customer.sap_cust_nm
    ) AS VARCHAR) AS sap_cust_nm,
    customer.sap_prnt_cust_key,
    customer.sap_prnt_cust_desc,
    customer.sap_bnr_key,
    customer.sap_bnr_desc,
    customer.sap_bnr_frmt_key,
    customer.sap_bnr_frmt_desc,
    customer.retail_env,
    zuellig_appr.sg_brand,
    zuellig_appr.gph_reg_frnchse_grp,
    zuellig_appr.gph_prod_frnchse,
    zuellig_appr.gph_prod_brnd,
    zuellig_appr.base_val
    FROM
    zuellig_appr
    LEFT JOIN custmstr_groupby AS customer
    ON UPPER(CAST((customer.sap_bnr_frmt_desc) AS TEXT)) = UPPER(CAST((zuellig_appr.sg_banner) AS TEXT))
            
),

sellin_2 as
(
    SELECT
        edw_vw_sg_sellin_sales_fact.co_cd,
        edw_vw_sg_sellin_sales_fact.cntry_nm AS cntry_key,
        edw_vw_sg_sellin_sales_fact.fisc_yr AS jj_yr,
        edw_vw_sg_sellin_sales_fact.jj_mnth_id,
        LTRIM(CAST((edw_vw_sg_sellin_sales_fact.item_cd) AS TEXT), CAST('0' AS TEXT)) AS item_cd,
        LTRIM( CAST((edw_vw_sg_sellin_sales_fact.cust_id) AS TEXT),CAST('0' AS TEXT) ) AS cust_id,
        edw_vw_sg_sellin_sales_fact.acct_no,
        SUM(edw_vw_sg_sellin_sales_fact.base_val) AS base_val,
        SUM(edw_vw_sg_sellin_sales_fact.sls_qty) AS sls_qty,
        SUM(edw_vw_sg_sellin_sales_fact.ret_qty) AS ret_qty
    FROM edw_vw_sg_sellin_sales_fact
    WHERE(
    
            CAST( edw_vw_sg_sellin_sales_fact.cntry_nm AS TEXT) = CAST('SG' AS TEXT)
            AND 
            (
                edw_vw_sg_sellin_sales_fact.jj_mnth_id IN (
                    SELECT DISTINCT itg_sg_zuellig_sellout.period
                    FROM itg_sg_zuellig_sellout
                )
            )
            
            )
        
    GROUP BY
        edw_vw_sg_sellin_sales_fact.co_cd,
        edw_vw_sg_sellin_sales_fact.cntry_nm,
        edw_vw_sg_sellin_sales_fact.fisc_yr,
        edw_vw_sg_sellin_sales_fact.jj_mnth_id,
        LTRIM(CAST((edw_vw_sg_sellin_sales_fact.item_cd) AS TEXT), CAST('0' AS TEXT)),
        LTRIM( CAST((edw_vw_sg_sellin_sales_fact.cust_id) AS TEXT),CAST('0' AS TEXT) ),
        edw_vw_sg_sellin_sales_fact.acct_no
),
a2 as
(
  SELECT
    sellin.co_cd,
    sellin.cntry_key,
    sellin.jj_yr,
    sellin.jj_mnth_id,
    sellin.item_cd,
    sellin.acct_no,
    glmap.gl_description,
    glmap.posted_where,
    prodmstr.gph_prod_brnd AS sg_brand,
    prodmstr.gph_reg_frnchse_grp,
    prodmstr.gph_prod_frnchse,
    prodmstr.gph_prod_brnd,
    prodmstr.gph_prod_sub_brnd,
    prodmstr.sap_matl_num,
    prodmstr.sap_mat_desc,
    custmstr.sap_cust_nm,
    custmstr.sap_prnt_cust_key,
    custmstr.sap_prnt_cust_desc,
    custmstr.sap_bnr_key,
    custmstr.sap_bnr_desc,
    custmstr.sap_bnr_frmt_key,
    custmstr.sap_bnr_frmt_desc,
    custmstr.sap_bnr_frmt_desc AS sg_banner,
    custmstr.retail_env,
    sellin.base_val,
    sellin.sls_qty,
    sellin.ret_qty
  FROM (
    (
      (
        SELLIN_2 as sellin
        LEFT JOIN prodmstr
          ON LTRIM(sellin.item_cd, CAST('0' AS TEXT)) = LTRIM(CAST((prodmstr.sap_matl_num) AS TEXT), CAST('0' AS TEXT))
      )
      LEFT JOIN  custmstr
        ON LTRIM(sellin.cust_id, CAST('0' AS TEXT)) = LTRIM(CAST((custmstr.sap_cust_id) AS TEXT), CAST('0' AS TEXT))   
    )
    LEFT JOIN itg_sg_ciw_mapping AS glmap
      ON LTRIM(sellin.acct_no, CAST('0' AS TEXT)) = LTRIM(CAST((glmap.gl) AS TEXT), CAST('0' AS TEXT))
  )
),
nzsellin as
(
  SELECT
    a.co_cd,
    a.cntry_key,
    a.jj_yr,
    a.jj_mnth_id,
    a.acct_no,
    a.gl_description,
    a.posted_where,
    a.sg_brand,
    a.gph_reg_frnchse_grp,
    a.gph_prod_frnchse,
    a.gph_prod_brnd,
    a.gph_prod_sub_brnd,
    a.sap_matl_num,
    a.sap_mat_desc,
    MIN(CAST((a.sap_cust_nm) AS TEXT)) AS sap_cust_nm,
    a.sap_prnt_cust_key,
    a.sap_prnt_cust_desc,
    a.sap_bnr_key,
    a.sap_bnr_desc,
    a.sap_bnr_frmt_key,
    a.sap_bnr_frmt_desc,
    a.sg_banner,
    a.retail_env,
    SUM(a.base_val) AS base_val,
    SUM(a.sls_qty) AS sls_qty,
    SUM(a.ret_qty) AS ret_qty
  FROM 
    a2  AS a
  GROUP BY
    a.co_cd,
    a.cntry_key,
    a.jj_yr,
    a.jj_mnth_id,
    a.acct_no,
    a.gl_description,
    a.posted_where,
    a.sg_brand,
    a.gph_reg_frnchse_grp,
    a.gph_prod_frnchse,
    a.gph_prod_brnd,
    a.gph_prod_sub_brnd,
    a.sap_matl_num,
    a.sap_mat_desc,
    a.sap_prnt_cust_key,
    a.sap_prnt_cust_desc,
    a.sap_bnr_key,
    a.sap_bnr_desc,
    a.sap_bnr_frmt_key,
    a.sap_bnr_frmt_desc,
    a.sg_banner,
    a.retail_env
),
nzsellin_2 as
(
  SELECT
    a.co_cd,
    a.cntry_key,
    a.jj_yr,
    a.jj_mnth_id,
    a.acct_no,
    a.gl_description,
    a.posted_where,
    a.sg_brand,
    a.gph_reg_frnchse_grp,
    a.gph_prod_frnchse,
    a.gph_prod_brnd,
    a.gph_prod_sub_brnd,
    a.sap_matl_num,
    a.sap_mat_desc,
    MIN(CAST((a.sap_cust_nm) AS TEXT)) AS sap_cust_nm,
    a.sap_prnt_cust_key,
    a.sap_prnt_cust_desc,
    a.sap_bnr_key,
    a.sap_bnr_desc,
    a.sap_bnr_frmt_key,
    a.sap_bnr_frmt_desc,
    a.sg_banner,
    a.retail_env,
    SUM(a.base_val) AS base_val,
    SUM(a.sls_qty) AS sls_qty,
    SUM(a.ret_qty) AS ret_qty
  FROM a2 AS a
  GROUP BY
    a.co_cd,
    a.cntry_key,
    a.jj_yr,
    a.jj_mnth_id,
    a.acct_no,
    a.gl_description,
    a.posted_where,
    a.sg_brand,
    a.gph_reg_frnchse_grp,
    a.gph_prod_frnchse,
    a.gph_prod_brnd,
    a.gph_prod_sub_brnd,
    a.sap_matl_num,
    a.sap_mat_desc,
    a.sap_prnt_cust_key,
    a.sap_prnt_cust_desc,
    a.sap_bnr_key,
    a.sap_bnr_desc,
    a.sap_bnr_frmt_key,
    a.sap_bnr_frmt_desc,
    a.sg_banner,
    a.retail_env
),
zsellout_1 as
(
    SELECT
        'ZSO'::VARCHAR AS data_source_cd,
        zsellout.data_source::VARCHAR AS data_source,
        CAST((COALESCE(zsellout.sap_cmp_id,'4481'::TEXT)) AS VARCHAR) AS sap_cmp_id,
        COALESCE(zsellout.sap_cntry_nm, 'Singapore'::VARCHAR) AS sap_cntry_nm,
        zsellout."year"::INT AS "year",
        zsellout.mnth_id::INT as mnth_id,
        1 AS gl_acct_no,
        CAST('Zuellig Sales Amount' AS VARCHAR) AS gl_description,
        CAST('Sales Amount (NIV)' AS VARCHAR) AS measure_bucket,
        CAST('Zuellig' AS VARCHAR) AS cust_l1,
        zsellout.sg_banner,
        zsellout.sap_bnr_frmt_desc,
        CAST((zsellout.sap_cust_nm) AS VARCHAR) AS sap_cust_nm,
        zsellout.sap_prnt_cust_desc,
        zsellout.retail_env,
        zsellout.sg_brand,
        zsellout.gph_reg_frnchse_grp,
        zsellout.gph_prod_frnchse,
        zsellout.gph_prod_brnd,
        CAST(NULL AS VARCHAR) AS gph_prod_sub_brnd,
        CAST(NULL AS VARCHAR) AS sap_matl_num,
        CAST(NULL AS VARCHAR) AS sap_mat_desc,
        zsellout.sales_value AS base_value
        FROM zsellout
),
zsellout_2 as
(
    SELECT
        CAST('ZSO' AS VARCHAR) AS data_source_cd,
        CAST((zsellout.data_source) AS VARCHAR) AS data_source,
        CAST((COALESCE(zsellout.sap_cmp_id, CAST('4481' AS TEXT))) AS VARCHAR) AS sap_cmp_id,
        COALESCE(zsellout.sap_cntry_nm, CAST('Singapore' AS VARCHAR)) AS sap_cntry_nm,
        CAST((zsellout."year") AS INT) AS "year",
        CAST((zsellout.mnth_id) AS INT) AS mnth_id,
        2 AS gl_acct_no,
        CAST('Zuellig Sales Quantity' AS VARCHAR) AS gl_description,
        CAST('Sales Quantity' AS VARCHAR) AS measure_bucket,
        CAST('Zuellig' AS VARCHAR) AS cust_l1,
        zsellout.sg_banner,
        zsellout.sap_bnr_frmt_desc,
        CAST((zsellout.sap_cust_nm) AS VARCHAR) AS sap_cust_nm,
        zsellout.sap_prnt_cust_desc,
        zsellout.retail_env,
        zsellout.sg_brand,
        zsellout.gph_reg_frnchse_grp,
        zsellout.gph_prod_frnchse,
        zsellout.gph_prod_brnd,
        CAST(NULL AS VARCHAR) AS gph_prod_sub_brnd,
        CAST(NULL AS VARCHAR) AS sap_matl_num,
        CAST(NULL AS VARCHAR) AS sap_mat_desc,
        zsellout.sales_units AS base_value
    FROM  zsellout
),
zsellout_3 as
(
    SELECT
        CAST('ZSO' AS VARCHAR) AS data_source_cd,
        CAST((zsellout.data_source) AS VARCHAR) AS data_source,
        CAST((COALESCE(zsellout.sap_cmp_id, CAST('4481' AS TEXT))) AS VARCHAR) AS sap_cmp_id,
        COALESCE(zsellout.sap_cntry_nm, CAST('Singapore' AS VARCHAR)) AS sap_cntry_nm,
        CAST((zsellout."year" ) AS INT) AS "year",
        CAST(( zsellout.mnth_id) AS INT) AS mnth_id,
        3 AS gl_acct_no,
        CAST('Zuellig Bonus Quantity' AS VARCHAR) AS gl_description,
        CAST('Bonus Quantity' AS VARCHAR) AS measure_bucket,
        CAST('Zuellig' AS VARCHAR) AS cust_l1,
        zsellout.sg_banner,
        zsellout.sap_bnr_frmt_desc,
        CAST((
        zsellout.sap_cust_nm
        ) AS VARCHAR) AS sap_cust_nm,
        zsellout.sap_prnt_cust_desc,
        zsellout.retail_env,
        zsellout.sg_brand,
        zsellout.gph_reg_frnchse_grp,
        zsellout.gph_prod_frnchse,
        zsellout.gph_prod_brnd,
        CAST(NULL AS VARCHAR) AS gph_prod_sub_brnd,
        CAST(NULL AS VARCHAR) AS sap_matl_num,
        CAST(NULL AS VARCHAR) AS sap_mat_desc,
        zsellout.bonus_units AS base_value
    FROM zsellout
),
zsellout_4 as
(
   SELECT
        CAST('ZSO' AS VARCHAR) AS data_source_cd,
        CAST((zsellout.data_source) AS VARCHAR) AS data_source,
        CAST((COALESCE(zsellout.sap_cmp_id, CAST('4481' AS TEXT))) AS VARCHAR) AS sap_cmp_id,
        COALESCE(zsellout.sap_cntry_nm, CAST('Singapore' AS VARCHAR)) AS sap_cntry_nm,
        CAST((zsellout."year") AS INT) AS "year",
        CAST((zsellout.mnth_id) AS INT) AS mnth_id,
        4 AS gl_acct_no,
        CAST('Zuellig Gross Sales Quantity' AS VARCHAR) AS gl_description,
        CAST('GTS Quantity' AS VARCHAR) AS measure_bucket,
        CAST('Zuellig' AS VARCHAR) AS cust_l1,
        zsellout.sg_banner,
        zsellout.sap_bnr_frmt_desc,
        CAST((zsellout.sap_cust_nm) AS VARCHAR) AS sap_cust_nm,
        zsellout.sap_prnt_cust_desc,
        zsellout.retail_env,
        zsellout.sg_brand,
        zsellout.gph_reg_frnchse_grp,
        zsellout.gph_prod_frnchse,
        zsellout.gph_prod_brnd,
        CAST(NULL AS VARCHAR) AS gph_prod_sub_brnd,
        CAST(NULL AS VARCHAR) AS sap_matl_num,
        CAST(NULL AS VARCHAR) AS sap_mat_desc,
        (
            (zsellout.bonus_units + zsellout.sales_units) 
            + zsellout.returns_units
        ) AS base_value
    FROM zsellout 
),
zsellout_5 as
(
    SELECT
        CAST('ZSO' AS VARCHAR) AS data_source_cd,
        CAST((
        zsellout.data_source
        ) AS VARCHAR) AS data_source,
        CAST((
        COALESCE(zsellout.sap_cmp_id, CAST('4481' AS TEXT))
        ) AS VARCHAR) AS sap_cmp_id,
        COALESCE(zsellout.sap_cntry_nm, CAST('Singapore' AS VARCHAR)) AS sap_cntry_nm,
        CAST((
        zsellout."year"
        ) AS INT) AS "year",
        CAST((
        zsellout.mnth_id
        ) AS INT) AS mnth_id,
        5 AS gl_acct_no,
        CAST('Zuellig Returns Quantity' AS VARCHAR) AS gl_description,
        CAST('Returns Quantity' AS VARCHAR) AS measure_bucket,
        CAST('Zuellig' AS VARCHAR) AS cust_l1,
        zsellout.sg_banner,
        zsellout.sap_bnr_frmt_desc,
        CAST((
        zsellout.sap_cust_nm
        ) AS VARCHAR) AS sap_cust_nm,
        zsellout.sap_prnt_cust_desc,
        zsellout.retail_env,
        zsellout.sg_brand,
        zsellout.gph_reg_frnchse_grp,
        zsellout.gph_prod_frnchse,
        zsellout.gph_prod_brnd,
        CAST(NULL AS VARCHAR) AS gph_prod_sub_brnd,
        CAST(NULL AS VARCHAR) AS sap_matl_num,
        CAST(NULL AS VARCHAR) AS sap_mat_desc,
        zsellout.returns_units AS base_value
    FROM  zsellout
),
zsellout_6 as
(
    SELECT
        CAST('ZSO' AS VARCHAR) AS data_source_cd,
        CAST((
            zsellout.data_source
        ) AS VARCHAR) AS data_source,
        CAST((
            COALESCE(zsellout.sap_cmp_id, CAST('4481' AS TEXT))
        ) AS VARCHAR) AS sap_cmp_id,
        COALESCE(zsellout.sap_cntry_nm, CAST('Singapore' AS VARCHAR)) AS sap_cntry_nm,
        CAST((
            zsellout."year"
        ) AS INT) AS "year",
        CAST((
            zsellout.mnth_id
        ) AS INT) AS mnth_id,
        6 AS gl_acct_no,
        CAST('Zuellig Net Sales Quantity' AS VARCHAR) AS gl_description,
        CAST('NTS Quantity' AS VARCHAR) AS measure_bucket,
        CAST('Zuellig' AS VARCHAR) AS cust_l1,
        zsellout.sg_banner,
        zsellout.sap_bnr_frmt_desc,
        CAST((
            zsellout.sap_cust_nm
        ) AS VARCHAR) AS sap_cust_nm,
        zsellout.sap_prnt_cust_desc,
        zsellout.retail_env,
        zsellout.sg_brand,
        zsellout.gph_reg_frnchse_grp,
        zsellout.gph_prod_frnchse,
        zsellout.gph_prod_brnd,
        CAST(NULL AS VARCHAR) AS gph_prod_sub_brnd,
        CAST(NULL AS VARCHAR) AS sap_matl_num,
        CAST(NULL AS VARCHAR) AS sap_mat_desc,
        (
            zsellout.bonus_units + zsellout.sales_units
        ) AS base_value
        FROM zsellout   
),
zsellout_7 as
(
    SELECT
        CAST('ZSO' AS VARCHAR) AS data_source_cd,
        CAST((
        zsellout.data_source
        ) AS VARCHAR) AS data_source,
        CAST((
        COALESCE(zsellout.sap_cmp_id, CAST('4481' AS TEXT))
        ) AS VARCHAR) AS sap_cmp_id,
        COALESCE(zsellout.sap_cntry_nm, CAST('Singapore' AS VARCHAR)) AS sap_cntry_nm,
        CAST((
        zsellout."year"
        ) AS INT) AS "year",
        CAST((
        zsellout.mnth_id
        ) AS INT) AS mnth_id,
        7 AS gl_acct_no,
        CAST('JJ calculated Zuellig Sales Value' AS VARCHAR) AS gl_description,
        CAST('GTS' AS VARCHAR) AS measure_bucket,
        CAST('Zuellig' AS VARCHAR) AS cust_l1,
        zsellout.sg_banner,
        zsellout.sap_bnr_frmt_desc,
        CAST((
        zsellout.sap_cust_nm
        ) AS VARCHAR) AS sap_cust_nm,
        zsellout.sap_prnt_cust_desc,
        zsellout.retail_env,
        zsellout.sg_brand,
        zsellout.gph_reg_frnchse_grp,
        zsellout.gph_prod_frnchse,
        zsellout.gph_prod_brnd,
        CAST(NULL AS VARCHAR) AS gph_prod_sub_brnd,
        CAST(NULL AS VARCHAR) AS sap_matl_num,
        CAST(NULL AS VARCHAR) AS sap_mat_desc,
        zsellout.jj_net_sales_value AS base_value
    FROM zsellout   
),
zsellout_8 as
(
    SELECT
        CAST('JJZSI' AS VARCHAR) AS data_source_cd,
        CAST('J&J Zuellig Sellin' AS VARCHAR) AS data_source,
        COALESCE(zapportionment.co_cd, CAST('4481' AS VARCHAR)) AS sap_cmp_id,
        CAST('Singapore' AS VARCHAR) AS sap_cntry_nm,
        zapportionment.jj_yr AS "year",
        CAST((zapportionment.jj_mnth_id) AS INT) AS mnth_id,
        CAST((
            CASE
            WHEN (CAST((zapportionment.acct_no) AS TEXT) = CAST('' AS TEXT))
            THEN CAST(NULL AS VARCHAR)
            ELSE zapportionment.acct_no
            END
        ) AS INT) AS gl_acct_no,
        zapportionment.gl_description,
        zapportionment.posted_where AS measure_bucket,
        CAST('Zuellig' AS VARCHAR) AS cust_l1,
        zapportionment.sg_banner,
        zapportionment.sap_bnr_frmt_desc,
        zapportionment.sap_cust_nm,
        zapportionment.sap_prnt_cust_desc,
        zapportionment.retail_env,
        zapportionment.sg_brand,
        zapportionment.gph_reg_frnchse_grp,
        zapportionment.gph_prod_frnchse,
        zapportionment.gph_prod_brnd,
        CAST(NULL AS VARCHAR) AS gph_prod_sub_brnd,
        CAST(NULL AS VARCHAR) AS sap_matl_num,
        CAST(NULL AS VARCHAR) AS sap_mat_desc,
        zapportionment.base_val AS base_value
    FROM zapportionment
),
zsellout_9 as(
    SELECT
        CAST('JJSI' AS VARCHAR) AS data_source_cd,
        CAST('J&J Sellin' AS VARCHAR) AS data_source,
        COALESCE(nzsellin.co_cd, CAST('4481' AS VARCHAR)) AS sap_cmp_id,
        CAST('Singapore' AS VARCHAR) AS sap_cntry_nm,
        nzsellin.jj_yr AS "year",
        CAST((
        nzsellin.jj_mnth_id
        ) AS INT) AS mnth_id,
        CAST((
        nzsellin.acct_no
        ) AS INT) AS gl_acct_no,
        nzsellin.gl_description,
        nzsellin.posted_where AS measure_bucket,
        CAST('J&J' AS VARCHAR) AS cust_l1,
        nzsellin.sap_bnr_frmt_desc AS sg_banner,
        nzsellin.sap_bnr_frmt_desc,
        CAST((
        nzsellin.sap_cust_nm
        ) AS VARCHAR) AS sap_cust_nm,
        nzsellin.sap_prnt_cust_desc,
        nzsellin.retail_env,
        nzsellin.sg_brand,
        nzsellin.gph_reg_frnchse_grp,
        nzsellin.gph_prod_frnchse,
        nzsellin.gph_prod_brnd,
        nzsellin.gph_prod_sub_brnd,
        nzsellin.sap_matl_num,
        nzsellin.sap_mat_desc,
        nzsellin.base_val AS base_value
    FROM nzsellin
),
zsellout_10 as
(
    SELECT
        CAST('JJSI' AS VARCHAR) AS data_source_cd,
        CAST('J&J Sellin' AS VARCHAR) AS data_source,
        COALESCE(nzsellin.co_cd, CAST('4481' AS VARCHAR)) AS sap_cmp_id,
        CAST('Singapore' AS VARCHAR) AS sap_cntry_nm,
        nzsellin.jj_yr AS "year",
        CAST((
        nzsellin.jj_mnth_id
        ) AS INT) AS mnth_id,
        CAST((
        nzsellin.acct_no
        ) AS INT) AS gl_acct_no,
        nzsellin.gl_description,
        CAST('GTS Quantity' AS VARCHAR) AS measure_bucket,
        CAST('J&J' AS VARCHAR) AS cust_l1,
        nzsellin.sap_bnr_frmt_desc AS sg_banner,
        nzsellin.sap_bnr_frmt_desc,
        CAST((
        nzsellin.sap_cust_nm
        ) AS VARCHAR) AS sap_cust_nm,
        nzsellin.sap_prnt_cust_desc,
        nzsellin.retail_env,
        nzsellin.sg_brand,
        nzsellin.gph_reg_frnchse_grp,
        nzsellin.gph_prod_frnchse,
        nzsellin.gph_prod_brnd,
        nzsellin.gph_prod_sub_brnd,
        nzsellin.sap_matl_num,
        nzsellin.sap_mat_desc,
        nzsellin.sls_qty AS base_value
    FROM nzsellin_2  AS nzsellin
    WHERE
        (
        CAST((
            nzsellin.posted_where
        ) AS TEXT) = CAST('GTS' AS TEXT)
        )
),
zsellout_11 as
(
    SELECT
        CAST('JJSI' AS VARCHAR) AS data_source_cd,
        CAST('J&J Sellin' AS VARCHAR) AS data_source,
        COALESCE(nzsellin.co_cd, CAST('4481' AS VARCHAR)) AS sap_cmp_id,
        CAST('Singapore' AS VARCHAR) AS sap_cntry_nm,
        nzsellin.jj_yr AS "year",
        CAST((
        nzsellin.jj_mnth_id
        ) AS INT) AS mnth_id,
        CAST((
        nzsellin.acct_no
        ) AS INT) AS gl_acct_no,
        nzsellin.gl_description,
        CAST('GTS Quantity' AS VARCHAR) AS measure_bucket,
        CAST('J&J' AS VARCHAR) AS cust_l1,
        nzsellin.sap_bnr_frmt_desc AS sg_banner,
        nzsellin.sap_bnr_frmt_desc,
        CAST((
        nzsellin.sap_cust_nm
        ) AS VARCHAR) AS sap_cust_nm,
        nzsellin.sap_prnt_cust_desc,
        nzsellin.retail_env,
        nzsellin.sg_brand,
        nzsellin.gph_reg_frnchse_grp,
        nzsellin.gph_prod_frnchse,
        nzsellin.gph_prod_brnd,
        nzsellin.gph_prod_sub_brnd,
        nzsellin.sap_matl_num,
        nzsellin.sap_mat_desc,
        nzsellin.ret_qty AS base_value
    FROM nzsellin_2 AS nzsellin
    WHERE
        (
        CAST((
            nzsellin.posted_where
        ) AS TEXT) = CAST('RETURN' AS TEXT)
        )
),
zsellout_12 as
(
    SELECT
        CAST('JJSI' AS VARCHAR) AS data_source_cd,
        CAST('J&J Sellin' AS VARCHAR) AS data_source,
        COALESCE(nzsellin.co_cd, CAST('4481' AS VARCHAR)) AS sap_cmp_id,
        CAST('Singapore' AS VARCHAR) AS sap_cntry_nm,
        nzsellin.jj_yr AS "year",
        CAST((
        nzsellin.jj_mnth_id
        ) AS INT) AS mnth_id,
        CAST((
        nzsellin.acct_no
        ) AS INT) AS gl_acct_no,
        nzsellin.gl_description,
        CAST('NTS Quantity' AS VARCHAR) AS measure_bucket,
        CAST('J&J' AS VARCHAR) AS cust_l1,
        nzsellin.sap_bnr_frmt_desc AS sg_banner,
        nzsellin.sap_bnr_frmt_desc,
        CAST((
        nzsellin.sap_cust_nm
        ) AS VARCHAR) AS sap_cust_nm,
        nzsellin.sap_prnt_cust_desc,
        nzsellin.retail_env,
        nzsellin.sg_brand,
        nzsellin.gph_reg_frnchse_grp,
        nzsellin.gph_prod_frnchse,
        nzsellin.gph_prod_brnd,
        nzsellin.gph_prod_sub_brnd,
        nzsellin.sap_matl_num,
        nzsellin.sap_mat_desc,
        (
        nzsellin.sls_qty - nzsellin.ret_qty
        ) AS base_value
  FROM nzsellin_2 AS nzsellin
  WHERE
    (
      (
        CAST((
          nzsellin.posted_where
        ) AS TEXT) = CAST('GTS' AS TEXT)
      )
      OR (
        CAST((
          nzsellin.posted_where
        ) AS TEXT) = CAST('RETURN' AS TEXT)
      )
    )
),
t1 as (
    select * from zsellout_1                                                      
    UNION ALL
    select * from zsellout_2
    
    UNION ALL
    select * from zsellout_3
    
    UNION ALL
    select * from zsellout_4
    
    UNION ALL
    select * from zsellout_5
    
    UNION ALL
    select * from zsellout_6
    
    UNION ALL
    select * from zsellout_7
    
    UNION ALL
    select * from zsellout_8
    
    UNION ALL
    select * from zsellout_9
    
    UNION ALL
    select * from zsellout_10
    
    UNION ALL
    select * from zsellout_11
    
    UNION ALL
    select * from zsellout_12
),
t2 as (
    
  SELECT DISTINCT
    EDW_VW_SG_CURR_DIM.to_ccy,
    EDW_VW_SG_CURR_DIM.exch_rate
  FROM EDW_VW_SG_CURR_DIM
  WHERE
    (
      (
        (
          CAST((
            EDW_VW_SG_CURR_DIM.cntry_key
          ) AS TEXT) = CAST('SG' AS TEXT)
        )
        AND (
          CAST((
            EDW_VW_SG_CURR_DIM.from_ccy
          ) AS TEXT) = CAST('SGD' AS TEXT)
        )
      )
      AND (
        EDW_VW_SG_CURR_DIM.jj_year = (
          SELECT
            edw_vw_os_time_dim."year"
          FROM edw_vw_os_time_dim
          WHERE edw_vw_os_time_dim.cal_date = current_timestamp()::date
        )
      )
    ) 
),
t3 as
(
  SELECT DISTINCT
    edw_vw_os_time_dim.qrtr,
    edw_vw_os_time_dim.mnth_id,
    edw_vw_os_time_dim.mnth_no
  FROM edw_vw_os_time_dim 
),
final as(
SELECT
    t1.data_source_cd,
    t1.data_source,
    t1.sap_cmp_id,
    t1.sap_cntry_nm,
    t1."year",
    SUBSTRING(t3.qrtr, 6, 2) AS qrtr,
    CAST((t1.mnth_id) AS VARCHAR) AS mnth_id,
    t3.mnth_no,
    t1.gl_acct_no,
    t1.gl_description,
    t1.measure_bucket,
    t1.cust_l1,
    t1.sg_banner,
    t1.sap_bnr_frmt_desc,
    t1.sap_cust_nm,
    t1.sap_prnt_cust_desc,
    t1.retail_env,
    t1.sg_brand,
    t1.gph_reg_frnchse_grp,
    t1.gph_prod_frnchse,
    t1.gph_prod_brnd,
    t1.gph_prod_sub_brnd,
    t1.sap_matl_num,
    t1.sap_mat_desc,
    t2.to_ccy AS currency,
    CASE
    WHEN t1.measure_bucket:: TEXT LIKE ('%Quantity%'::TEXT)
    THEN t1.base_value
    ELSE (t1.base_value * t2.exch_rate::DOUBLE)
    END AS base_value
FROM t1,t2, t3
WHERE
  (
    CAST((
      t1.mnth_id
    ) AS TEXT) = t3.mnth_id
  )
)
select * from final
