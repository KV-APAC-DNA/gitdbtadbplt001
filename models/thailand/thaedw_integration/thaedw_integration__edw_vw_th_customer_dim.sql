with edw_gch_customerhierarchy as(
    select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),
edw_customer_base_dim as(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_company_dim as(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_dstrbtn_chnl as(
    select * from {{ ref('aspedw_integration__edw_dstrbtn_chnl') }}
),
edw_sales_org_dim as(
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
edw_customer_sales_dim as(
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_code_descriptions as(
    select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
edw_subchnl_retail_env_mapping as(
    select * from {{ source('aspedw_integration', 'edw_subchnl_retail_env_mapping') }}
),
t as (
    (
                    SELECT 
                        ltrim((cbd.cust_num)::text, (0)::text) AS sap_cust_id,
                        cbd.cust_nm AS sap_cust_nm,
                        csd.sls_org AS sap_sls_org,
                        ltrim((cd.company)::text, (0)::text) AS sap_cmp_id,
                        cd.ctry_key AS sap_cntry_cd,
                        cd.ctry_nm AS sap_cntry_nm,
                        cbd.addr AS sap_addr,
                        cbd.rgn AS sap_region,
                        cbd.dstrc AS sap_state_cd,
                        cbd.city AS sap_city,
                        cbd.pstl_cd AS sap_post_cd,
                        csd.dstr_chnl AS sap_chnl_cd,
                        dc.txtsh AS sap_chnl_desc,
                        csd.sls_ofc AS sap_sls_office_cd,
                        csd.sls_ofc_desc AS sap_sls_office_desc,
                        csd.sls_grp AS sap_sls_grp_cd,
                        csd.sls_grp_desc AS sap_sls_grp_desc,
                        csd.crncy_key AS sap_curr_cd,
                        csd.prnt_cust_key AS sap_prnt_cust_key,
                        cddes_pck.code_desc AS sap_prnt_cust_desc,
                        csd.chnl_key AS sap_cust_chnl_key,
                        cddes_chnl.code_desc AS sap_cust_chnl_desc,
                        csd.sub_chnl_key AS sap_cust_sub_chnl_key,
                        cddes_subchnl.code_desc AS sap_sub_chnl_desc,
                        csd.go_to_mdl_key AS sap_go_to_mdl_key,
                        cddes_gtm.code_desc AS sap_go_to_mdl_desc,
                        csd.bnr_key AS sap_bnr_key,
                        cddes_bnrkey.code_desc AS sap_bnr_desc,
                        csd.bnr_frmt_key AS sap_bnr_frmt_key,
                        cddes_bnrfmt.code_desc AS sap_bnr_frmt_desc,
                        subchnl_retail_env.retail_env,
                        gch.gcgh_region AS gch_region,
                        gch.gcgh_cluster AS gch_cluster,
                        gch.gcgh_subcluster AS gch_subcluster,
                        gch.gcgh_market AS gch_market,
                        gch.gcch_retail_banner AS gch_retail_banner,
                        row_number() OVER(
                            PARTITION BY ltrim((csd.cust_num)::text, (0)::text)
                            ORDER BY CASE
                                    WHEN (
                                        ((csd.cust_del_flag)::text = NULL::text)
                                        OR (
                                            (csd.cust_del_flag IS NULL)
                                            AND (NULL  IS NULL)
                                        )
                                    ) THEN 'O'::character varying
                                    WHEN (
                                        ((csd.cust_del_flag)::text = ''::text)
                                        OR (
                                            (csd.cust_del_flag IS NULL)
                                            AND ('' IS NULL)
                                        )
                                    ) THEN 'O'::character varying
                                    ELSE csd.cust_del_flag
                                END,
                                csd.sls_org,
                                csd.dstr_chnl
                        ) AS rnk
                    FROM (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (
                                                                         edw_customer_sales_dim csd
                                                                        JOIN  edw_customer_base_dim cbd ON (
                                                                            (
                                                                                ltrim((csd.cust_num)::text, (0)::text) = ltrim((cbd.cust_num)::text, (0)::text)
                                                                            )
                                                                        )
                                                                    )
                                                                    LEFT JOIN  edw_gch_customerhierarchy gch ON (
                                                                        (
                                                                            ltrim((gch.customer)::text, (0)::text) = ltrim((cbd.cust_num)::text, (0)::text)
                                                                        )
                                                                    )
                                                                )
                                                                JOIN  edw_dstrbtn_chnl dc ON (
                                                                    (
                                                                        ltrim((csd.dstr_chnl)::text, (0)::text) = ltrim((dc.distr_chan)::text, (0)::text)
                                                                    )
                                                                )
                                                            )
                                                            JOIN  edw_sales_org_dim sod ON (
                                                                (
                                                                    (
                                                                        (
                                                                             trim((csd.sls_org)::text) =  trim((sod.sls_org)::text)
                                                                        )
                                                                        AND ((sod.ctry_key)::text = 'TH'::text)
                                                                    )
                                                                    AND (
                                                                        ((sod.sls_org)::text = '2400'::text)
                                                                        OR ((sod.sls_org)::text = '2500'::text)
                                                                    )
                                                                )
                                                            )
                                                        )
                                                        JOIN  edw_company_dim cd ON (((sod.sls_org_co_cd)::text = (cd.co_cd)::text))
                                                    )
                                                    LEFT JOIN  edw_code_descriptions cddes_pck ON (
                                                        (
                                                            (
                                                                (cddes_pck.code_type)::text = 'Parent Customer Key'::text
                                                            )
                                                            AND (
                                                                (cddes_pck.code)::text = (csd.prnt_cust_key)::text
                                                            )
                                                        )
                                                    )
                                                )
                                                LEFT JOIN  edw_code_descriptions cddes_bnrkey ON (
                                                    (
                                                        (
                                                            (cddes_bnrkey.code_type)::text = 'Banner Key'::text
                                                        )
                                                        AND ((cddes_bnrkey.code)::text = (csd.bnr_key)::text)
                                                    )
                                                )
                                            )
                                            LEFT JOIN  edw_code_descriptions cddes_bnrfmt ON (
                                                (
                                                    (
                                                        (cddes_bnrfmt.code_type)::text = 'Banner Format Key'::text
                                                    )
                                                    AND (
                                                        (cddes_bnrfmt.code)::text = (csd.bnr_frmt_key)::text
                                                    )
                                                )
                                            )
                                        )
                                        LEFT JOIN  edw_code_descriptions cddes_chnl ON (
                                            (
                                                (
                                                    (cddes_chnl.code_type)::text = 'Channel Key'::text
                                                )
                                                AND ((cddes_chnl.code)::text = (csd.chnl_key)::text)
                                            )
                                        )
                                    )
                                    LEFT JOIN  edw_code_descriptions cddes_gtm ON (
                                        (
                                            (
                                                (cddes_gtm.code_type)::text = 'Go To Model Key'::text
                                            )
                                            AND (
                                                (cddes_gtm.code)::text = (csd.go_to_mdl_key)::text
                                            )
                                        )
                                    )
                                )
                                LEFT JOIN  edw_code_descriptions cddes_subchnl ON (
                                    (
                                        (
                                            (cddes_subchnl.code_type)::text = 'Sub Channel Key'::text
                                        )
                                        AND (
                                            (cddes_subchnl.code)::text = (csd.sub_chnl_key)::text
                                        )
                                    )
                                )
                            )
                            LEFT JOIN  edw_subchnl_retail_env_mapping subchnl_retail_env ON (
                                (
                                    upper((subchnl_retail_env.sub_channel)::text) = upper((cddes_subchnl.code_desc)::text)
                                )
                            )
                        )
                    WHERE (
                            ((csd.sls_org)::text = '2400'::text)
                            OR ((csd.sls_org)::text = '2500'::text)
                        )
                )
),
trans as (
SELECT (t.sap_cust_id)::character varying AS sap_cust_id,
                t.sap_cust_nm,
                t.sap_sls_org,
                (t.sap_cmp_id)::character varying AS sap_cmp_id,
                t.sap_cntry_cd,
                t.sap_cntry_nm,
                t.sap_addr,
                t.sap_region,
                t.sap_state_cd,
                t.sap_city,
                t.sap_post_cd,
                t.sap_chnl_cd,
                t.sap_chnl_desc,
                t.sap_sls_office_cd,
                t.sap_sls_office_desc,
                t.sap_sls_grp_cd,
                t.sap_sls_grp_desc,
                t.sap_curr_cd,
                t.sap_prnt_cust_key,
                t.sap_prnt_cust_desc,
                t.sap_cust_chnl_key,
                t.sap_cust_chnl_desc,
                t.sap_cust_sub_chnl_key,
                t.sap_sub_chnl_desc,
                t.sap_go_to_mdl_key,
                t.sap_go_to_mdl_desc,
                t.sap_bnr_key,
                t.sap_bnr_desc,
                t.sap_bnr_frmt_key,
                t.sap_bnr_frmt_desc,
                t.retail_env,
                t.gch_region,
                t.gch_cluster,
                t.gch_subcluster,
                t.gch_market,
                t.gch_retail_banner
            FROM  t
            WHERE (t.rnk = 1)
)

select * from trans