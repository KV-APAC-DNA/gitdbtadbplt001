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
    
        select 
            ltrim((cbd.cust_num)::text, (0)::text) as sap_cust_id,
            cbd.cust_nm as sap_cust_nm,
            csd.sls_org as sap_sls_org,
            ltrim((cd.company)::text, (0)::text) as sap_cmp_id,
            cd.ctry_key as sap_cntry_cd,
            cd.ctry_nm as sap_cntry_nm,
            cbd.addr as sap_addr,
            cbd.rgn as sap_region,
            cbd.dstrc as sap_state_cd,
            cbd.city as sap_city,
            cbd.pstl_cd as sap_post_cd,
            csd.dstr_chnl as sap_chnl_cd,
            dc.txtsh as sap_chnl_desc,
            csd.sls_ofc as sap_sls_office_cd,
            csd.sls_ofc_desc as sap_sls_office_desc,
            csd.sls_grp as sap_sls_grp_cd,
            csd.sls_grp_desc as sap_sls_grp_desc,
            csd.crncy_key as sap_curr_cd,
            csd.prnt_cust_key as sap_prnt_cust_key,
            cddes_pck.code_desc as sap_prnt_cust_desc,
            csd.chnl_key as sap_cust_chnl_key,
            cddes_chnl.code_desc as sap_cust_chnl_desc,
            csd.sub_chnl_key as sap_cust_sub_chnl_key,
            cddes_subchnl.code_desc as sap_sub_chnl_desc,
            csd.go_to_mdl_key as sap_go_to_mdl_key,
            cddes_gtm.code_desc as sap_go_to_mdl_desc,
            csd.bnr_key as sap_bnr_key,
            cddes_bnrkey.code_desc as sap_bnr_desc,
            csd.bnr_frmt_key as sap_bnr_frmt_key,
            cddes_bnrfmt.code_desc as sap_bnr_frmt_desc,
            subchnl_retail_env.retail_env,
            gch.gcgh_region as gch_region,
            gch.gcgh_cluster as gch_cluster,
            gch.gcgh_subcluster as gch_subcluster,
            gch.gcgh_market as gch_market,
            gch.gcch_retail_banner as gch_retail_banner,
            row_number() over(
                partition by ltrim((csd.cust_num)::text, (0)::text)
                order by case
                        when (
                            ((csd.cust_del_flag)::text = null::text)
                            or (
                                (csd.cust_del_flag is null)
                                and (null  is null)
                            )
                        ) then 'O'::character varying
                        when (
                            ((csd.cust_del_flag)::text = ''::text)
                            or (
                                (csd.cust_del_flag is null)
                                and ('' is null)
                            )
                        ) then 'O'::character varying
                        else csd.cust_del_flag
                    end,
                    csd.sls_org,
                    csd.dstr_chnl
            ) as rnk
        from (
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
                                            join  edw_customer_base_dim cbd on (
                                                (
                                                    ltrim((csd.cust_num)::text, (0)::text) = ltrim((cbd.cust_num)::text, (0)::text)
                                                )
                                            )
                                        )
                                        left join  edw_gch_customerhierarchy gch on (
                                            (
                                                ltrim((gch.customer)::text, (0)::text) = ltrim((cbd.cust_num)::text, (0)::text)
                                            )
                                        )
                                    )
                                    join  edw_dstrbtn_chnl dc on (
                                        (
                                            ltrim((csd.dstr_chnl)::text, (0)::text) = ltrim((dc.distr_chan)::text, (0)::text)
                                        )
                                    )
                                )
                                join  edw_sales_org_dim sod on (
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
                            join  edw_company_dim cd on (((sod.sls_org_co_cd)::text = (cd.co_cd)::text))
                        )
                        left join  edw_code_descriptions cddes_pck on (
                            (
                                (
                                    (cddes_pck.code_type)::text = 'Parent Customer Key'::text
                                )
                                and (
                                    (cddes_pck.code)::text = (csd.prnt_cust_key)::text
                                )
                            )
                        )
                    )
                    left join  edw_code_descriptions cddes_bnrkey on (
                        (
                            (
                                (cddes_bnrkey.code_type)::text = 'Banner Key'::text
                            )
                            and ((cddes_bnrkey.code)::text = (csd.bnr_key)::text)
                        )
                    )
                )
                left join  edw_code_descriptions cddes_bnrfmt on (
                    (
                        (
                            (cddes_bnrfmt.code_type)::text = 'Banner Format Key'::text
                        )
                        and (
                            (cddes_bnrfmt.code)::text = (csd.bnr_frmt_key)::text
                        )
                    )
                )
            )
            left join  edw_code_descriptions cddes_chnl on (
                (
                    (
                        (cddes_chnl.code_type)::text = 'Channel Key'::text
                    )
                    and ((cddes_chnl.code)::text = (csd.chnl_key)::text)
                )
            )
        )
        left join  edw_code_descriptions cddes_gtm on (
            (
                (
                    (cddes_gtm.code_type)::text = 'Go To Model Key'::text
                )
                and (
                    (cddes_gtm.code)::text = (csd.go_to_mdl_key)::text
                )
            )
        )
    )
    left join  edw_code_descriptions cddes_subchnl on (
        (
            (
                (cddes_subchnl.code_type)::text = 'Sub Channel Key'::text
            )
            and (
                (cddes_subchnl.code)::text = (csd.sub_chnl_key)::text
            )
        )
    )
)
left join  edw_subchnl_retail_env_mapping subchnl_retail_env on (
    (
        upper((subchnl_retail_env.sub_channel)::text) = upper((cddes_subchnl.code_desc)::text)
    )
)
)
        where (
                ((csd.sls_org)::text = '2400'::text)
                OR ((csd.sls_org)::text = '2500'::text)
            )
    
),
trans as 
(
    select 
        (t.sap_cust_id)::character varying as sap_cust_id,
        t.sap_cust_nm,
        t.sap_sls_org,
        (t.sap_cmp_id)::character varying as sap_cmp_id,
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
    from  t
    where (t.rnk = 1)
)

select * from trans