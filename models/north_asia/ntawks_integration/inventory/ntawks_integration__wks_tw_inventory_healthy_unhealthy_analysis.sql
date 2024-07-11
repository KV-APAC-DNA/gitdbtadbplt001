with edw_gch_customerhierarchy as(
    select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),
edw_customer_sales_dim as(
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
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
edw_code_descriptions as(
    select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
edw_subchnl_retail_env_mapping as(
    select * from {{ source('aspedw_integration', 'edw_subchnl_retail_env_mapping') }}
),
edw_customer_dim as(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
wks_taiwan_siso_propagate_final as(
    select * from {{ ref('ntawks_integration__wks_taiwan_siso_propagate_final') }}
),
edw_vw_greenlight_skus as(
    select * from {{ ref('aspedw_integration__edw_vw_greenlight_skus') }}
),
edw_product_attr_dim as(
    select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
),
edw_material_dim as 
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
customer as 
(
    Select 
        distinct SAP_PRNT_CUST_KEY,
        SAP_PRNT_CUST_DESC,
        SAP_CUST_CHNL_KEY,
        SAP_CUST_CHNL_DESC,
        SAP_CUST_SUB_CHNL_KEY,
        SAP_SUB_CHNL_DESC,
        SAP_GO_TO_MDL_KEY,
        SAP_GO_TO_MDL_DESC,
        SAP_BNR_KEY,
        SAP_BNR_DESC,
        SAP_BNR_FRMT_KEY,
        SAP_BNR_FRMT_DESC,
        RETAIL_ENV,
        ROW_NUMBER () OVER (
            Partition by sap_bnr_key,
            sap_bnr_desc,
            sap_go_to_mdl_key
            order by sap_prnt_cust_key,
                sap_prnt_cust_desc,
                sap_go_to_mdl_key
        ) as rnk
    from 
        (
            select 
                distinct ecbd.cust_num as sap_cust_id,
                ecbd.cust_nm as sap_cust_nm,
                ecsd.sls_org as sap_sls_org,
                ecd.company as sap_cmp_id,
                ecd.ctry_key as sap_cntry_cd,
                ecd.ctry_nm as sap_cntry_nm,
                ecsd.prnt_cust_key as sap_prnt_cust_key,
                cddes_pck.code_desc as sap_prnt_cust_desc,
                ecsd.chnl_key as sap_cust_chnl_key,
                cddes_chnl.code_desc as sap_cust_chnl_desc,
                ecsd.sub_chnl_key as sap_cust_sub_chnl_key,
                cddes_subchnl.code_desc as sap_sub_chnl_desc,
                ecsd.go_to_mdl_key as sap_go_to_mdl_key,
                cddes_gtm.code_desc as sap_go_to_mdl_desc,
                ecsd.bnr_key as sap_bnr_key,
                cddes_bnrkey.code_desc as sap_bnr_desc,
                ecsd.bnr_frmt_key as sap_bnr_frmt_key,
                cddes_bnrfmt.code_desc as sap_bnr_frmt_desc,
                subchnl_retail_env.retail_env,
                regzone.region_name as region,
                regzone.zone_name as zone_or_area,
                egch.gcgh_region as gch_region,
                egch.gcgh_cluster as gch_cluster,
                egch.gcgh_subcluster as gch_subcluster,
                egch.gcgh_market as gch_market,
                egch.gcch_retail_banner as gch_retail_banner,
                row_number() over (
                    partition by sap_cust_id
                    order by sap_prnt_cust_key desc
                ) as rank
            from 
                edw_gch_customerhierarchy egch,
                edw_customer_sales_dim ecsd,
                edw_customer_base_dim ecbd,
                edw_company_dim ecd,
                edw_dstrbtn_chnl edc,
                edw_sales_org_dim esod,
                edw_code_descriptions cddes_pck,
                edw_code_descriptions cddes_bnrkey,
                edw_code_descriptions cddes_bnrfmt,
                edw_code_descriptions cddes_chnl,
                edw_code_descriptions cddes_gtm,
                edw_code_descriptions cddes_subchnl,
                edw_subchnl_retail_env_mapping subchnl_retail_env,
                (
                    SELECT 
                        cust_num,
                        min(
                            decode(cust_del_flag, null, 'O', '', 'O', cust_del_flag)
                        ) as cust_del_flag
                    from edw_customer_sales_dim
                    where sls_org in ('1200')
                    group by cust_num
                ) A,
                (
                    select distinct customer_code,
                        region_name,
                        zone_name
                    from edw_customer_dim
                ) regzone
            where rtrim(egch.customer(+)) = rtrim(ecbd.cust_num)
                and rtrim(ecsd.cust_num) = rtrim(ecbd.cust_num)
                and decode(
                    ecsd.cust_del_flag,
                    null,
                    'O',
                    '',
                    'O',
                    ecsd.cust_del_flag
                ) = a.cust_del_flag
                and rtrim(a.cust_num) = rtrim(ecsd.cust_num)
                and rtrim(ecsd.dstr_chnl) = rtrim(edc.distr_chan)
                and rtrim(ecsd.sls_org) = rtrim(esod.sls_org)
                and rtrim(esod.sls_org_co_cd) = rtrim(ecd.co_cd)
                and ecsd.sls_org in ('1200')
                and trim(upper(cddes_pck.code_type(+))) = 'PARENT CUSTOMER KEY'
                and rtrim(cddes_pck.code(+)) = rtrim(ecsd.prnt_cust_key)
                and trim(upper(cddes_bnrkey.code_type(+))) = 'BANNER KEY'
                and rtrim(cddes_bnrkey.code(+)) = rtrim(ecsd.bnr_key)
                and trim(upper(cddes_bnrfmt.code_type(+))) = 'BANNER FORMAT KEY'
                and rtrim(cddes_bnrfmt.code(+)) = rtrim(ecsd.bnr_frmt_key)
                and trim(upper(cddes_chnl.code_type(+))) = 'CHANNEL KEY'
                and rtrim(cddes_chnl.code(+)) = rtrim(ecsd.chnl_key)
                and trim(upper(cddes_gtm.code_type(+))) = 'GO TO MODEL KEY'
                and rtrim(cddes_gtm.code(+)) = rtrim(ecsd.go_to_mdl_key)
                and trim(upper(cddes_subchnl.code_type(+))) = 'SUB CHANNEL KEY'
                and rtrim(cddes_subchnl.code(+)) = rtrim(ecsd.sub_chnl_key)
                and upper(subchnl_retail_env.sub_channel(+)) = upper(cddes_subchnl.code_desc)
                and ltrim(ecsd.cust_num, '0') = regzone.customer_code(+)
        )
    where sap_prnt_cust_key <> ''
),
final as
(   
    SELECT 
        month,
        TRIM(NVL(NULLIF(T5.prod_hier_l4, ''), 'NA')) AS GLOBAL_PROD_BRAND,
        TRIM(NVL(NULLIF(T5.prod_hier_l7, ''), 'NA')) AS GLOBAL_PROD_VARIANT,
        TRIM(NVL(NULLIF(T5.prod_hier_l5, ''), 'NA')) AS GLOBAL_PROD_SEGMENT,
        TRIM(NVL(NULLIF(T5.prod_hier_l6, ''), 'NA')) AS GLOBAL_PROD_CATEGORY,
        TRIM(NVL(NULLIF(t5.pka_product_key, ''), 'NA')) AS pka_product_key,
        TRIM(NVL(NULLIF(t5.pka_size_desc, ''), 'NA')) AS pka_size_desc,
        TRIM(NVL(NULLIF(T4.SAP_PRNT_CUST_KEY, ''),'Not Assingned')) AS SAP_PRNT_CUST_KEY,
        sum(last_3months_so_value) as last_3months_so_val,
        sum(last_6months_so_value) as last_6months_so_val,
        sum(last_12months_so_value) as last_12months_so_val,
        sum(last_36months_so_value) as last_36months_so_val,
        CASE
            WHEN COALESCE(last_36months_so_val, 0) > 0
            and COALESCE(last_12months_so_val, 0) <= 0 THEN 'N'
            ELSE 'Y'
        END AS healthy_inventory
    FROM 
        (
            Select * from wks_taiwan_siso_propagate_final
        ) SISO,
        (
            Select *
            from (
                    Select *,
                        row_number() over( partition by matl_num order by matl_num ) rnk
                    from 
                        (
                            select m.matl_num,
                                m.matl_desc,
                                --m.greenlight_sku_flag ,
                                m.pka_product_key,
                                m.pka_size_desc,
                                m.pka_product_key_description,
                                m.pka_product_key as product_key,
                                m.pka_product_key_description as product_key_description,
                                pa.* -- from  (Select * from edw_vw_greenlight_skus WHERE sls_org in ( '1200') ) m 
                            from (
                                    Select *
                                    from edw_material_dim
                                ) m,
                                edw_product_attr_dim pa
                            where rtrim(pa.cntry) = 'TW'
                                and ltrim(pa.sap_matl_num, '0') = ltrim(m.matl_num, '0')
                        )
                )
            where rnk = 1
        ) T5,
        (
            SELECT * FROM CUSTOMER where rnk = 1
        ) T4
    WHERE rtrim(SISO.ean_num) = rtrim(T5.ean(+))
        and rtrim(T4.sap_bnr_key(+)) = rtrim(SISO.bnr_key)
    GROUP BY 
        month,
        TRIM(NVL(NULLIF(T5.prod_hier_l4, ''), 'NA')),
        TRIM(NVL(NULLIF(T5.prod_hier_l7, ''), 'NA')),
        TRIM(NVL(NULLIF(T5.prod_hier_l5, ''), 'NA')),
        TRIM(NVL(NULLIF(T5.prod_hier_l6, ''), 'NA')),
        TRIM(NVL(NULLIF(t5.pka_product_key, ''), 'NA')),
        TRIM(NVL(NULLIF(t5.pka_size_desc, ''), 'NA')),
        TRIM(NVL(NULLIF(T4.SAP_PRNT_CUST_KEY, ''),'Not Assingned'))
)
select * from final