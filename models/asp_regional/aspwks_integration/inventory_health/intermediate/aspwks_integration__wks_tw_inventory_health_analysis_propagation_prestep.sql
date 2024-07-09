with edw_gch_customerhierarchy as
(
    select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),
edw_customer_sales_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_customer_base_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_company_dim as
(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_dstrbtn_chnl as
(
    select * from {{ ref('aspedw_integration__edw_dstrbtn_chnl') }}
),
edw_sales_org_dim as
(
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
edw_code_descriptions as
(
    select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
edw_subchnl_retail_env_mapping as
(
    select * from {{ source('aspedw_integration','edw_subchnl_retail_env_mapping')}}
),
edw_customer_dim as
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
wks_taiwan_siso_propagate_final as
(
    select * from {{ ref('ntawks_integration__wks_taiwan_siso_propagate_final') }}
),
edw_vw_greenlight_skus as
(
    select * from {{ ref('aspedw_integration__edw_vw_greenlight_skus') }}
),
vw_edw_reg_exch_rate as
(
    select * from {{ ref('aspedw_integration__vw_edw_reg_exch_rate') }}
),
edw_copa_trans_fact as
(
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_billing_fact as
(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_product_attr_dim as
(
    select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
cal as 
(
    select distinct cal_year,
        cal_qrtr_no,
        cal_mnth_id,
        cal_mnth_no
    from edw_vw_os_time_dim
),
currency AS 
(
    Select * from vw_edw_reg_exch_rate where cntry_key = 'TW'
),
customer AS 
(
    select 
        distinct sap_prnt_cust_key,
        sap_prnt_cust_desc,
        sap_cust_chnl_key,
        sap_cust_chnl_desc,
        sap_cust_sub_chnl_key,
        sap_sub_chnl_desc,
        sap_go_to_mdl_key,
        sap_go_to_mdl_desc,
        sap_bnr_key,
        sap_bnr_desc,
        sap_bnr_frmt_key,
        sap_bnr_frmt_desc,
        retail_env,
        row_number () over (
            partition by sap_bnr_key,
            sap_bnr_desc,
            sap_go_to_mdl_key
            order by sap_prnt_cust_key,
                sap_prnt_cust_desc,
                sap_go_to_mdl_key
        ) as rnk
    from 
        (
            SELECT 
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
                    select cust_num,
                        min(
                            decode(cust_del_flag, null, 'O', '', 'O', CUST_DEL_FLAG)
                        ) AS CUST_DEL_FLAG
                    from edw_customer_sales_dim
                    where sls_org in ('1200')
                    group by cust_num
                ) a,
                (
                    select distinct customer_code,
                        region_name,
                        zone_name
                    from edw_customer_dim
                ) regzone
            where egch.customer(+) = ecbd.cust_num
                and ecsd.cust_num = ecbd.cust_num
                and decode(
                    ecsd.cust_del_flag,
                    NULL,
                    'O',
                    '',
                    'O',
                    ecsd.cust_del_flag
                ) = a.cust_del_flag
                and a.cust_num = ecsd.cust_num
                and ecsd.dstr_chnl = edc.distr_chan
                and ecsd.sls_org = esod.sls_org
                and esod.sls_org_co_cd = ecd.co_cd
                and ECSD.SLS_ORG IN ('1200')
                and trim(upper(cddes_pck.code_type(+))) = 'PARENT CUSTOMER KEY'
                and cddes_pck.code(+) = ecsd.prnt_cust_key
                and trim(upper(cddes_bnrkey.code_type(+))) = 'BANNER KEY'
                and cddes_bnrkey.code(+) = ecsd.bnr_key
                and trim(upper(cddes_bnrfmt.code_type(+))) = 'BANNER FORMAT KEY'
                and cddes_bnrfmt.code(+) = ecsd.bnr_frmt_key
                and trim(upper(cddes_chnl.code_type(+))) = 'CHANNEL KEY'
                and cddes_chnl.code(+) = ecsd.chnl_key
                and trim(upper(cddes_gtm.code_type(+))) = 'GO TO MODEL KEY'
                and cddes_gtm.code(+) = ecsd.go_to_mdl_key
                and trim(upper(cddes_subchnl.code_type(+))) = 'SUB CHANNEL KEY'
                and cddes_subchnl.code(+) = ecsd.sub_chnl_key
                and upper(subchnl_retail_env.sub_channel(+)) = upper(cddes_subchnl.code_desc)
                and ltrim(ecsd.cust_num, '0') = regzone.customer_code(+)
        )
    where sap_prnt_cust_key <> ''
),
inv_so_si AS 
(
    Select * from wks_taiwan_siso_propagate_final
),
onsesea as 
(
    Select * from 
    (

        SELECT 
            CAL.CAL_YEAR,
            cast(CAL.CAL_QRTR_NO as VARCHAR) as CAL_QRTR_NO,
            cast(CAL.cal_MNTH_ID as VARCHAR) as cal_MNTH_ID,
            CAL.CAL_MNTH_NO,
            'Taiwan' AS CNTRY_NM,
            TRIM(
                NVL(NULLIF(T1.sap_parent_customer_key, ''), 'NA')
            ) AS DSTRBTR_GRP_CD,
            'NA' as DSTRBTR_GRP_CD_name,
            TRIM(NVL(NULLIF(T5.prod_hier_l2, ''), 'NA')) AS GLOBAL_PROD_FRANCHISE,
            TRIM(NVL(NULLIF(T5.prod_hier_l4, ''), 'NA')) AS GLOBAL_PROD_BRAND,
            'NA' AS GLOBAL_PROD_SUB_BRAND,
            TRIM(NVL(NULLIF(T5.prod_hier_l7, ''), 'NA')) AS GLOBAL_PROD_VARIANT,
            TRIM(NVL(NULLIF(T5.prod_hier_l5, ''), 'NA')) AS GLOBAL_PROD_SEGMENT,
            'NA' AS GLOBAL_PROD_SUBSEGMENT,
            TRIM(NVL(NULLIF(T5.prod_hier_l6, ''), 'NA')) AS GLOBAL_PROD_CATEGORY,
            'NA' AS GLOBAL_PROD_SUBCATEGORY,
            --TRIM(NVL(NULLIF(T5.prod_hier_l9,''),'NA')) AS GLOBAL_PUT_UP_DESC,
            TRIM(NVL(NULLIF(T1.ean_num, ''), 'NA')) AS ean_num,
            TRIM(NVL(NULLIF(T5.SAP_MATL_NUM, ''), 'NA')) AS SKU_CD,
            --       t_lp.amount as invoice_price,
            --       t_lp1.amount as list_price,
            TRIM(NVL(NULLIF(T5.MATL_DESC, ''), 'NA')) AS SKU_DESCRIPTION,
            --TRIM(NVL(NULLIF(t5.greenlight_sku_flag,''),'NA')) AS greenlight_sku_flag,
            TRIM(NVL(NULLIF(t5.pka_product_key, ''), 'NA')) AS pka_product_key,
            TRIM(NVL(NULLIF(t5.pka_size_desc, ''), 'NA')) AS pka_size_desc,
            TRIM(
                NVL(NULLIF(t5.pka_product_key_description, ''), 'NA')
            ) AS pka_product_key_description,
            TRIM(NVL(NULLIF(t5.product_key, ''), 'NA')) AS product_key,
            TRIM(
                NVL(NULLIF(t5.product_key_description, ''), 'NA')
            ) AS product_key_description,
            'TWD' AS FROM_CCY,
            'USD' AS TO_CCY,
            T2.EXCH_RATE,
            TRIM(
                NVL(
                    NULLIF(T4.SAP_PRNT_CUST_KEY, ''),
                    'Not Assingned'
                )
            ) AS SAP_PRNT_CUST_KEY,
            TRIM(
                NVL(
                    NULLIF(T4.SAP_PRNT_CUST_DESC, ''),
                    'Not Assigned'
                )
            ) AS SAP_PRNT_CUST_DESC,
            TRIM(NVL(NULLIF(T4.SAP_CUST_CHNL_KEY, ''), 'NA')) AS SAP_CUST_CHNL_KEY,
            TRIM(NVL(NULLIF(T4.SAP_CUST_CHNL_DESC, ''), 'NA')) AS SAP_CUST_CHNL_DESC,
            TRIM(NVL(NULLIF(T4.SAP_CUST_SUB_CHNL_KEY, ''), 'NA')) AS SAP_CUST_SUB_CHNL_KEY,
            TRIM(NVL(NULLIF(T4.SAP_SUB_CHNL_DESC, ''), 'NA')) AS SAP_SUB_CHNL_DESC,
            TRIM(NVL(NULLIF(T4.SAP_GO_TO_MDL_KEY, ''), 'NA')) AS SAP_GO_TO_MDL_KEY,
            TRIM(NVL(NULLIF(T4.SAP_GO_TO_MDL_DESC, ''), 'NA')) AS SAP_GO_TO_MDL_DESC,
            TRIM(NVL(NULLIF(T4.SAP_BNR_KEY, ''), 'NA')) AS SAP_BNR_KEY,
            TRIM(NVL(NULLIF(T4.SAP_BNR_DESC, ''), 'NA')) AS SAP_BNR_DESC,
            TRIM(NVL(NULLIF(T4.SAP_BNR_FRMT_KEY, ''), 'NA')) AS SAP_BNR_FRMT_KEY,
            TRIM(NVL(NULLIF(T4.SAP_BNR_FRMT_DESC, ''), 'NA')) AS SAP_BNR_FRMT_DESC,
            TRIM(NVL(NULLIF(T4.RETAIL_ENV, ''), 'NA')) AS RETAIL_ENV,
            'TAIWAN' as REGION,
            'TAIWAN' as ZONE_OR_AREA,
            sum(last_3months_so) as last_3months_so_qty,
            sum(last_6months_so) as last_6months_so_qty,
            sum(last_12months_so) as last_12months_so_qty,
            sum(last_3months_so_value) as last_3months_so_val,
            sum(last_6months_so_value) as last_6months_so_val,
            sum(last_12months_so_value) as last_12months_so_val,
            sum(last_36months_so_value) as last_36months_so_val,
            cast(
                (sum(last_3months_so_value * T2.Exch_rate)) as numeric(38, 5)
            ) as last_3months_so_val_usd,
            cast(
                (sum(last_6months_so_value * T2.Exch_rate)) as numeric(38, 5)
            ) as last_6months_so_val_usd,
            cast(
                (sum(last_12months_so_value * T2.Exch_rate)) as numeric(38, 5)
            ) as last_12months_so_val_usd,
            propagate_flag,
            propagate_from,
            case
                when propagate_flag = 'N' then 'Not propagate'
                else reason
            end as reason,
            replicated_flag,
            SUM(T1.sell_in_qty) AS SI_SLS_QTY,
            SUM(T1.sell_in_value) AS SI_GTS_VAL,
            SUM(T1.sell_in_value * T2.EXCH_RATE) AS SI_GTS_VAL_USD,
            SUM(T1.inv_qty) AS INVENTORY_QUANTITY,
            SUM(T1.inv_value) AS INVENTORY_VAL,
            SUM(T1.inv_value * T2.EXCH_RATE) AS INVENTORY_VAL_USD,
            SUM(T1.SO_QTY) AS SO_SLS_QTY,
            SUM(T1.so_value) AS SO_TRD_SLS,
            --SUM(T1.SO_QTY * T_LP.amount) AS SO_TRD_SLS,
            SUM(T1.so_value * T2.EXCH_RATE) AS SO_TRD_SLS_USD
        FROM INV_SO_SI T1,
            --Filtered out arich 
            (
                select * from currency
                where to_ccy = 'USD'
                    and jj_mnth_id = (
                        select max(jj_mnth_id)
                        from currency
                    )
            ) T2,
            (
                select * from customer where rnk = 1
            ) T4,
            CAL,
            (
                Select * from 
                    (
                        Select *,
                            row_number() over(partition by matl_num order by matl_num) rnk
                        from 
                            (
                                select 
                                    m.matl_num,
                                    m.matl_desc,
                                    --m.greenlight_sku_flag ,
                                    m.pka_product_key,
                                    m.pka_size_desc,
                                    m.pka_product_key_description,
                                    m.pka_product_key as product_key,
                                    m.pka_product_key_description as product_key_description,
                                    pa.* --from  (Select * from edw_vw_greenlight_skus WHERE sls_org in ( '1200') ) m 
                                from (
                                        Select *
                                        from edw_material_dim
                                    ) m,
                                    edw_product_attr_dim pa
                                where pa.cntry = 'TW'
                                    and ltrim(pa.sap_matl_num, '0') = ltrim(m.matl_num, '0')
                            )
                    )
                where rnk = 1
            ) T5
        where t4.sap_bnr_key(+) = t1.bnr_key
            and cal.cal_mnth_id = t1.month
            and cal.cal_year >= (date_part(year, convert_timezone('UTC', current_timestamp())) -2)
            and t1.ean_num = t5.ean(+) --and t1.dstr_cd=t_lp.cust_sls(+)
            --and   ltrim(t1.ean_num ,0)=t_lp.ean_num (+)
            --and  ltrim(t1.dstr_cd,0)=ltrim(t_lp.cust_sls(+),0)
            --and   ltrim(t1.ean_num ,0)=t_lp1.ean_num (+)
        group by cal.cal_year,
            cal.cal_qrtr_no,
            cal.cal_mnth_id,
            cal.cal_mnth_no,
            cntry_nm,
            t1.sap_parent_customer_key,
            t5.prod_hier_l2,
            t5.prod_hier_l4,
            --t3.gph_prod_sub_brnd   ,
            t5.prod_hier_l7,
            t5.prod_hier_l5,
            --t3.gph_prod_subsgmnt   ,
            t5.prod_hier_l6,
            --t3.gph_prod_subctgry   ,
            --t5.prod_hier_l9       , 
            t1.ean_num,
            t5.sap_matl_num,
            --       t_lp.amount,
            --       t_lp1.amount,
            t5.matl_desc,
            --greenlight_sku_flag,
            pka_product_key,
            pka_size_desc,
            pka_product_key_description,
            product_key,
            product_key_description,
            from_ccy,
            to_ccy,
            t2.exch_rate,
            t4.sap_prnt_cust_key,
            t4.sap_prnt_cust_desc,
            t4.sap_cust_chnl_key,
            t4.sap_cust_chnl_desc,
            t4.sap_cust_sub_chnl_key,
            t4.sap_sub_chnl_desc,
            t4.sap_go_to_mdl_key,
            t4.sap_go_to_mdl_desc,
            t4.sap_bnr_key,
            t4.sap_bnr_desc,
            t4.sap_bnr_frmt_key,
            t4.sap_bnr_frmt_desc,
            t4.retail_env,
            region,
            zone_or_area,
            propagate_flag,
            propagate_from,
            reason,
            replicated_flag
    )
),
regional as 
( 
    select *,
        sum(si_gts_val) over (partition by cntry_nm, cal_year, cal_mnth_id) as si_inv_db_val,
        sum(si_gts_val_usd) over (partition by cntry_nm, cal_year, cal_mnth_id) as si_inv_db_val_usd
    from onsesea
    where cntry_nm || sap_prnt_cust_desc in (
            select cntry_nm || sap_prnt_cust_desc as inclusion
            from (
                    select cntry_nm,
                        sap_prnt_cust_desc,
                        nvl(sum(inventory_val), 0) as inv_val,
                        nvl(sum(so_trd_sls), 0) as sellout_val
                    from onsesea
                    where sap_prnt_cust_desc is not null
                    group by cntry_nm,
                        sap_prnt_cust_desc
                    having inv_val <> 0
                )
        )
),
final as
(    
    Select * from 
    (
        Select cal_year,
            cal_qrtr_no,
            cal_mnth_id,
            cal_mnth_no,
            cntry_nm,
            dstrbtr_grp_cd,
            dstrbtr_grp_cd_name,
            global_prod_franchise,
            global_prod_brand,
            global_prod_sub_brand,
            global_prod_variant,
            global_prod_segment,
            global_prod_subsegment,
            global_prod_category,
            global_prod_subcategory,
            pka_size_desc as global_put_up_desc,
            sku_cd,
            --invoice_price,list_price,
            sku_description,
            --greenlight_sku_flag,
            pka_product_key,
            pka_product_key_description,
            product_key,
            product_key_description,
            from_ccy,
            to_ccy,
            exch_rate,
            sap_prnt_cust_key,
            sap_prnt_cust_desc,
            sap_cust_chnl_key,
            sap_cust_chnl_desc,
            sap_cust_sub_chnl_key,
            sap_sub_chnl_desc,
            sap_go_to_mdl_key,
            sap_go_to_mdl_desc,
            sap_bnr_key,
            sap_bnr_desc,
            sap_bnr_frmt_key,
            sap_bnr_frmt_desc,
            retail_env,
            region,
            zone_or_area,
            round(cast(si_sls_qty as numeric(38, 5)), 5) as si_sls_qty,
            round(cast(si_gts_val as numeric (38, 5)), 5) as si_gts_val,
            round(cast(si_gts_val_usd as numeric(38, 5)), 5) as si_gts_val_usd,
            round(cast (inventory_quantity as numeric(38, 5)), 5) as inventory_quantity,
            round(cast(inventory_val as numeric(38, 5)), 5) as inventory_val,
            round(cast (inventory_val_usd as numeric(38, 5)), 5) as inventory_val_usd,
            round(cast (so_sls_qty as numeric(38, 5)), 5) as so_sls_qty,
            round(cast (so_trd_sls as numeric(38, 5)), 5) as so_trd_sls,
            round(cast (so_trd_sls_usd as numeric(38, 5)), 5) as so_trd_sls_usd,
            last_3months_so_qty,
            last_6months_so_qty,
            last_12months_so_qty,
            last_3months_so_val,
            last_3months_so_val_usd,
            last_6months_so_val,
            last_6months_so_val_usd,
            last_12months_so_val,
            last_12months_so_val_usd,
            propagate_flag,
            propagate_from,
            reason,
            last_36months_so_val
        from Regional
    )
)
select * from final