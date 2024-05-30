{% macro build_edw_p_load_lppb_fact_upd() %}
    {{ log("===============================================================================================") }}
    {{ log("Step1: Trying to fetch the distributor group & month from WKS table: idnwks_integration.wks_itg_all_distributor_sellin_sellout_fact ") }}
    {{ log("===============================================================================================") }}
    {{ log("Trying to set the query to fetch the distributor group & month ") }}
    {{ log("===============================================================================================") }}
    {% set get_rows_query %}
        select distinct dstrbtr_grp_cd,jj_mnth_id from alaksh01_workspace.idnwks_integration__wks_itg_all_distributor_sellin_sellout_fact
        order by dstrbtr_grp_cd,jj_mnth_id;
    {% endset %}
    {{ log("Try to execute the query to fetch the distributor group & month ") }}
    {{ log("===============================================================================================") }}
        {% set get_rows_query_result = run_query(get_rows_query) %}
        {% if execute %}
                {% set results_list = get_rows_query_result.rows %}
            {% else %}
                {% set results_list = [] %}
            {% endif %}
    {{ log("Try to execute the query to fetch the run loop ") }}
    {{ log("===============================================================================================") }}
        {% for i in results_list %}
            {% set delete_from_edw_query %}
            create or replace temporary table wks_lppb_fact_dist_grp_sellin_sellout as
            
                select distinct dstrbtr_grp_cd,jj_mnth_id from alaksh01_workspace.idnwks_integration__wks_itg_all_distributor_sellin_sellout_fact
                where upper(trim(dstrbtr_grp_cd))=upper(trim('{{ i[0] }}'))
                and jj_mnth_id='{{ i[1] }}'
            ;
            create or replace temporary table wks_lppb_fact_dist_grp_dist_id_delete as
            
                SELECT DISTINCT UPPER(TRIM(IDENTIFIER)) AS IDENTIFIER,UPPER(TRIM(SLS.JJ_SAP_DSTRBTR_ID)) AS JJ_SAP_DSTRBTR_ID,
            					   UPPER(TRIM(SLS.DSTRBTR_GRP_CD)) AS DSTRBTR_GRP_CD,
            						 SLS.JJ_MNTH_ID
            					FROM snapidnedw_integration.EDW_ALL_DISTRIBUTOR_LPPB_FACT SLS,
            					   wks_lppb_fact_dist_grp_sellin_sellout SLS1,
            					   alaksh01_workspace.idnwks_integration__WKS_ITG_ALL_DISTRIBUTOR_SELLIN_SELLOUT_MASTER DIM
            					WHERE UPPER(TRIM(SLS.DSTRBTR_GRP_CD)) = UPPER(TRIM(DIM.DSTRBTR_GRP_CD))
            					AND UPPER(TRIM(SLS.DSTRBTR_GRP_CD)) = UPPER(TRIM(SLS1.DSTRBTR_GRP_CD))
            					AND SLS.JJ_MNTH_ID=SLS1.JJ_MNTH_ID
            					AND SLS.JJ_MNTH_ID=DIM.JJ_MNTH_ID
            					AND UPPER(TRIM(IDENTIFIER))='SISO_SI'
            					UNION ALL			
            					SELECT DISTINCT UPPER(TRIM(IDENTIFIER)),UPPER(TRIM(SLS.JJ_SAP_DSTRBTR_ID)),
            					       UPPER(TRIM(SLS.DSTRBTR_GRP_CD)),
            							   SLS.JJ_MNTH_ID
            						FROM snapidnedw_integration.EDW_ALL_DISTRIBUTOR_LPPB_FACT SLS,
            							 wks_lppb_fact_dist_grp_sellin_sellout SLS1,
            							 alaksh01_workspace.idnwks_integration__WKS_ITG_ALL_DISTRIBUTOR_SELLIN_SELLOUT_MASTER DIM
            						WHERE UPPER(TRIM(SLS.DSTRBTR_GRP_CD)) = UPPER(TRIM(DIM.DSTRBTR_GRP_CD))
            						 AND UPPER(TRIM(SLS.DSTRBTR_GRP_CD)) = UPPER(TRIM(SLS1.DSTRBTR_GRP_CD))
            						 AND SLS.JJ_MNTH_ID=SLS1.JJ_MNTH_ID
            						 AND SLS.JJ_MNTH_ID=DIM.JJ_MNTH_ID
            						 AND UPPER(TRIM(IDENTIFIER))='SISO_SO'			
            					UNION ALL	 
            					 SELECT DISTINCT UPPER(TRIM(IDENTIFIER)),UPPER(TRIM(SLS.JJ_SAP_DSTRBTR_ID)),
            							 UPPER(TRIM(SLS.DSTRBTR_GRP_CD)),
            							SLS.JJ_MNTH_ID
            					 FROM snapidnedw_integration.EDW_ALL_DISTRIBUTOR_LPPB_FACT SLS,
            						  wks_lppb_fact_dist_grp_sellin_sellout SLS1,
            						  alaksh01_workspace.idnwks_integration__WKS_ITG_ALL_DISTRIBUTOR_SELLIN_SELLOUT_MASTER DIM
            					 WHERE UPPER(TRIM(SLS.DSTRBTR_GRP_CD)) = UPPER(TRIM(DIM.DSTRBTR_GRP_CD))
            					  AND UPPER(TRIM(SLS.DSTRBTR_GRP_CD)) = UPPER(TRIM(SLS1.DSTRBTR_GRP_CD))
            					  AND SLS.JJ_MNTH_ID=SLS1.JJ_MNTH_ID
            					  AND SLS.JJ_MNTH_ID=DIM.JJ_MNTH_ID
            					  AND UPPER(TRIM(IDENTIFIER))='SISO_NKA_ECOM';
            
            delete from {% if target.name=='prod' %}
                    idnedw_integration.edw_all_distributor_lppb_fact
                {% else %}
                    {{schema}}.idnedw_integration__edw_all_distributor_lppb_fact
                {% endif %}	
            WHERE (UPPER(TRIM(JJ_SAP_DSTRBTR_ID)),JJ_MNTH_ID) IN (SELECT DISTINCT UPPER(TRIM(DIM.JJ_SAP_DSTRBTR_ID)),SLS.JJ_MNTH_ID
            FROM wks_lppb_fact_dist_grp_sellin_sellout SLS,alaksh01_workspace.idnwks_integration__WKS_ITG_ALL_DISTRIBUTOR_SELLIN_SELLOUT_MASTER DIM
            WHERE UPPER(TRIM(SLS.DSTRBTR_GRP_CD)) = UPPER(TRIM(DIM.DSTRBTR_GRP_CD)) AND SLS.JJ_MNTH_ID=DIM.JJ_MNTH_ID
            UNION ALL
            SELECT DISTINCT UPPER(TRIM(SLS.JJ_SAP_DSTRBTR_ID)),SLS.JJ_MNTH_ID FROM wks_lppb_fact_dist_grp_dist_id_delete SLS WHERE UPPER(TRIM(IDENTIFIER))='SISO_SI') AND   UPPER(TRIM(IDENTIFIER))='SISO_SI';

            delete from {% if target.name=='prod' %}
                    idnedw_integration.edw_all_distributor_lppb_fact
                {% else %}
                    {{schema}}.idnedw_integration__edw_all_distributor_lppb_fact
                {% endif %}
            WHERE (UPPER(TRIM(JJ_SAP_DSTRBTR_ID)),JJ_MNTH_ID) IN (SELECT DISTINCT UPPER(TRIM(DIM.JJ_SAP_DSTRBTR_ID)),SLS.JJ_MNTH_ID 
            FROM wks_lppb_fact_dist_grp_sellin_sellout SLS, alaksh01_workspace.idnwks_integration__WKS_ITG_ALL_DISTRIBUTOR_SELLIN_SELLOUT_MASTER DIM  
            WHERE UPPER(TRIM(SLS.DSTRBTR_GRP_CD)) = UPPER(TRIM(DIM.DSTRBTR_GRP_CD)) AND SLS.JJ_MNTH_ID=DIM.JJ_MNTH_ID 
            UNION ALL 
            SELECT DISTINCT UPPER(TRIM(SLS.JJ_SAP_DSTRBTR_ID)),SLS.JJ_MNTH_ID FROM wks_lppb_fact_dist_grp_dist_id_delete SLS WHERE UPPER(TRIM(IDENTIFIER))='SISO_SO') AND  UPPER(TRIM(IDENTIFIER))='SISO_SO';

            delete from {% if target.name=='prod' %}
                    idnedw_integration.edw_all_distributor_lppb_fact
                {% else %}
                    {{schema}}.idnedw_integration__edw_all_distributor_lppb_fact
                {% endif %}
            WHERE (UPPER(TRIM(JJ_SAP_DSTRBTR_ID)),UPPER(TRIM(DSTRBTR_GRP_CD)),JJ_MNTH_ID) IN (SELECT DISTINCT UPPER(TRIM(EDD.AREA)),UPPER(TRIM(SLS.DSTRBTR_GRP_CD)),SLS.JJ_MNTH_ID 
            FROM wks_lppb_fact_dist_grp_sellin_sellout SLS, alaksh01_workspace.idnwks_integration__WKS_ITG_ALL_DISTRIBUTOR_SELLIN_SELLOUT_MASTER DIM,SNAPIDNEDW_INTEGRATION.EDW_DISTRIBUTOR_DIM EDD 
            WHERE UPPER(TRIM(SLS.DSTRBTR_GRP_CD)) = UPPER(TRIM(DIM.DSTRBTR_GRP_CD)) AND UPPER(TRIM(EDD.JJ_SAP_DSTRBTR_ID)) = UPPER(TRIM(DIM.JJ_SAP_DSTRBTR_ID))  AND  SLS.JJ_MNTH_ID = DIM.JJ_MNTH_ID 
            UNION ALL
            SELECT DISTINCT UPPER(TRIM(SLS.JJ_SAP_DSTRBTR_ID)),UPPER(TRIM(SLS.DSTRBTR_GRP_CD)),SLS.JJ_MNTH_ID FROM wks_lppb_fact_dist_grp_dist_id_delete SLS WHERE UPPER(TRIM(IDENTIFIER))='SISO_NKA_ECOM' ) AND   UPPER(TRIM(IDENTIFIER)) = 'SISO_NKA_ECOM';

            insert into {% if target.name=='prod' %}
                    idnedw_integration.edw_all_distributor_lppb_fact
                {% else %}
                    {{schema}}.idnedw_integration__edw_all_distributor_lppb_fact
                {% endif %}
            (
                IDENTIFIER,
                REC_KEY,
                CUST_PROD_CD,
                JJ_MNTH_ID,
                JJ_SAP_DSTRBTR_ID,
                DSTRBTR_GRP_CD,
                JJ_SAP_PROD_ID,
                SELLIN_QTY,
                SELLOUT_QTY,
                STRT_INV_QTY,
                END_INV_QTY,
                SELLIN_VAL,
                GROSS_SELLIN_VAL,
                SELLOUT_VAL,
                GROSS_SELLOUT_VAL,
                STRT_INV_VAL,
                END_INV_VAL,
                GROSS_STRT_INV_VAL,
                GROSS_END_INV_VAL,
                STOCK_ON_HAND_QTY,
                STOCK_ON_HAND_VAL,
                SALEABLE_STOCK_QTY,
                SALEABLE_STOCK_VALUE,
                NON_SALEABLE_STOCK_QTY,
                NON_SALEABLE_STOCK_VALUE,
                CRTD_DTTM,
                UPTD_DTTM
            )
            SELECT IDENTIFIER,
            JJ_MNTH_ID||TRIM(UPPER(JJ_SAP_DSTRBTR_ID)) ||TRIM(UPPER(JJ_SAP_PROD_ID)) AS REC_KEY,
            TRIM(UPPER(JJ_SAP_DSTRBTR_ID)) ||TRIM(UPPER(JJ_SAP_PROD_ID)) AS CUST_PROD_CD,
            JJ_MNTH_ID,
            TRIM(UPPER(JJ_SAP_DSTRBTR_ID)) AS JJ_SAP_DSTRBTR_ID,
            TRIM(UPPER(DSTRBTR_GRP_CD)) AS DSTRBTR_GRP_CD,
            TRIM(UPPER(JJ_SAP_PROD_ID)) AS JJ_SAP_PROD_ID,
            SUM(SELLIN_QTY) AS SELLIN_QTY,
            SUM(SELLOUT_QTY) AS SELLOUT_QTY,
            SUM(STRT_INV_QTY) AS STRT_INV_QTY,
            SUM(CASE WHEN SPLIT_PART(IDENTIFIER,'_',1) = 'SISO' THEN (STRT_INV_QTY + SELLIN_QTY - SELLOUT_QTY) ELSE 0 END) AS END_INV_QTY,
            SUM(SELLIN_VAL) AS SELLIN_VAL,
            SUM(GROSS_SELLIN_VAL) AS GROSS_SELLIN_VAL,
            SUM(SELLOUT_VAL) AS SELLOUT_VAL,
            SUM(GROSS_SELLOUT_VAL) AS GROSS_SELLOUT_VAL,
            SUM(STRT_INV_VAL) AS STRT_INV_VAL,
            SUM(CASE WHEN SPLIT_PART(IDENTIFIER,'_',1) = 'SISO' THEN (STRT_INV_VAL + SELLIN_VAL - SELLOUT_VAL) ELSE 0 END) AS END_INV_VAL,
            SUM(GROSS_STRT_INV_VAL) AS GROSS_STRT_INV_VAL,
            SUM(CASE WHEN SPLIT_PART(IDENTIFIER,'_',1) = 'SISO' THEN (GROSS_STRT_INV_VAL + GROSS_SELLIN_VAL - GROSS_SELLOUT_VAL) ELSE 0 END) AS GROSS_END_INV_VAL,
            SUM(STOCK_ON_HAND_QTY) AS STOCK_ON_HAND_QTY,
            SUM(STOCK_ON_HAND_VAL) AS STOCK_ON_HAND_VAL,
            SUM(SALEABLE_STOCK_QTY) AS SALEABLE_STOCK_QTY,
            SUM(SALEABLE_STOCK_VALUE) AS SALEABLE_STOCK_VALUE,
            SUM(NON_SALEABLE_STOCK_QTY) AS NON_SALEABLE_STOCK_QTY,
            SUM(NON_SALEABLE_STOCK_VALUE) AS NON_SALEABLE_STOCK_VALUE,
            MAX(current_timestamp()::timestamp_ntz),
            MAX(CAST(NULL AS TIMESTAMP))
            FROM (
            SELECT 'SISO_SI' AS IDENTIFIER,
            T1.JJ_MNTH_ID,
            T1.JJ_SAP_DSTRBTR_ID,
            DIM.DSTRBTR_GRP_CD,
            T1.JJ_SAP_PROD_ID,
            SUM(T1.SELLIN_QTY) AS SELLIN_QTY,
            SUM(0) AS SELLOUT_QTY,
            SUM(0) AS STRT_INV_QTY,
            SUM(0) AS END_INV_QTY,
            SUM(T1.SELLIN_VAL) AS SELLIN_VAL,
            SUM(T1.GROSS_SELLIN_VAL) AS GROSS_SELLIN_VAL,
            SUM(0) AS SELLOUT_VAL,
            SUM(0) AS GROSS_SELLOUT_VAL,
            SUM(0) AS STRT_INV_VAL,
            SUM(0) AS END_INV_VAL,
            SUM(0) AS GROSS_STRT_INV_VAL,
            SUM(0) AS GROSS_END_INV_VAL,
            SUM(0) AS STOCK_ON_HAND_QTY,
            SUM(0) AS STOCK_ON_HAND_VAL,
            SUM(0) AS SALEABLE_STOCK_QTY,
            SUM(0) AS SALEABLE_STOCK_VALUE,
            SUM(0) AS NON_SALEABLE_STOCK_QTY,
            SUM(0) AS NON_SALEABLE_STOCK_VALUE
            FROM alaksh01_workspace.idnwks_integration__wks_itg_all_distributor_sellin_sellout_fact T1,
                alaksh01_workspace.idnwks_integration__WKS_ITG_ALL_DISTRIBUTOR_SELLIN_SELLOUT_MASTER DIM,
                WKS_LPPB_FACT_DIST_GRP_SELLIN_SELLOUT M
            WHERE T1.JJ_MNTH_ID = M.JJ_MNTH_ID
            AND   M.JJ_MNTH_ID = DIM.JJ_MNTH_ID
            AND   UPPER(TRIM(T1.JJ_SAP_DSTRBTR_ID)) = UPPER(TRIM(DIM.JJ_SAP_DSTRBTR_ID))
            AND   UPPER(TRIM(DIM.DSTRBTR_GRP_CD)) = UPPER(TRIM(M.DSTRBTR_GRP_CD))
            AND   UPPER(TRIM(T1.IDENTIFIER)) = 'SELLIN'
            GROUP BY T1.JJ_MNTH_ID,
                    T1.JJ_SAP_DSTRBTR_ID,
                    DIM.DSTRBTR_GRP_CD,
                    T1.JJ_SAP_PROD_ID


            UNION ALL

            SELECT 'SISO_SO' AS IDENTIFIER,
                T1.JJ_MNTH_ID,
                T1.JJ_SAP_DSTRBTR_ID,
                DIM.DSTRBTR_GRP_CD,
                T1.JJ_SAP_PROD_ID,
                SUM(0) AS SELLIN_QTY,
                SUM(T1.SELLOUT_QTY) AS SELLOUT_QTY,
                SUM(0) AS STRT_INV_QTY,
                SUM(0) AS END_INV_QTY,
                SUM(0) AS SELLIN_VAL,
                SUM(0) AS GROSS_SELLIN_VAL,
                SUM(T1.SELLOUT_VAL) AS SELLOUT_VAL,
                SUM(T1.GROSS_SELLOUT_VAL) AS GROSS_SELLOUT_VAL,
                SUM(0) AS STRT_INV_VAL,
                SUM(0) AS END_INV_VAL,
                SUM(0) AS GROSS_STRT_INV_VAL,
                SUM(0) AS GROSS_END_INV_VAL,
                SUM(0) AS STOCK_ON_HAND_QTY,
                SUM(0) AS STOCK_ON_HAND_VAL,
                SUM(0) AS SALEABLE_STOCK_QTY,
                SUM(0) AS SALEABLE_STOCK_VALUE,
                SUM(0) AS NON_SALEABLE_STOCK_QTY,
                SUM(0) AS NON_SALEABLE_STOCK_VALUE
            FROM alaksh01_workspace.idnwks_integration__wks_itg_all_distributor_sellin_sellout_fact T1,
                alaksh01_workspace.idnwks_integration__WKS_ITG_ALL_DISTRIBUTOR_SELLIN_SELLOUT_MASTER DIM,
                snapidnedw_integration.EDW_DISTRIBUTOR_DIM EDD,
                WKS_LPPB_FACT_DIST_GRP_SELLIN_SELLOUT M
            WHERE T1.JJ_MNTH_ID = M.JJ_MNTH_ID
            AND   M.JJ_MNTH_ID = DIM.JJ_MNTH_ID
            AND   UPPER(TRIM(T1.JJ_SAP_DSTRBTR_ID)) = UPPER(TRIM(DIM.JJ_SAP_DSTRBTR_ID))
            AND   UPPER(TRIM(DIM.DSTRBTR_GRP_CD)) = UPPER(TRIM(M.DSTRBTR_GRP_CD))
            AND   UPPER(TRIM(DIM.JJ_SAP_DSTRBTR_ID)) = UPPER(TRIM(EDD.JJ_SAP_DSTRBTR_ID))
            and T1.JJ_MNTH_ID between EDD.effective_from(+) and EDD.effective_to(+)  
            AND   TRIM(UPPER(T1.JJ_SAP_PROD_ID)) IS NOT NULL
            AND   TRIM(UPPER(T1.JJ_SAP_PROD_ID)) != 'DAOG20'
            AND   UPPER(TRIM(EDD.REGION)) NOT IN ('NATIONAL ACCOUNT','E-COMMERCE')
            AND   UPPER(TRIM(T1.IDENTIFIER)) = 'SELLOUT'
            GROUP BY T1.JJ_MNTH_ID,
                    T1.JJ_SAP_DSTRBTR_ID,
                    DIM.DSTRBTR_GRP_CD,
                    T1.JJ_SAP_PROD_ID
            HAVING SUM(SELLOUT_VAL) <> 0
            UNION ALL

            SELECT 'SISO_SI' AS IDENTIFIER,
                CAST(M.JJ_MNTH_ID AS VARCHAR) AS JJ_MNTH_ID,
                T1.JJ_SAP_DSTRBTR_ID,
                DIM.DSTRBTR_GRP_CD,
                T1.JJ_SAP_PROD_ID,
                SUM(0) AS SELLIN_QTY,
                SUM(0) AS SELLOUT_QTY,
                SUM(END_INV_QTY) AS STRT_INV_QTY,
                SUM(0) AS END_INV_QTY,
                SUM(0) AS SELLIN_VAL,
                SUM(0) AS GROSS_SELLIN_VAL,
                SUM(0) AS SELLOUT_VAL,
                SUM(0) AS GROSS_SELLOUT_VAL,
                SUM(END_INV_VAL) AS STRT_INV_VAL,
                SUM(0) AS END_INV_VAL,
                SUM(GROSS_END_INV_VAL) AS GROSS_STRT_INV_VAL,
                SUM(0) AS GROSS_END_INV_VAL,
                SUM(0) AS STOCK_ON_HAND_QTY,
                SUM(0) AS STOCK_ON_HAND_VAL,
                SUM(0) AS SALEABLE_STOCK_QTY,
                SUM(0) AS SALEABLE_STOCK_VALUE,
                SUM(0) AS NON_SALEABLE_STOCK_QTY,
                SUM(0) AS NON_SALEABLE_STOCK_VALUE
            FROM ALAKSH01_WORKSPACE.IDNEDW_INTEGRATION__EDW_ALL_DISTRIBUTOR_LPPB_FACT T1,
                (SELECT DISTINCT DSTRBTR_GRP_CD,JJ_MNTH_ID FROM alaksh01_workspace.idnwks_integration__WKS_ITG_ALL_DISTRIBUTOR_SELLIN_SELLOUT_MASTER) DIM,
                WKS_LPPB_FACT_DIST_GRP_SELLIN_SELLOUT M
            WHERE T1.JJ_MNTH_ID = TO_CHAR(ADD_MONTHS(TO_DATE(M.JJ_MNTH_ID,'YYYYMM'),-1),'YYYYMM')
            AND   M.JJ_MNTH_ID = DIM.JJ_MNTH_ID
            AND   UPPER(TRIM(T1.DSTRBTR_GRP_CD)) = UPPER(TRIM(DIM.DSTRBTR_GRP_CD))
            AND   UPPER(TRIM(DIM.DSTRBTR_GRP_CD)) = UPPER(TRIM(M.DSTRBTR_GRP_CD))
            AND   T1.JJ_MNTH_ID != '202012'
            GROUP BY M.JJ_MNTH_ID,
                    T1.JJ_SAP_DSTRBTR_ID,
                    DIM.DSTRBTR_GRP_CD,
                    T1.JJ_SAP_PROD_ID
            UNION ALL
            SELECT 'SISO_SI' AS IDENTIFIER,
                CAST(M.JJ_MNTH_ID AS VARCHAR) AS JJ_MNTH_ID,
                DIST.JJ_SAP_DSTRBTR_ID,
                DIST.DSTRBTR_GRP_CD,
                DIST.JJ_SAP_PROD_ID,
                SUM(0) AS SELLIN_QTY,
                SUM(0) AS SELLOUT_QTY,
                SUM(NVL (DIST.TOT_STOCK,0) + NVL (DIST.CONFIRMED_QTY,0)) AS STRT_INV_QTY,
                SUM(0) AS END_INV_QTY,
                SUM(0) AS SELLIN_VAL,
                SUM(0) AS GROSS_SELLIN_VAL,
                SUM(0) AS SELLOUT_VAL,
                SUM(0) AS GROSS_SELLOUT_VAL,
                SUM(NVL (DIST.STOCK_ON_HAND_NIV,0) + NVL (DIST.BILLING_VALUE,0)) AS STRT_INV_VAL,
                SUM(0) AS END_INV_VAL,
                SUM(NVL (DIST.GROSS_INTRANSIT,0) + NVL (DIST.STOCK_VAL,0)) AS GROSS_STRT_INV_VAL,
                SUM(0) AS GROSS_END_INV_VAL,
                SUM(0) AS STOCK_ON_HAND_QTY,
                SUM(0) AS STOCK_ON_HAND_VAL,
                SUM(0) AS SALEABLE_STOCK_QTY,
                SUM(0) AS SALEABLE_STOCK_VALUE,
                SUM(0) AS NON_SALEABLE_STOCK_QTY,
                SUM(0) AS NON_SALEABLE_STOCK_VALUE
            FROM WKS_LPPB_FACT_DIST_GRP_SELLIN_SELLOUT M,
                ((SELECT CAST(T1.JJ_MNTH_ID AS VARCHAR(10)) AS JJ_MNTH_ID,
                        T1.SHIP_TO AS JJ_SAP_DSTRBTR_ID,
                        EDD.DSTRBTR_GRP_CD,
                        T1.MATL_NUM AS JJ_SAP_PROD_ID,
                        SUM(NVL (T1.CONFIRMED_QTY,0)*NVL (PD.UOM,1)) AS CONFIRMED_QTY,
                        SUM(NVL (T1.BILLING_VALUE,0)) AS BILLING_VALUE,
                        SUM(NVL (T1.CONFIRMED_QTY,0)*NVL (PD.UOM,1)*NVL (PD.PRICE,0)) AS GROSS_INTRANSIT,
                        0 AS TOT_STOCK,
                        0 AS STOCK_VAL,
                        0 AS STOCK_ON_HAND_NIV
                FROM (select t2.*,TD.jj_mnth_id from snapidnitg_integration.ITG_MDS_ID_LAV_INVENTORY_INTRANSIT T2,snapidnedw_integration.EDW_TIME_DIM TD WHERE  T2.CREATED_ON1 = TO_DATE(TD.CAL_DATE) ) T1 ,
                        snapidnedw_integration.EDW_DISTRIBUTOR_DIM EDD,
                        WKS_LPPB_FACT_DIST_GRP_SELLIN_SELLOUT M,
                        snapidnedw_integration.EDW_PRODUCT_DIM PD
                WHERE  
                    UPPER(TRIM(T1.SHIP_TO)) = UPPER(TRIM(EDD.JJ_SAP_DSTRBTR_ID))
                AND   UPPER(TRIM(EDD.DSTRBTR_GRP_CD)) = UPPER(TRIM(M.DSTRBTR_GRP_CD))
                and T1.jj_mnth_id between EDD.effective_from(+) and EDD.effective_to(+)   
                AND   T1.POD = '#N/A'
                AND   PD.JJ_SAP_PROD_ID(+) = LTRIM(T1.MATL_NUM,'0')
                and T1.jj_mnth_id between PD.effective_from(+) and PD.effective_to(+)   
                AND   T1.JJ_MNTH_ID = '202012'
                GROUP BY CAST(T1.JJ_MNTH_ID AS VARCHAR(10)),
                            T1.SHIP_TO,
                            EDD.DSTRBTR_GRP_CD,
                            T1.MATL_NUM)
                UNION ALL
                (SELECT T1.JJ_MNTH_ID,
                        T1.JJ_SAP_DSTRBTR_ID,
                        EDD.DSTRBTR_GRP_CD,
                        T1.JJ_SAP_PROD_ID,
                        0 AS CONFRIMED_QTY,
                        0 AS BILLING_VALUE,
                        0 AS GROSS_INTRANSIT,
                        SUM(T1.TOT_STOCK) AS TOT_STOCK,
                        SUM(T1.STOCK_VAL) AS STOCK_VAL,
                        CASE
                            WHEN TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) = 'DNR' THEN
                            CASE
                                WHEN UPPER(EPD.BRAND) = 'IMODIUM' THEN SUM(T1.STOCK_VAL)*0.946
                                WHEN UPPER(EPD.BRAND) = 'MOTILIUM' THEN SUM(T1.STOCK_VAL)*0.946
                                WHEN UPPER(EPD.BRAND) = 'NIZORAL' THEN SUM(T1.STOCK_VAL)*0.946
                                ELSE SUM(T1.STOCK_VAL)*0.92
                            END 
                            WHEN TRIM(UPPER(EDD.DSTRBTR_GRP_CD)) = 'SPS' THEN
                            CASE
                                WHEN UPPER(EPD.BRAND) = 'IMODIUM' THEN SUM(T1.STOCK_VAL)*0.946
                                WHEN UPPER(EPD.BRAND) = 'MOTILIUM' THEN SUM(T1.STOCK_VAL)*0.946
                                WHEN UPPER(EPD.BRAND) = 'NIZORAL' THEN SUM(T1.STOCK_VAL)*0.946
                                ELSE SUM(T1.STOCK_VAL)*0.93
                            END 
                            ELSE
                            CASE
                                WHEN UPPER(EPD.BRAND) = 'IMODIUM' THEN SUM(T1.STOCK_VAL)*0.946
                                WHEN UPPER(EPD.BRAND) = 'MOTILIUM' THEN SUM(T1.STOCK_VAL)*0.946
                                WHEN UPPER(EPD.BRAND) = 'NIZORAL' THEN SUM(T1.STOCK_VAL)*0.946
                                ELSE SUM(T1.STOCK_VAL)*0.93*0.99
                            END 
                        END AS STOCK_ON_HAND_NIV
                FROM snapidnitg_integration.ITG_STOCK_DIST_MAP T1,
                        snapidnedw_integration.EDW_DISTRIBUTOR_DIM AS EDD,
                        snapidnedw_integration.EDW_PRODUCT_DIM AS EPD,
                        WKS_LPPB_FACT_DIST_GRP_SELLIN_SELLOUT M
                WHERE T1.JJ_MNTH_ID = '202012'
                AND   T1.JJ_WK = '53'
                AND   UPPER(TRIM(EDD.DSTRBTR_GRP_CD)) = UPPER(TRIM(M.DSTRBTR_GRP_CD))
                AND   TRIM(UPPER(EDD.JJ_SAP_DSTRBTR_ID(+))) = TRIM(UPPER(T1.JJ_SAP_DSTRBTR_ID))
                and   T1.jj_mnth_id between EDD.effective_from(+) and EDD.effective_to(+)   
                AND   TRIM(UPPER(EPD.JJ_SAP_PROD_ID(+))) = TRIM(UPPER(T1.JJ_SAP_PROD_ID))
                and   T1.jj_mnth_id between EPD.effective_from(+) and EPD.effective_to(+)  
                GROUP BY T1.JJ_MNTH_ID,
                            T1.JJ_SAP_DSTRBTR_ID,
                            T1.JJ_SAP_PROD_ID,
                            EDD.DSTRBTR_GRP_CD,
                            EPD.BRAND)) DIST
            WHERE DIST.JJ_MNTH_ID = TO_CHAR(ADD_MONTHS(TO_DATE(M.JJ_MNTH_ID,'YYYYMM'),-1),'YYYYMM')
            GROUP BY M.JJ_MNTH_ID,
                    DIST.JJ_SAP_DSTRBTR_ID,
                    DIST.DSTRBTR_GRP_CD,
                    DIST.JJ_SAP_PROD_ID

                    UNION ALL
            SELECT 'SISO_NKA_ECOM' AS IDENTIFIER,
                CAST(T1.JJ_MNTH_ID AS VARCHAR) AS JJ_MNTH_ID,
                EDD.AREA AS JJ_SAP_DSTRBTR_ID,
                DIM.DSTRBTR_GRP_CD,
                T1.JJ_SAP_PROD_ID,
                SUM(0) AS SELLIN_QTY,
                SUM(T1.SELLOUT_QTY) AS SELLOUT_QTY,
                SUM(0) AS STRT_INV_QTY,
                SUM(0) AS END_INV_QTY,
                SUM(0) AS SELLIN_VAL,
                SUM(0) AS GROSS_SELLIN_VAL,
                SUM(T1.SELLOUT_VAL) AS SELLOUT_VAL,
                SUM(T1.GROSS_SELLOUT_VAL) AS GROSS_SELLOUT_VAL,
                SUM(0) AS STRT_INV_VAL,
                SUM(0) AS END_INV_VAL,
                SUM(0) AS GROSS_STRT_INV_VAL,
                SUM(0) AS GROSS_END_INV_VAL,
                SUM(0) AS STOCK_ON_HAND_QTY,
                SUM(0) AS STOCK_ON_HAND_VAL,
                SUM(0) AS SALEABLE_STOCK_QTY,
                SUM(0) AS SALEABLE_STOCK_VALUE,
                SUM(0) AS NON_SALEABLE_STOCK_QTY,
                SUM(0) AS NON_SALEABLE_STOCK_VALUE
                FROM alaksh01_workspace.idnwks_integration__wks_itg_all_distributor_sellin_sellout_fact T1,
                    alaksh01_workspace.idnwks_integration__WKS_ITG_ALL_DISTRIBUTOR_SELLIN_SELLOUT_MASTER DIM,
                    snapidnedw_integration.EDW_DISTRIBUTOR_DIM EDD,
                    WKS_LPPB_FACT_DIST_GRP_SELLIN_SELLOUT M
                WHERE T1.JJ_MNTH_ID = M.JJ_MNTH_ID
                AND   M.JJ_MNTH_ID = DIM.JJ_MNTH_ID
                AND   UPPER(TRIM(T1.JJ_SAP_DSTRBTR_ID)) = UPPER(TRIM(DIM.JJ_SAP_DSTRBTR_ID))
                AND   UPPER(TRIM(DIM.DSTRBTR_GRP_CD)) = UPPER(TRIM(M.DSTRBTR_GRP_CD))
                AND   UPPER(TRIM(DIM.JJ_SAP_DSTRBTR_ID)) = UPPER(TRIM(EDD.JJ_SAP_DSTRBTR_ID))
                and   T1.jj_mnth_id between EDD.effective_from(+) and EDD.effective_to(+)  
                AND   UPPER(TRIM(T1.JJ_SAP_PROD_ID)) IS NOT NULL
                AND   UPPER(TRIM(T1.JJ_SAP_PROD_ID)) != 'DAOG20'
                AND   UPPER(IDENTIFIER) = 'SELLOUT_NKA_ECOM'
                GROUP BY T1.JJ_MNTH_ID,
                        EDD.AREA,
                        DIM.DSTRBTR_GRP_CD,
                        T1.JJ_SAP_PROD_ID)
                GROUP BY IDENTIFIER,
                        JJ_MNTH_ID||JJ_SAP_DSTRBTR_ID||JJ_SAP_PROD_ID,
                        JJ_SAP_DSTRBTR_ID||JJ_SAP_PROD_ID,
                        JJ_MNTH_ID,
                        JJ_SAP_DSTRBTR_ID,
                        DSTRBTR_GRP_CD,
                        JJ_SAP_PROD_ID;
            {% endset %}
            {{ log("Execute edw_all_distributor_lppb_fact table load") }}
            {% do run_query(delete_from_edw_query) %}
        {% endfor %}
        {% set delete_from_query %}
        delete from {% if target.name=='prod' %}
                    idnedw_integration.edw_all_distributor_lppb_fact
                {% else %}
                    {{schema}}.idnedw_integration__edw_all_distributor_lppb_fact
                {% endif %}
        WHERE INTRANSIT_QTY > 0
        AND   NVL(JJ_MNTH_ID,'999912') IN (SELECT DISTINCT NVL(JJ_MNTH_ID,999912)
                              FROM snapidnitg_integration.ITG_MDS_ID_LAV_INVENTORY_INTRANSIT T1,
                                   snapidnedw_integration.EDW_TIME_DIM TD
                              WHERE T1.CREATED_ON1 = TO_DATE(TD.CAL_DATE))
							  AND UPPER(IDENTIFIER)='INTRANSIT';
        insert into {% if target.name=='prod' %}
                    idnedw_integration.edw_all_distributor_lppb_fact
                {% else %}
                    {{schema}}.idnedw_integration__edw_all_distributor_lppb_fact
                {% endif %}
        (
            IDENTIFIER,
            REC_KEY,
            CUST_PROD_CD,
            JJ_MNTH_ID,
            JJ_SAP_DSTRBTR_ID,
            DSTRBTR_GRP_CD,
            JJ_SAP_PROD_ID,
            INTRANSIT_QTY,
            INTRANSIT_HNA,
            INTRANSIT_NIV,
            CRTD_DTTM,
            UPTD_DTTM
        )
        SELECT 'INTRANSIT' AS IDENTIFIER,
        T1.JJ_MNTH_ID||TRIM(UPPER(LTRIM(T1.SHIP_TO,'0'))) ||TRIM(UPPER(LTRIM(T1.MATL_NUM,'0'))) AS REC_KEY,
        TRIM(UPPER(LTRIM(T1.SHIP_TO,'0'))) ||TRIM(UPPER(LTRIM(T1.MATL_NUM,'0'))) AS CUST_PROD_CD,
        T1.JJ_MNTH_ID,
        TRIM(UPPER(LTRIM(T1.SHIP_TO,'0'))) AS JJ_SAP_DSTRBTR_ID,
        TRIM(UPPER(DSTRBTR_GRP_CD)) AS DSTRBTR_GRP_CD,
        TRIM(UPPER(LTRIM(T1.MATL_NUM,'0'))) AS JJ_SAP_PROD_ID,
        SUM(NVL (CONFIRMED_QTY,0)*NVL (UOM,1)) AS INTRANSIT_QTY,
        SUM(NVL (CONFIRMED_QTY,0)*NVL (UOM,1)*NVL(PD.PRICE,0)) AS INTRANSIT_HNA,
        SUM(BILLING_VALUE) AS INTRANSIT_NIV,
        MAX(current_timestamp()::timestamp_ntz),
        MAX(CAST(NULL AS TIMESTAMP))
		FROM 
		(select T2.*,TD.JJ_MNTH_ID from snapidnitg_integration.ITG_MDS_ID_LAV_INVENTORY_INTRANSIT T2,snapidnedw_integration.EDW_TIME_DIM TD where  T2.CREATED_ON1 = TO_DATE(TD.CAL_DATE)) T1,
		    snapidnedw_integration.EDW_DISTRIBUTOR_DIM EDD,
			snapidnedw_integration.EDW_PRODUCT_DIM PD 
		WHERE 
		  T1.POD = '#N/A'
		AND  TRIM(UPPER(LTRIM(T1.SHIP_TO,'0'))) = UPPER(TRIM(EDD.JJ_SAP_DSTRBTR_ID))
		and   T1.jj_mnth_id between EDD.effective_from(+) and EDD.effective_to(+)  
		AND   PD.JJ_SAP_PROD_ID(+) = LTRIM(T1.MATL_NUM,'0')
		and   T1.jj_mnth_id between PD.effective_from(+) and PD.effective_to(+)   
		GROUP BY T1.JJ_MNTH_ID||TRIM(UPPER(LTRIM(T1.SHIP_TO,'0'))) ||TRIM(UPPER(LTRIM(T1.MATL_NUM,'0'))),
				 TRIM(UPPER(LTRIM(T1.SHIP_TO,'0'))) ||TRIM(UPPER(LTRIM(T1.MATL_NUM,'0'))),
				 T1.JJ_MNTH_ID,
				 TRIM(UPPER(LTRIM(T1.SHIP_TO,'0'))),
				 TRIM(UPPER(DSTRBTR_GRP_CD)),
				 TRIM(UPPER(LTRIM(T1.MATL_NUM,'0')));
        
        UPDATE {% if target.name=='prod' %}
                    idnedw_integration.edw_all_distributor_lppb_fact
                {% else %}
                    {{schema}}.idnedw_integration__edw_all_distributor_lppb_fact
                {% endif %}
        SET GROSS_STRT_INV_VAL = 0,
        GROSS_END_INV_VAL = 0
        WHERE JJ_MNTH_ID <= 202012;
        {% endset %}
        {% do run_query(delete_from_query) %}
{% endmacro %}