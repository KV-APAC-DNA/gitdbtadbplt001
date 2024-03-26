with EDW_VW_VN_SI_SO_INV_ANALYSIS as (

),
edw_vw_vn_si_so_inv_analysis_dksh as (

),
siso as (
    SELECT JJ_MNTH_ID AS MONTH,
            COALESCE(NULLIF(sku, ''), 'NA') AS MATL_NUM,
            NVL(SAP_PRNT_CUST_KEY, 'NA') AS SAP_PARENT_CUSTOMER_KEY,
            NVL(SAP_PRNT_CUST_DESC, 'NA') AS SAP_PARENT_CUSTOMER_DESC,
            SUM(SI_SLS_QTY) SELL_IN_QTY,
            SUM(SI_NTS_VAL) SELL_IN_VALUE,
            SUM(END_STOCK_QTY) AS INV_QTY,
            SUM(END_STOCK_VAL) AS INV_VALUE,
            SUM(SO_SLS_QTY_PC) - sum(SO_RET_QTY_PC) SO_QTY,
            SUM(SO_GRS_TRD_SLS) + sum(SO_RET_VAL) SO_VALUE
        FROM EDW_VW_VN_SI_SO_INV_ANALYSIS
        GROUP BY JJ_MNTH_ID,
            SKU,
            NVL(SAP_PRNT_CUST_KEY, 'NA'),
            NVL(SAP_PRNT_CUST_DESC, 'NA')
),
siso_dksh as (
    Select JJ_MNTH_ID,
            sku,
            sap_prnt_cust_key,
            sap_prnt_cust_desc,
            sum(si_sls_qty),
            sum(si_gts_val),
            sum(end_stock_qty),
            sum(end_stock_val),
            sum(so_sls_qty_pc),
            sum(so_grs_trd_sls)
        from edw_vw_vn_si_so_inv_analysis_dksh
        group by JJ_MNTH_ID,
            sku,
            sap_prnt_cust_key,
            sap_prnt_cust_desc
),
final as (
    SELECT 
        MONTH,
        COALESCE(NULLIF(matl_num, ''), 'NA') AS MATL_NUM,
        NVL(SAP_PARENT_CUSTOMER_KEY, 'NA') AS SAP_PARENT_CUSTOMER_KEY,
        NVL(SAP_PARENT_CUSTOMER_DESC, 'NA') AS SAP_PARENT_CUSTOMER_DESC,
        SELL_IN_QTY,
        SELL_IN_VALUE,
        INV_QTY,
        INV_VALUE,
        SO_QTY,
        SO_VALUE
    FROM
    (
        select * from siso
        union all
        select * from siso_dksh
    )
    WHERE left(month, 4) >= (DATE_PART(YEAR, SYSDATE) -6)
)
select * from final