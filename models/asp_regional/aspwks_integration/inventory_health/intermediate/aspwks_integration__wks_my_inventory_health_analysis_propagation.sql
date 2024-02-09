with wks_my_siso_propagate_final as
(
    select * from {{ ref('myswks_integration__wks_my_siso_propagate_final') }}
),
itg_my_dstrbtrr_dim as
(
    select * from {{ ref('mysitg_integration__itg_my_dstrbtrr_dim') }}
),
edw_vw_my_customer_dim as
(
    select * from {{ ref('mysedw_integration__edw_vw_my_customer_dim') }}
),
vw_edw_reg_exch_rate as
(
    select * from {{ ref('aspedw_integration__vw_edw_reg_exch_rate') }}
),
edw_vw_my_si_pos_inv_analysis as
(
    select * from {{ ref('mysedw_integration__edw_vw_my_si_pos_inv_analysis') }}
),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_copa_trans_fact as
(
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_my_siso_analysis as
(
   select * from {{ ref('mysedw_integration__edw_my_siso_analysis') }}
),
edw_company_dim as
(
   select * from {{ ref('aspedw_integration__edw_company_dim') }} 
),
v_edw_customer_sales_dim as
(
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }} 
),
siso as
(
        select * from wks_my_siso_propagate_final
        where sap_parent_customer_desc <> ''
),
t2 as
(
        select
        cust_id,
        (lvl1 || '-' || region) as region,
        substring
        (
            replace(replace(lvl3, '(', '- '), ')', ''),
            case
            when position('-', replace(replace(lvl3, '(', '- '), ')', '')) + 1 = 1
            then 999
            else position('-', replace(replace(lvl3, '(', '- '), ')', '')) + 1
            end
        ) as zone_or_area
        from itg_my_dstrbtrr_dim
),
cust as
(
    select
        t1.*,
        t2.region,
        t2.zone_or_area
    from edw_vw_my_customer_dim as t1, t2
    where
        t1.sap_cntry_cd = 'MY' and ltrim(t1.sap_cust_id, '0') = t2.cust_id(+)
),
c as
(
    SELECT
        *
    FROM vw_edw_reg_exch_rate
    WHERE
        cntry_key = 'MY'
        AND TO_CCY = 'USD'
        AND JJ_MNTH_ID = (
        SELECT
            MAX(JJ_MNTH_ID)
        FROM vw_edw_reg_exch_rate
        )
),
product as
(
    SELECT
        *
    FROM (
        SELECT DISTINCT
        GLOBAL_PROD_FRANCHISE,
        GLOBAL_PROD_BRAND,
        GLOBAL_PROD_SUB_BRAND,
        GLOBAL_PROD_VARIANT,
        GLOBAL_PROD_SEGMENT,
        GLOBAL_PROD_SUBSEGMENT,
        GLOBAL_PROD_CATEGORY,
        GLOBAL_PROD_SUBCATEGORY,
        GLOBAL_PUT_UP_DESC,
        COALESCE(NULLIF(sku, ''), 'NA') AS sku,
        SKU_DESC
        FROM EDW_MY_SISO_ANALYSIS
        WHERE
        TO_CCY = 'MYR'
        UNION
        SELECT DISTINCT
        GLOBAL_PROD_FRANCHISE,
        GLOBAL_PROD_BRAND,
        GLOBAL_PROD_SUB_BRAND,
        GLOBAL_PROD_VARIANT,
        GLOBAL_PROD_SEGMENT,
        GLOBAL_PROD_SUBSEGMENT,
        GLOBAL_PROD_CATEGORY,
        GLOBAL_PROD_SUBCATEGORY,
        GLOBAL_PUT_UP_DESC,
        COALESCE(NULLIF(sku, ''), 'NA') AS sku,
        SKU_DESC
        FROM EDW_VW_MY_SI_POS_INV_ANALYSIS
    )
    WHERE
        sku <> 'NA'
),
product1 as
(
    SELECT
        *
    FROM (
        SELECT
        product.*,
        EMD.pka_product_key as pka_product_key,
        EMD.pka_product_key_description as pka_product_key_description,
        EMD.pka_product_key as product_key,
        EMD.pka_product_key_description as product_key_description,
        EMD.pka_size_desc as pka_size_desc,
        ROW_NUMBER() OVER (PARTITION BY sku ORDER BY sku ) AS rnk
        FROM  product
        LEFT JOIN (
        select * from edw_material_dim
        ) AS EMD
        ON product.sku = LTRIM(EMD.MATL_NUM, '0')
    )
    WHERE
        rnk = 1
),
TIME as
(
    SELECT DISTINCT
        cal_year AS YEAR,
        cal_qrtr_no AS qrtr_no,
        cal_MNTH_ID AS mnth_id,
        cal_MNTH_NO AS mnth_no
    FROM EDW_VW_OS_TIME_DIM
),

ONSESEA AS (
      SELECT
        CAST(TIME.YEAR AS VARCHAR(10)) AS YEAR,
        CAST(TIME.QRTR_NO AS VARCHAR(14)) AS QRTR_NO,
        CAST(TIME.MNTH_ID AS VARCHAR(21)) AS MONTH_YEAR,
        CAST(TIME.MNTH_NO AS VARCHAR(10)) AS mnth_no,
        CAST('Malaysia' AS VARCHAR) AS COUNTRY_NAME,
        TRIM(COALESCE(NULLIF(SISO.DSTRBTR_GRP_CD, ''), 'NA')) AS DSTRBTR_GRP_CD,
        TRIM(COALESCE(NULLIF(SISO.distributor, ''), 'NA')) AS distributor_id_name,
        TRIM(COALESCE(NULLIF(product.GLOBAL_PROD_FRANCHISE, ''), 'NA')) AS GLOBAL_PROD_FRANCHISE,
        TRIM(COALESCE(NULLIF(product.GLOBAL_PROD_BRAND, ''), 'NA')) AS GLOBAL_PROD_BRAND,
        TRIM(COALESCE(NULLIF(product.GLOBAL_PROD_SUB_BRAND, ''), 'NA')) AS GLOBAL_PROD_SUB_BRAND,
        TRIM(COALESCE(NULLIF(product.GLOBAL_PROD_VARIANT, ''), 'NA')) AS GLOBAL_PROD_VARIANT,
        TRIM(COALESCE(NULLIF(product.GLOBAL_PROD_SEGMENT, ''), 'NA')) AS GLOBAL_PROD_SEGMENT,
        TRIM(COALESCE(NULLIF(product.GLOBAL_PROD_SUBSEGMENT, ''), 'NA')) AS GLOBAL_PROD_SUBSEGMENT,
        TRIM(COALESCE(NULLIF(product.GLOBAL_PROD_CATEGORY, ''), 'NA')) AS GLOBAL_PROD_CATEGORY,
        TRIM(COALESCE(NULLIF(product.GLOBAL_PROD_SUBCATEGORY, ''), 'NA')) AS GLOBAL_PROD_SUBCATEGORY,
        TRIM(COALESCE(NULLIF(product.pka_size_desc, ''), 'NA')) AS pka_size_desc,
        TRIM(COALESCE(NULLIF(product.SKU, ''), 'NA')) AS SKU_CD,
        TRIM(COALESCE(NULLIF(product.SKU_DESC, ''), 'NA')) AS SKU_DESCRIPTION,
        TRIM(COALESCE(NULLIF(product.pka_product_key, ''), 'NA')) AS pka_product_key,
        TRIM(COALESCE(NULLIF(product.pka_product_key_description, ''), 'NA')) AS pka_product_key_description,
        TRIM(COALESCE(NULLIF(product.product_key, ''), 'NA')) AS product_key,
        TRIM(COALESCE(NULLIF(product.product_key_description, ''), 'NA')) AS product_key_description,
        CAST('MYR' AS VARCHAR) AS FROM_CCY,
        'USD' AS TO_CCY,
        C.EXCH_RATE,
        TRIM(COALESCE(NULLIF(cust.SAP_PRNT_CUST_KEY, ''), 'NA')) AS SAP_PRNT_CUST_KEY,
        TRIM(COALESCE(NULLIF(cust.SAP_PRNT_CUST_DESC, ''), 'NA')) AS SAP_PRNT_CUST_DESC,
        TRIM(COALESCE(NULLIF(cust.SAP_CUST_CHNL_KEY, ''), 'NA')) AS SAP_CUST_CHNL_KEY,
        TRIM(COALESCE(NULLIF(cust.SAP_CUST_CHNL_DESC, ''), 'NA')) AS SAP_CUST_CHNL_DESC,
        TRIM(COALESCE(NULLIF(cust.SAP_CUST_SUB_CHNL_KEY, ''), 'NA')) AS SAP_CUST_SUB_CHNL_KEY,
        TRIM(COALESCE(NULLIF(cust.SAP_SUB_CHNL_DESC, ''), 'NA')) AS SAP_SUB_CHNL_DESC,
        TRIM(COALESCE(NULLIF(cust.SAP_GO_TO_MDL_KEY, ''), 'NA')) AS SAP_GO_TO_MDL_KEY,
        TRIM(COALESCE(NULLIF(cust.SAP_GO_TO_MDL_DESC, ''), 'NA')) AS SAP_GO_TO_MDL_DESC,
        TRIM(COALESCE(NULLIF(cust.SAP_BNR_KEY, ''), 'NA')) AS SAP_BNR_KEY,
        TRIM(COALESCE(NULLIF(cust.SAP_BNR_DESC, ''), 'NA')) AS SAP_BNR_DESC,
        TRIM(COALESCE(NULLIF(cust.SAP_BNR_FRMT_KEY, ''), 'NA')) AS SAP_BNR_FRMT_KEY,
        TRIM(COALESCE(NULLIF(cust.SAP_BNR_FRMT_DESC, ''), 'NA')) AS SAP_BNR_FRMT_DESC,
        TRIM(COALESCE(NULLIF(cust.RETAIL_ENV, ''), 'NA')) AS RETAIL_ENV,
        CASE
          WHEN SAP_PRNT_CUST_KEY = 'PC0004'
          THEN 'Not Applicable'
          ELSE TRIM(COALESCE(NULLIF(cust.REGION, ''), 'NA'))
        END AS REGION,
        CASE
          WHEN SAP_PRNT_CUST_KEY = 'PC0004'
          THEN 'Not Applicable'
          ELSE TRIM(COALESCE(NULLIF(cust.ZONE_OR_AREA, ''), 'NA'))
        END AS ZONE_OR_AREA,
        SUM(last_3months_so) AS last_3months_so_qty,
        SUM(last_6months_so) AS last_6months_so_qty,
        SUM(last_12months_so) AS last_12months_so_qty,
        SUM(last_3months_so_value) AS last_3months_so_val,
        SUM(last_6months_so_value) AS last_6months_so_val,
        SUM(last_12months_so_value) AS last_12months_so_val,
        SUM(last_36months_so_value) AS last_36months_so_val,
        CAST((SUM(last_3months_so_value) * c.Exch_rate) AS DECIMAL(38, 5)) AS last_3months_so_val_usd,
        CAST((SUM(last_6months_so_value) * c.Exch_rate) AS DECIMAL(38, 5)) AS last_6months_so_val_usd,
        CAST((SUM(last_12months_so_value) * c.Exch_rate) AS DECIMAL(38, 5)) AS last_12months_so_val_usd,
        propagate_flag,
        propagate_from,
        CASE WHEN propagate_flag = 'N' THEN 'Not propagate' ELSE reason END AS reason,
        replicated_flag,
        SUM(sell_in_qty) AS SI_SLS_QTY,
        SUM(sell_in_value) AS SI_GTS_VAL,
        SUM(sell_in_value * C.EXCH_RATE) AS SI_GTS_VAL_USD,
        SUM(inv_qty) AS INVENTORY_QUANTITY,
        SUM(inv_value) AS INVENTORY_VAL,
        SUM(inv_value * C.EXCH_RATE) AS INVENTORY_VAL_USD,
        SUM(so_qty) AS SO_SLS_QTY,
        SUM(so_value) AS SO_GRS_TRD_SLS,
        ROUND(SUM(so_value * C.EXCH_RATE)) AS SO_GRS_TRD_SLS_USD
      FROM SISO,cust,time,product1 as product,
      C
      /* (SELECT * from CUST where rank=1)customer */
     WHERE 
        LTRIM(SISO.DSTRBTR_GRP_CD,'0')=LTRIM(cust.SAP_CUST_ID(+),'0')
        AND LEFT(SISO.month,4) >=(DATE_PART(YEAR,CURRENT_TIMESTAMP::date)-2)
        AND SISO.month=time.MNTH_ID
        AND SISO.matl_num=product.sku(+)
      GROUP BY
        time.YEAR,
        time.QRTR_NO,
        time.MNTH_ID,
        time.MNTH_NO, /* MNTH_WK_NO, */
        CNTRY_NM,
        DSTRBTR_GRP_CD,
        distributor,
        GLOBAL_PROD_FRANCHISE,
        GLOBAL_PROD_BRAND,
        GLOBAL_PROD_SUB_BRAND,
        GLOBAL_PROD_VARIANT,
        GLOBAL_PROD_SEGMENT,
        GLOBAL_PROD_SUBSEGMENT,
        GLOBAL_PROD_CATEGORY,
        GLOBAL_PROD_SUBCATEGORY,
        pka_size_desc, /* GLOBAL_PUT_UP_DESC, */
        SKU,
        SKU_DESC,
        pka_product_key,
        pka_product_key_description,
        product_key,
        product_key_description,
        C.EXCH_RATE,
        SAP_PRNT_CUST_KEY,
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
        CASE
          WHEN SAP_PRNT_CUST_KEY = 'PC0004'
          THEN 'Not Applicable'
          ELSE TRIM(COALESCE(NULLIF(cust.REGION, ''), 'NA'))
        END,
        CASE
          WHEN SAP_PRNT_CUST_KEY = 'PC0004'
          THEN 'Not Applicable'
          ELSE TRIM(COALESCE(NULLIF(cust.ZONE_OR_AREA, ''), 'NA'))
        END,
        propagate_flag,
        propagate_from,
        reason,
        replicated_flag
),
Regional AS 
(
    
    SELECT
    *,
    SUM(SI_GTS_VAL) OVER (PARTITION BY COUNTRY_NAME, YEAR, MONTH_YEAR) AS SI_INV_DB_VAL,
    SUM(SI_GTS_VAL_USD) OVER (PARTITION BY COUNTRY_NAME, YEAR, MONTH_YEAR) AS SI_INV_DB_VAL_USD
    FROM ONSESEA
    WHERE
    COUNTRY_NAME || SAP_PRNT_CUST_DESC IN 
    (
        SELECT
        COUNTRY_NAME || SAP_PRNT_CUST_DESC AS INCLUSION
        FROM 
        (
            SELECT
                COUNTRY_NAME,
                SAP_PRNT_CUST_DESC,
                COALESCE(SUM(INVENTORY_VAL), 0) AS INV_VAL,
                COALESCE(SUM(SO_GRS_TRD_SLS), 0) AS Sellout_val
            FROM ONSESEA
            WHERE
                NOT SAP_PRNT_CUST_DESC IS NULL
            GROUP BY
                COUNTRY_NAME,
                SAP_PRNT_CUST_DESC
                HAVING
                INV_VAL <> 0
        )
    )
),
prestep as
(
    select cast(year as integer) as year,
            qrtr_no as year_quarter,
            month_year,
            cast(mnth_no as integer) as month_number,
            country_name,
            dstrbtr_grp_cd,
            distributor_id_name,
            global_prod_franchise as franchise,
            global_prod_brand as brand,
            global_prod_sub_brand as prod_sub_brand,
            global_prod_variant as variant,
            global_prod_segment as segment,
            global_prod_subsegment as prod_subsegment,
            global_prod_category as prod_category,
            global_prod_subcategory as prod_subcategory,
            pka_size_desc as put_up_description,
            sku_cd,
            sku_description,
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
            round(cast (so_grs_trd_sls as numeric(38, 5)), 5) as so_grs_trd_sls,
            so_grs_trd_sls_usd as so_grs_trd_sls_usd,
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
), 
RegionalCurrency AS
(
    SELECT
        cntry_key,
        cntry_nm,
        rate_type,
        from_ccy,
        to_ccy,
        valid_date,
        jj_year,
        jj_mnth_id AS MNTH_ID,
        (CAST(EXCH_RATE AS DECIMAL(15, 5))) AS EXCH_RATE
    FROM vw_edw_reg_exch_rate
    WHERE
    cntry_key = 'MY'
    AND jj_mnth_id >= (
        DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 2
    )
    AND to_ccy = 'USD'
),
sellin_all as (
    Select ctry_key,
        obj_crncy_co_obj,
        prnt_cust_key,
        caln_yr_mo,
        fisc_yr,
    (cast(gts as numeric(38, 15))) as gts
    from (
            select copa.ctry_key as ctry_key,
                obj_crncy_co_obj,
                cus_sales_extn.prnt_cust_key,
                substring(fisc_yr_per, 1, 4) || substring(fisc_yr_per, 6, 2) as caln_yr_mo,
                fisc_yr,
                SUM(amt_obj_crncy) AS gts
            from edw_copa_trans_fact copa
                LEFT JOIN edw_company_dim cmp ON copa.co_cd = cmp.co_cd
                LEFT JOIN v_edw_customer_sales_dim cus_sales_extn ON copa.sls_org = cus_sales_extn.sls_org
                AND copa.dstr_chnl = cus_sales_extn.dstr_chnl::TEXT
                AND copa.div = cus_sales_extn.div
                AND copa.cust_num = cus_sales_extn.cust_num
            WHERE cmp.ctry_group = 'Malaysia'
                and left(fisc_yr_per, 4) >= (DATE_PART(YEAR, current_timestamp) -2)
                and copa.cust_num is not null
                and copa.acct_hier_shrt_desc = 'GTS'
                and amt_obj_crncy > 0
            group by 1,
                2,
                3,
                4,
                5
        )
),
available_customers as (
    select month_year,
        country_name,
        sap_prnt_cust_key,
        sap_prnt_cust_desc,
        sum(si_gts_val) as si_gts_val,
        sum(si_sls_qty) as si_sls_qty
    from prestep inv
    where country_name in ('Malaysia')
    group by 1,
        2,
        3,
        4
    having (
            sum(inventory_quantity) <> 0
            or sum(inventory_val) <> 0
        )
    order by 1 desc,
        2,
        3,
        4
),
GTS AS 
(
    Select 
    ctry_key,
    obj_crncy_co_obj,
    caln_yr_mo,
    fisc_yr,
    sum(SI_ALL_DB_VAL) as gts_value,
    sum(
        case
            when avail_customer is null then 0
            else si_all_db_val
        end
    ) as si_inv_db_val
from(
        select a.ctry_key,
                a.obj_crncy_co_obj,
                a.caln_yr_mo,
                a.fisc_yr,
                a.prnt_cust_key as total_customer,
                b.sap_prnt_cust_key as avail_customer,
                sum(gts) as SI_ALL_DB_VAL
            from sellin_all a
                left join available_customers b on b.month_year = a.caln_yr_mo
                and a.prnt_cust_key = b.sap_prnt_cust_key
            group by 1,
                2,
                3,
                4,
                5,
                6
            order by 1 desc,
                2,
                3,
                4
      )
    group by 1,
                    2,
                    3,
                    4
),
COPA AS 
(
    Select ctry_key,
                obj_crncy_co_obj,
                caln_yr_mo,
                fisc_yr,
                (cast (gts_value as numeric(38, 5))) as gts,
                si_inv_db_val,
                Case
                    when ctry_key = 'MY' then cast((gts_value * exch_rate) / 1000 as numeric(38, 5))
                end as GTS_USD,
                case
                    when ctry_key = 'MY' then cast((si_inv_db_val * exch_rate) / 1000 as numeric(38, 5))
                end as si_inv_db_val_usd
            FROM gts,
                RegionalCurrency
            WHERE GTS.obj_crncy_co_obj = RegionalCurrency.from_ccy
                AND RegionalCurrency.MNTH_ID =(
                    Select max(MNTH_ID)
                    from RegionalCurrency
                )
),
final as
(
    SELECT
        cast(year as integer) as year,
            qrtr_no as year_quarter,
            month_year,
            cast(mnth_no as integer) as month_number,
            country_name,
            dstrbtr_grp_cd,
            distributor_id_name,
            global_prod_franchise as franchise,
            global_prod_brand as brand,
            global_prod_sub_brand as prod_sub_brand,
            global_prod_variant as variant,
            global_prod_segment as segment,
            global_prod_subsegment as prod_subsegment,
            global_prod_category as prod_category,
            global_prod_subcategory as prod_subcategory,
            pka_size_desc as put_up_description,
            sku_cd,
            sku_description,
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
            round(cast (so_grs_trd_sls as numeric(38, 5)), 5) as so_grs_trd_sls,
            so_grs_trd_sls_usd as so_grs_trd_sls_usd,
            round(cast (COPA.gts as numeric(38, 5)), 5) as SI_ALL_DB_VAL,
            round(cast (COPA.gts_usd as numeric (38, 5)), 5) as SI_ALL_DB_VAL_USD,
            round(cast (COPA.si_inv_db_val as numeric(38, 5)), 5) as si_inv_db_val,
            round(cast (COPA.si_inv_db_val_usd as numeric(38, 5)), 5) as si_inv_db_val_usd,
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
        from Regional,
            COPA
        where Regional.year = COPA.fisc_yr
            and Regional.month_year = COPA.caln_yr_mo
            and Regional.from_ccy = COPA.obj_crncy_co_obj
)
select
    year::number(18,0) as year,
    year_quarter::varchar(14) as year_quarter,
    month_year::varchar(21) as month_year,
    month_number::number(18,0) as month_number,
    country_name::varchar(8) as country_name,
    dstrbtr_grp_cd::varchar(30) as dstrbtr_grp_cd,
    distributor_id_name::varchar(40) as distributor_id_name,
    franchise::varchar(30) as franchise,
    brand::varchar(30) as brand,
    prod_sub_brand::varchar(100) as prod_sub_brand,
    variant::varchar(100) as variant,
    segment::varchar(50) as segment,
    prod_subsegment::varchar(100) as prod_subsegment,
    prod_category::varchar(50) as prod_category,
    prod_subcategory::varchar(50) as prod_subcategory,
    put_up_description::varchar(30) as put_up_description,
    sku_cd::varchar(100) as sku_cd,
    sku_description::varchar(100) as sku_description,
    pka_product_key::varchar(68) as pka_product_key,
    pka_product_key_description::varchar(255) as pka_product_key_description,
    product_key::varchar(68) as product_key,
    product_key_description::varchar(255) as product_key_description,
    from_ccy::varchar(3) as from_ccy,
    to_ccy::varchar(3) as to_ccy,
    exch_rate::number(15,5) as exch_rate,
    sap_prnt_cust_key::varchar(12) as sap_prnt_cust_key,
    sap_prnt_cust_desc::varchar(50) as sap_prnt_cust_desc,
    sap_cust_chnl_key::varchar(12) as sap_cust_chnl_key,
    sap_cust_chnl_desc::varchar(50) as sap_cust_chnl_desc,
    sap_cust_sub_chnl_key::varchar(12) as sap_cust_sub_chnl_key,
    sap_sub_chnl_desc::varchar(50) as sap_sub_chnl_desc,
    sap_go_to_mdl_key::varchar(12) as sap_go_to_mdl_key,
    sap_go_to_mdl_desc::varchar(50) as sap_go_to_mdl_desc,
    sap_bnr_key::varchar(12) as sap_bnr_key,
    sap_bnr_desc::varchar(50) as sap_bnr_desc,
    sap_bnr_frmt_key::varchar(12) as sap_bnr_frmt_key,
    sap_bnr_frmt_desc::varchar(50) as sap_bnr_frmt_desc,
    retail_env::varchar(50) as retail_env,
    region::varchar(61) as region,
    zone_or_area::varchar(80) as zone_or_area,
    si_sls_qty::number(38,5) as si_sls_qty,
    si_gts_val::number(38,5) as si_gts_val,
    si_gts_val_usd::number(38,5) as si_gts_val_usd,
    inventory_quantity::number(38,5) as inventory_quantity,
    inventory_val::number(38,5) as inventory_val,
    inventory_val_usd::number(38,5) as inventory_val_usd,
    so_sls_qty::number(38,5) as so_sls_qty,
    so_grs_trd_sls::number(38,5) as so_grs_trd_sls,
    so_grs_trd_sls_usd::number(17,0) as so_grs_trd_sls_usd,
    si_all_db_val::number(38,5) as si_all_db_val,
    si_all_db_val_usd::number(38,5) as si_all_db_val_usd,
    si_inv_db_val::number(38,5) as si_inv_db_val,
    si_inv_db_val_usd::number(38,5) as si_inv_db_val_usd,
    last_3months_so_qty::number(38,6) as last_3months_so_qty,
    last_6months_so_qty::number(38,6) as last_6months_so_qty,
    last_12months_so_qty::number(38,6) as last_12months_so_qty,
    last_3months_so_val::number(38,17) as last_3months_so_val,
    last_3months_so_val_usd::number(38,5) as last_3months_so_val_usd,
    last_6months_so_val::number(38,17) as last_6months_so_val,
    last_6months_so_val_usd::number(38,5) as last_6months_so_val_usd,
    last_12months_so_val::number(38,17) as last_12months_so_val,
    last_12months_so_val_usd::number(38,5) as last_12months_so_val_usd,
    propagate_flag::varchar(1) as propagate_flag,
    propagate_from::number(18,0) as propagate_from,
    reason::varchar(100) as reason,
    last_36months_so_val::number(38,17) as last_36months_so_val
 from final