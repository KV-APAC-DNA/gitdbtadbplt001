with vw_pacific_inventory as
(
    select * from {{ ref('pcfedw_integration__vw_pacific_inventory') }}
),
vw_pacific_sellout as
(
    select * from {{ ref('pcfedw_integration__vw_pacific_sellout') }}
),
itg_mds_pacific_prod_mapping_cwh as
(
    select * from {{ ref('pcfitg_integration__itg_mds_pacific_prod_mapping_cwh') }}
),
edw_copa_trans_fact as
(
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_customer_sales_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_customer_base_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_code_descriptions as
(
    select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
inv_so as
(
   SELECT inv.sap_parent_customer_key,
      inv.sap_parent_customer_desc,
      inv.matl_num,
      JJ_mnth as inv_month,
      0 as so_qty,
      0 as so_value,
      sum(inv.inventory_qty) inv_qty,
      sum(inv.inventory_amount) as inv_value
   FROM vw_pacific_inventory inv,
   (
      SELECT sap_parent_customer_key,
            sap_parent_customer_desc,
            --							   matl_num,
            JJ_mnth as inv_month1,
            max(inv_date) max_inv_date
      FROM vw_pacific_inventory inn
      GROUP BY inn.sap_parent_customer_key,
            inn.sap_parent_customer_desc,
            --  inn.matl_num,
            JJ_mnth
   ) inv_sel
   WHERE inv.sap_parent_customer_key = inv_sel.sap_parent_customer_key --AND  coalesce(inv.matl_num,'NA')=coalesce(inv_sel.matl_num,'NA')
      AND JJ_mnth = inv_sel.inv_month1
      AND inv.inv_date = inv_sel.max_inv_date
   GROUP BY inv.sap_parent_customer_key,
      inv.sap_parent_customer_desc,
      inv.matl_num,
      JJ_mnth
   UNION ALL
   --Sellout
   SELECT SAP_PRNT_CUST_KEY,
      SAP_PRNT_CUST_DESC,
      matl_id,
      "month",
      sum(so_qty) as so_qty,
      sum(so_value) as so_value,
      0 as inv_qty,
      0 as inv_value
   FROM vw_pacific_sellout
   GROUP BY SAP_PRNT_CUST_KEY,
      SAP_PRNT_CUST_DESC,
      matl_id,
      "month"
),
gts as
(
   SELECT SAP_PRNT_CUST_KEY,
      SAP_PRNT_CUST_DESC,
      ltrim(copa.matl_num, '0') matl_num,
      substring(copa.fisc_yr_per, 1, 4) || substring(copa.fisc_yr_per, 6, 2) as fisc_month,
      0 as so_qty,
      0 as inv_qty,
      sum(copa.qty) AS gts_qty,
      sum(copa.amt_obj_crncy) AS gts_value
   FROM edw_copa_trans_fact copa,
   (
      SELECT DISTINCT ECBD.CUST_NUM,
            ECSD.PRNT_CUST_KEY AS SAP_PRNT_CUST_KEY,
            CDDES_PCK.CODE_DESC AS SAP_PRNT_CUST_DESC
      FROM EDW_CUSTOMER_SALES_DIM ECSD,
            EDW_CUSTOMER_BASE_DIM ECBD,
            EDW_CODE_DESCRIPTIONS CDDES_PCK
      WHERE ECSD.CUST_NUM = ECBD.CUST_NUM
            AND ECSD.SLS_ORG IN ('3300', '330B', '330H')
            AND CDDES_PCK.CODE_TYPE(+) = 'Parent Customer Key'
            AND CDDES_PCK.CODE(+) = ECSD.PRNT_CUST_KEY ---Jan 2023 CWH Ingestion - Added code below to stop CWH here to fetch it later after join with CWH PROD MDS table
            AND UPPER(CDDES_PCK.CODE_DESC) != 'CHEMIST WAREHOUSE'
   ) Parent_Key
   WHERE ctry_key = 'AU'
      AND copa.acct_hier_shrt_desc = 'GTS'
      AND copa.cust_num = Parent_key.cust_num
      AND fisc_yr >= (DATE_PART(YEAR, current_timestamp) -6)
   GROUP BY SAP_PRNT_CUST_KEY,
      SAP_PRNT_CUST_DESC,
      matl_num,
      substring(copa.fisc_yr_per, 1, 4) || substring(copa.fisc_yr_per, 6, 2)
   union all
   SELECT SAP_PRNT_CUST_KEY,
      SAP_PRNT_CUST_DESC,
      ltrim(copa.matl_num, '0') matl_num,
      substring(copa.fisc_yr_per, 1, 4) || substring(copa.fisc_yr_per, 6, 2) as fisc_month,
      0 as so_qty,
      0 as inv_qty,
      sum(copa.qty) AS gts_qty,
      sum(copa.amt_obj_crncy) AS gts_value
   FROM edw_copa_trans_fact copa,
      itg_mds_pacific_prod_mapping_CWH cwhprod,
      (
         SELECT 
            DISTINCT ECBD.CUST_NUM,
            ECSD.PRNT_CUST_KEY AS SAP_PRNT_CUST_KEY,
            CDDES_PCK.CODE_DESC AS SAP_PRNT_CUST_DESC
         FROM EDW_CUSTOMER_SALES_DIM ECSD,
         EDW_CUSTOMER_BASE_DIM ECBD,
         EDW_CODE_DESCRIPTIONS CDDES_PCK
         WHERE ECSD.CUST_NUM = ECBD.CUST_NUM
         AND ECSD.SLS_ORG IN ('3300', '330B', '330H')
         AND CDDES_PCK.CODE_TYPE(+) = 'Parent Customer Key'
         AND CDDES_PCK.CODE(+) = ECSD.PRNT_CUST_KEY ---Jan 2023 CWH Ingestion - Added code below to stop CWH here to fetch it later after join with CWH PROD MDS table
         AND UPPER(CDDES_PCK.CODE_DESC) = 'CHEMIST WAREHOUSE'
      ) Parent_Key
   WHERE ctry_key = 'AU'
   AND copa.acct_hier_shrt_desc = 'GTS'
   AND copa.cust_num = Parent_key.cust_num
   and ltrim(copa.matl_num, '0') = ltrim(cwhprod.matl_num, '0')
   AND fisc_yr >= (DATE_PART(YEAR, current_timestamp) -6)
   GROUP BY SAP_PRNT_CUST_KEY,
      SAP_PRNT_CUST_DESC,
      copa.matl_num,
      substring(copa.fisc_yr_per, 1, 4) || substring(copa.fisc_yr_per, 6, 2)
),
lf as
(
   SELECT SAP_PRNT_CUST_KEY,
      SAP_PRNT_CUST_DESC,
      ltrim(copa1.matl_num, '0') matl_num,
      substring(copa1.fisc_yr_per, 1, 4) || substring(copa1.fisc_yr_per, 6, 2) as fisc_month,
      0 as so_qty,
      0 as inv_qty,
      sum(copa1.qty) AS lf_qty,
      sum(copa1.amt_obj_crncy) AS lf_value
   FROM edw_copa_trans_fact copa1,
      (
         SELECT 
            DISTINCT ECBD.CUST_NUM,
            ECSD.PRNT_CUST_KEY AS SAP_PRNT_CUST_KEY,
            CDDES_PCK.CODE_DESC AS SAP_PRNT_CUST_DESC
         FROM EDW_CUSTOMER_SALES_DIM ECSD,
         EDW_CUSTOMER_BASE_DIM ECBD,
         EDW_CODE_DESCRIPTIONS CDDES_PCK
         WHERE ECSD.CUST_NUM = ECBD.CUST_NUM
         AND ECSD.SLS_ORG IN ('3300', '330B', '330H')
         AND CDDES_PCK.CODE_TYPE(+) = 'Parent Customer Key' ---Jan 2023 CWH Ingestion - Added code below to stop CWH here to fetch it later after join with CWH PROD MDS table
         AND UPPER(CDDES_PCK.CODE_DESC) != 'CHEMIST WAREHOUSE'
         AND CDDES_PCK.CODE(+) = ECSD.PRNT_CUST_KEY
      ) Parent_Key
   WHERE ctry_key = 'AU'
   AND copa1.acct_hier_shrt_desc = 'LF'
   AND copa1.cust_num = Parent_key.cust_num
   AND fisc_yr >= (DATE_PART(YEAR, current_timestamp) -6)
   GROUP BY SAP_PRNT_CUST_KEY,
   SAP_PRNT_CUST_DESC,
   copa1.matl_num,
   substring(copa1.fisc_yr_per, 1, 4) || substring(copa1.fisc_yr_per, 6, 2)
   UNION ALL
   SELECT SAP_PRNT_CUST_KEY,
      SAP_PRNT_CUST_DESC,
      ltrim(copa1.matl_num, '0') matl_num,
      substring(copa1.fisc_yr_per, 1, 4) || substring(copa1.fisc_yr_per, 6, 2) as fisc_month,
      0 as so_qty,
      0 as inv_qty,
      sum(copa1.qty) AS lf_qty,
      sum(copa1.amt_obj_crncy) AS lf_value
   FROM edw_copa_trans_fact copa1,
      itg_mds_pacific_prod_mapping_CWH cwhprod,
      (
         SELECT DISTINCT ECBD.CUST_NUM,
               ECSD.PRNT_CUST_KEY AS SAP_PRNT_CUST_KEY,
               CDDES_PCK.CODE_DESC AS SAP_PRNT_CUST_DESC
         FROM EDW_CUSTOMER_SALES_DIM ECSD,
               EDW_CUSTOMER_BASE_DIM ECBD,
               EDW_CODE_DESCRIPTIONS CDDES_PCK
         WHERE ECSD.CUST_NUM = ECBD.CUST_NUM
               AND ECSD.SLS_ORG IN ('3300', '330B', '330H')
               AND CDDES_PCK.CODE_TYPE(+) = 'Parent Customer Key'
               AND UPPER(CDDES_PCK.CODE_DESC) = 'CHEMIST WAREHOUSE'
               AND CDDES_PCK.CODE(+) = ECSD.PRNT_CUST_KEY
      ) Parent_Key
   WHERE ctry_key = 'AU'
      AND copa1.acct_hier_shrt_desc = 'LF'
      AND copa1.cust_num = Parent_key.cust_num
      and ltrim(copa1.matl_num, '0') = ltrim(cwhprod.matl_num, '0')
      AND fisc_yr >= (DATE_PART(YEAR, current_timestamp) -6)
   GROUP BY SAP_PRNT_CUST_KEY,
      SAP_PRNT_CUST_DESC,
      copa1.matl_num,
      substring(copa1.fisc_yr_per, 1, 4) || substring(copa1.fisc_yr_per, 6, 2)
),
final as 
(
    select 
        sap_parent_customer_key::varchar(20) as sap_parent_customer_key,
        sap_parent_customer_desc::varchar(75) as sap_parent_customer_desc,
        month::varchar(21) as month,
        matl_num::varchar(100) as matl_num,
        sum(so_qty)::number(38,4) as so_qty,
        sum(so_value)::number(38,4) as so_value,
        sum(inv_qty)::number(38,5) as inv_qty,
        sum(inv_value)::number(38,4) as inv_value,
        sum(sell_in_qty)::number(38,5) as sell_in_qty,
        sum(sell_in_value)::number(38,5) as sell_in_value,
   from 
    (
        Select sap_parent_customer_key,
            sap_parent_customer_desc,
            month,
            coalesce(nullif(matl_num, ''), 'NA') as matl_num,
            so_qty,
            so_value,
            inv_qty,
            inv_value,
            sell_in_qty,
            sell_in_value
        from
        (
            SELECT 
                sap_parent_customer_key,
                sap_parent_customer_desc,
                matl_num,
                inv_month as month,
                so_qty,
                so_value,
                inv_qty,
                inv_value,
                0 as sell_in_qty,
                0 as sell_in_value
            FROM inv_so
            union all
            select gts.sap_prnt_cust_key,
                upper(gts.sap_prnt_cust_desc) as sap_prnt_cust_desc,
                gts.matl_num as matl_num,
                gts.fisc_month,
                0 as so_qty,
                0 as so_value,
                0 as inv_qty,
                0 as inv_value,
                gts_qty as sell_in_qty,
                gts_value - nvl(lf_value, 0) sell_in_value
            from gts,lf
            where gts.sap_prnt_cust_key = lf.sap_prnt_cust_key (+)
                and gts.matl_num = lf.matl_num (+)
                and gts.fisc_month = lf.fisc_month(+)
        )
      )
   where --(sap_parent_customer_desc in ('COLES','WOOLWORTHS','METCASH','SYMBION','CENTRAL HEALTHCARE SERVICES PTY LTD','API','SIGMA'))
      --Jan 2023 CWH Ingestion including Chemist Warehouse sellout, sellin and inventory from above sources
      (
         sap_parent_customer_desc in (
               'COLES',
               'WOOLWORTHS',
               'METCASH',
               'SYMBION',
               'CENTRAL HEALTHCARE SERVICES PTY LTD',
               'API',
               'SIGMA',
               'CHEMIST WAREHOUSE'
         )
      ) --OR (month >= 202208 and UPPER(sap_parent_customer_desc) = 'CHEMIST WAREHOUSE')
   group by sap_parent_customer_key,
      sap_parent_customer_desc,
      month,
      matl_num
)
select * from final