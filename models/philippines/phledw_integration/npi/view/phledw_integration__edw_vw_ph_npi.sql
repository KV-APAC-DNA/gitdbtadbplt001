with edw_ph_sellin_analysis as  (
select * from {{ ref('phledw_integration__edw_ph_sellin_analysis') }}
),
edw_vw_ph_material_dim as (
select * from {{ ref('phledw_integration__edw_vw_ph_material_dim') }}
),
itg_mds_ph_npi_peg_item as (
select * from {{ ref('phlitg_integration__itg_mds_ph_npi_peg_item') }}
),
itg_mds_ph_npi_sales_groupings as (
select * from {{ ref('phlitg_integration__itg_mds_ph_npi_sales_groupings') }}
),
itg_mds_ph_targets_by_accounts_and_skus as (
select * from {{ ref('phlitg_integration__itg_mds_ph_targets_by_accounts_and_skus') }}
),
itg_mds_ph_targets_by_national_and_skus as (
select * from {{ ref('phlitg_integration__itg_mds_ph_targets_by_national_and_skus') }}
),
edw_calendar_dim as (
select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
ref_date as (
(SELECT DISTINCT TO_DATE(left(edw_calendar_dim.fisc_per,4)||right(edw_calendar_dim.fisc_per,2)||'01','YYYYMMDD') AS fisc_date,
                      dateadd(month ,1,TO_DATE(left(edw_calendar_dim.fisc_per,4)||right(edw_calendar_dim.fisc_per,2)||'01','YYYYMMDD')::TIMESTAMP WITHOUT TIME ZONE) AS m_1_fisc_date,
                      dateadd(month,2,TO_DATE(left(edw_calendar_dim.fisc_per,4)||right(edw_calendar_dim.fisc_per,2)||'01','YYYYMMDD')::TIMESTAMP WITHOUT TIME ZONE) AS m_2_fisc_date,
                      dateadd(month,- 2,TO_DATE(left(edw_calendar_dim.fisc_per,4)||right(edw_calendar_dim.fisc_per,2)||'01','YYYYMMDD')::TIMESTAMP WITHOUT TIME ZONE) AS m_p2_fisc_date
               FROM edw_calendar_dim)
) ,
pipeline_1 as (
       (SELECT 'Pipeline'::CHARACTER VARYING AS subsource_type,
              'M1'::CHARACTER VARYING AS pipeline,
              s.jj_mnth_id,
              s.sku,
              s.sku_desc,
              s.chnl_desc,
              s.sub_chnl_desc,
              s.sls_grp_desc,
              s.parent_customer_cd,
              s.parent_customer,
              s.sap_sls_office_desc,
              s.sold_to,
              s.sold_to_nm,
              s.account_name,
              s.sls_grping,
              s.trade_type,
              NULL::CHARACTER VARYING AS peg_itemcode,
              NULL::CHARACTER VARYING AS peg_itemdesc,
              epne.salescycle,
              s.nts_val
       FROM (SELECT itg_mds_ph_npi_peg_item.npi_item_cd AS npi_itemcode,
                    itg_mds_ph_npi_peg_item.peg_item_cd,
                    itg_mds_ph_npi_peg_item.peg_item_desc,
                    itg_mds_ph_npi_peg_item.sales_cycle AS salescycle
             FROM itg_mds_ph_npi_peg_item) epne
         JOIN  ref_date ON TO_DATE (epne.salescycle::TEXT,'YYYYMM'::CHARACTER VARYING::TEXT) = ref_date.fisc_date
         LEFT JOIN (SELECT 'Sellin'::CHARACTER VARYING AS subsource_type,
                           a.jj_mnth_id,
                           a.sku,
                           a.sku_desc,
                           a.chnl_desc,
                           a.sub_chnl_desc,
                           a.sls_grp_desc,
                           a.parent_customer_cd,
                           a.parent_customer,
                           a.sap_sls_office_desc,
                           a.sold_to,
                           a.sold_to_nm,
                           cd.account_name,
                           cd.sls_grping,
                           cd.trade_type,
                           SUM(a.nts_val) AS nts_val
                    FROM edw_ph_sellin_analysis a
                      JOIN (SELECT itg_mds_ph_npi_peg_item.npi_item_cd,
                                   itg_mds_ph_npi_peg_item.peg_item_cd,
                                   itg_mds_ph_npi_peg_item.peg_item_desc,
                                   itg_mds_ph_npi_peg_item.sales_cycle
                            FROM itg_mds_ph_npi_peg_item) npi ON a.sku::TEXT = npi.npi_item_cd::TEXT
                        LEFT JOIN itg_mds_ph_npi_sales_groupings cd ON a.sold_to::TEXT = cd.cust_cd::TEXT
                    GROUP BY 1,
                             a.jj_mnth_id,
                             a.sku,
                             a.sku_desc,
                             a.chnl_desc,
                             a.sub_chnl_desc,
                             a.sls_grp_desc,
                             a.parent_customer_cd,
                             a.parent_customer,
                             a.sap_sls_office_desc,
                             a.sold_to,
                             a.sold_to_nm,
                             cd.account_name,
                             cd.sls_grping,
                             cd.trade_type) s
                ON s.sku::TEXT = epne.npi_itemcode::TEXT
               AND ref_date.fisc_date = TO_DATE (s.jj_mnth_id::TEXT,'YYYYMM'::CHARACTER VARYING::TEXT)
       WHERE s.sku IS NOT NULL
       )) ,
pipeline_2 as (
SELECT 'Pipeline'::CHARACTER VARYING AS subsource_type,
              'M2'::CHARACTER VARYING AS pipeline,
              s.jj_mnth_id,
              s.sku,
              s.sku_desc,
              s.chnl_desc,
              s.sub_chnl_desc,
              s.sls_grp_desc,
              s.parent_customer_cd,
              s.parent_customer,
              s.sap_sls_office_desc,
              s.sold_to,
              s.sold_to_nm,
              s.account_name,
              s.sls_grping,
              s.trade_type,
              NULL::CHARACTER VARYING AS peg_itemcode,
              NULL::CHARACTER VARYING AS peg_itemdesc,
              epne.salescycle,
              s.nts_val
       FROM (SELECT itg_mds_ph_npi_peg_item.npi_item_cd AS npi_itemcode,
                    itg_mds_ph_npi_peg_item.peg_item_cd,
                    itg_mds_ph_npi_peg_item.peg_item_desc,
                    itg_mds_ph_npi_peg_item.sales_cycle AS salescycle
             FROM itg_mds_ph_npi_peg_item) epne
         JOIN  ref_date ON TO_DATE (epne.salescycle::TEXT,'YYYYMM'::CHARACTER VARYING::TEXT) = ref_date.fisc_date
         LEFT JOIN (SELECT 'Sellin'::CHARACTER VARYING AS subsource_type,
                           a.jj_mnth_id,
                           a.sku,
                           a.sku_desc,
                           a.chnl_desc,
                           a.sub_chnl_desc,
                           a.sls_grp_desc,
                           a.parent_customer_cd,
                           a.parent_customer,
                           a.sap_sls_office_desc,
                           a.sold_to,
                           a.sold_to_nm,
                           cd.account_name,
                           cd.sls_grping,
                           cd.trade_type,
                           SUM(a.nts_val) AS nts_val
                    FROM edw_ph_sellin_analysis a
                      JOIN (SELECT itg_mds_ph_npi_peg_item.npi_item_cd,
                                   itg_mds_ph_npi_peg_item.peg_item_cd,
                                   itg_mds_ph_npi_peg_item.peg_item_desc,
                                   itg_mds_ph_npi_peg_item.sales_cycle
                            FROM itg_mds_ph_npi_peg_item) npi ON a.sku::TEXT = npi.npi_item_cd::TEXT
                        LEFT JOIN itg_mds_ph_npi_sales_groupings cd ON a.sold_to::TEXT = cd.cust_cd::TEXT
                    GROUP BY 1,
                             a.jj_mnth_id,
                             a.sku,
                             a.sku_desc,
                             a.chnl_desc,
                             a.sub_chnl_desc,
                             a.sls_grp_desc,
                             a.parent_customer_cd,
                             a.parent_customer,
                             a.sap_sls_office_desc,
                             a.sold_to,
                             a.sold_to_nm,
                             cd.account_name,
                             cd.sls_grping,
                             cd.trade_type) s
                ON s.sku::TEXT = epne.npi_itemcode::TEXT
               AND ref_date.m_1_fisc_date = TO_DATE (s.jj_mnth_id::TEXT,'YYYYMM'::CHARACTER VARYING::TEXT)::TIMESTAMP WITHOUT TIME ZONE
       WHERE s.sku IS NOT NULL
) ,
pipeline_3 as (
 SELECT 'Pipeline'::CHARACTER VARYING AS subsource_type,
              'M3'::CHARACTER VARYING AS pipeline,
              s.jj_mnth_id,
              s.sku,
              s.sku_desc,
              s.chnl_desc,
              s.sub_chnl_desc,
              s.sls_grp_desc,
              s.parent_customer_cd,
              s.parent_customer,
              s.sap_sls_office_desc,
              s.sold_to,
              s.sold_to_nm,
              s.account_name,
              s.sls_grping,
              s.trade_type,
              NULL::CHARACTER VARYING AS peg_itemcode,
              NULL::CHARACTER VARYING AS peg_itemdesc,
              epne.salescycle,
              s.nts_val
       FROM (SELECT itg_mds_ph_npi_peg_item.npi_item_cd AS npi_itemcode,
                    itg_mds_ph_npi_peg_item.peg_item_cd,
                    itg_mds_ph_npi_peg_item.peg_item_desc,
                    itg_mds_ph_npi_peg_item.sales_cycle AS salescycle
             FROM itg_mds_ph_npi_peg_item) epne
         JOIN  ref_date ON TO_DATE (epne.salescycle::TEXT,'YYYYMM'::CHARACTER VARYING::TEXT) = ref_date.fisc_date
         LEFT JOIN (SELECT 'Sellin'::CHARACTER VARYING AS subsource_type,
                           a.jj_mnth_id,
                           a.sku,
                           a.sku_desc,
                           a.chnl_desc,
                           a.sub_chnl_desc,
                           a.sls_grp_desc,
                           a.parent_customer_cd,
                           a.parent_customer,
                           a.sap_sls_office_desc,
                           a.sold_to,
                           a.sold_to_nm,
                           cd.account_name,
                           cd.sls_grping,
                           cd.trade_type,
                           SUM(a.nts_val) AS nts_val
                    FROM edw_ph_sellin_analysis a
                      JOIN (SELECT itg_mds_ph_npi_peg_item.npi_item_cd,
                                   itg_mds_ph_npi_peg_item.peg_item_cd,
                                   itg_mds_ph_npi_peg_item.peg_item_desc,
                                   itg_mds_ph_npi_peg_item.sales_cycle
                            FROM itg_mds_ph_npi_peg_item) npi ON a.sku::TEXT = npi.npi_item_cd::TEXT
                       LEFT JOIN itg_mds_ph_npi_sales_groupings cd ON a.sold_to::TEXT = cd.cust_cd::TEXT
                    GROUP BY 1,
                             a.jj_mnth_id,
                             a.sku,
                             a.sku_desc,
                             a.chnl_desc,
                             a.sub_chnl_desc,
                             a.sls_grp_desc,
                             a.parent_customer_cd,
                             a.parent_customer,
                             a.sap_sls_office_desc,
                             a.sold_to,
                             a.sold_to_nm,
                             cd.account_name,
                             cd.sls_grping,
                             cd.trade_type) s
                ON s.sku::TEXT = epne.npi_itemcode::TEXT
               AND ref_date.m_2_fisc_date = TO_DATE (s.jj_mnth_id::TEXT,'YYYYMM'::CHARACTER VARYING::TEXT)::TIMESTAMP WITHOUT TIME ZONE
       WHERE s.sku IS NOT NULL
),
pipeline as (
SELECT derived_table1.subsource_type,
       derived_table1.pipeline,
       derived_table1.jj_mnth_id,
       derived_table1.sku,
       derived_table1.sku_desc,
       derived_table1.chnl_desc,
       derived_table1.sub_chnl_desc,
       derived_table1.sls_grp_desc,
       derived_table1.parent_customer_cd,
       derived_table1.parent_customer,
       derived_table1.sap_sls_office_desc,
       derived_table1.sold_to,
       derived_table1.sold_to_nm,
       null AS dstrbtr_cust_cd,
       derived_table1.account_name,
       derived_table1.sls_grping,
       derived_table1.trade_type,
       null AS peg_itemcode,
       null AS peg_itemdesc,
       derived_table1.salescycle AS sales_cycle,
       derived_table1.nts_val
FROM ( select * from pipeline_1
       UNION ALL
       select * from pipeline_2
       union all
       select * from pipeline_3
      ) derived_table1),
sellin as (
SELECT 'Sellin'::CHARACTER VARYING AS subsource_type,
       NULL::CHARACTER VARYING AS pipeline,
       a.jj_mnth_id,
       a.sku,
       a.sku_desc,
       a.chnl_desc,
       a.sub_chnl_desc,
       a.sls_grp_desc,
       a.parent_customer_cd,
       a.parent_customer,
       a.sap_sls_office_desc,
       a.sold_to,
       a.sold_to_nm,
       NULL::CHARACTER VARYING AS dstrbtr_cust_cd,
       cd.account_name,
       cd.sls_grping,
       cd.trade_type,
       NULL::CHARACTER VARYING AS peg_itemcode,
       NULL::CHARACTER VARYING AS peg_itemdesc,
       npi.sales_cycle,
       SUM(a.nts_val) AS nts_val
FROM edw_ph_sellin_analysis a
  LEFT JOIN  itg_mds_ph_npi_sales_groupings cd ON a.sold_to::TEXT = cd.cust_cd::TEXT
  JOIN (SELECT itg_mds_ph_npi_peg_item.npi_item_cd,
               itg_mds_ph_npi_peg_item.peg_item_cd,
               itg_mds_ph_npi_peg_item.peg_item_desc,
               itg_mds_ph_npi_peg_item.sales_cycle
        FROM itg_mds_ph_npi_peg_item) npi ON a.sku::TEXT = npi.npi_item_cd::TEXT
GROUP BY 1,
         2,
         a.jj_mnth_id,
         a.sku,
         a.sku_desc,
         a.chnl_desc,
         a.sub_chnl_desc,
         a.sls_grp_desc,
         a.parent_customer_cd,
         a.parent_customer,
         a.sap_sls_office_desc,
         a.sold_to,
         a.sold_to_nm,
         14,
         cd.account_name,
         cd.sls_grping,
         cd.trade_type,
         18,
         19,
         npi.sales_cycle
) ,
nt as (
SELECT 'NT'::CHARACTER VARYING AS subsource_type,
       null AS pipeline,
       f.year::CHARACTER VARYING ||f.month::CHARACTER VARYING AS jj_mnth_id,
       f.item_cd AS sku,
       m.sap_mat_desc AS sku_desc,
       null AS chnl_desc,
       null AS sub_chnl_desc,
       null AS sls_grp_desc,
       null AS parent_customer_cd,
       null AS parent_customer,
       null AS sap_sls_office_desc,
       null AS sold_to,
       null AS sold_to_nm,
       null AS dstrbtr_cust_cd,
       null AS account_name,
       null AS sls_grping,
       null AS trade_type  ,
       null AS peg_itemcode,
       null AS peg_itemdesc,
       null AS sales_cycle,
       f.target_value AS nts_val
FROM itg_mds_ph_targets_by_national_and_skus f
  LEFT JOIN (SELECT *                                   
             FROM edw_vw_os_material_dim
             WHERE edw_vw_os_material_dim.cntry_key::TEXT = 'PH'::CHARACTER VARYING::TEXT) m ON 
             UPPER (LTRIM (m.sap_matl_num::TEXT,0::CHARACTER VARYING::TEXT)) = UPPER (LTRIM (f.item_cd::TEXT,0::CHARACTER VARYING::TEXT))
  JOIN (SELECT itg_mds_ph_npi_peg_item.npi_item_cd,
               itg_mds_ph_npi_peg_item.peg_item_cd,
               itg_mds_ph_npi_peg_item.peg_item_desc,
               itg_mds_ph_npi_peg_item.sales_cycle
        FROM itg_mds_ph_npi_peg_item) npi ON f.item_cd::TEXT = npi.npi_item_cd::TEXT
) ,
forecast as (
SELECT 'Forecast'::CHARACTER VARYING AS subsource_type,
       NULL::CHARACTER VARYING AS pipeline,
       a.yearmo::CHARACTER VARYING (100) AS jj_mnth_id,
       a.item_cd AS sku,
       m.sap_mat_desc AS sku_desc,
       NULL::CHARACTER VARYING AS chnl_desc,
       NULL::CHARACTER VARYING AS sub_chnl_desc,
       NULL::CHARACTER VARYING AS sls_grp_desc,
       NULL::CHARACTER AS parent_customer_cd,
       NULL::CHARACTER AS parent_customer,
       NULL::CHARACTER VARYING AS sap_sls_office_desc,
       NULL::CHARACTER VARYING AS sold_to,
       NULL::CHARACTER VARYING AS sold_to_nm,
       NULL::CHARACTER VARYING AS dstrbtr_cust_cd,
       a.account_nm account_name,
       a.sls_grping,
       a.trade_type,
       NULL::CHARACTER VARYING AS peg_itemcode,
       NULL::CHARACTER VARYING AS peg_itemdesc,
       npi.sales_cycle,
       SUM(a.amount) AS nts_val
FROM (SELECT a.item_cd,
             a.year :: character varying||month :: character varying yearmo,
             a.target_value amount,
             a.account_nm,
             b.sls_grping,
             b.trade_type             
      FROM itg_mds_ph_targets_by_accounts_and_skus a
        LEFT JOIN (select distinct account_name,sls_grping,trade_type from itg_mds_ph_npi_sales_groupings) b 
        ON a.account_nm::TEXT = b.account_name::TEXT) a
  LEFT JOIN (SELECT *
             FROM edw_vw_os_material_dim
             WHERE edw_vw_os_material_dim.cntry_key::TEXT = 'PH'::CHARACTER VARYING::TEXT) m ON UPPER (LTRIM (m.sap_matl_num ::TEXT,0::CHARACTER VARYING::TEXT)) = UPPER (LTRIM (a.item_cd::TEXT,0::CHARACTER VARYING::TEXT))
  JOIN (SELECT itg_mds_ph_npi_peg_item.npi_item_cd,
               itg_mds_ph_npi_peg_item.peg_item_cd,
               itg_mds_ph_npi_peg_item.peg_item_desc,
               itg_mds_ph_npi_peg_item.sales_cycle
        FROM itg_mds_ph_npi_peg_item) npi ON a.item_cd::TEXT = npi.npi_item_cd::TEXT
WHERE a.item_cd IS NOT NULL
GROUP BY 1,
         2,
         a.yearmo::CHARACTER VARYING (100),
         a.item_cd,
         m.sap_mat_desc,
         6,
         7,
         8,
         9,
         10,
         11,
         12,
         13,
         14,
         a.account_nm,
         a.sls_grping,
         a.trade_type,
         18,
         19,
         npi.sales_cycle
HAVING SUM(a.amount) > 0.0
),
final as (
select * from sellin

UNION ALL

--National Target

select * from nt
UNION ALL

--Pipeline

select * from pipeline
       
UNION ALL

-- Forecast
select * from forecast
)
select * from final