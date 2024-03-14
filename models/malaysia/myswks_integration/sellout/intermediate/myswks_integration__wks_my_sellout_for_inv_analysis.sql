with edw_vw_my_curr_dim as
(
    select * from {{ ref('mysedw_integration__edw_vw_my_curr_dim') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_vw_my_dstrbtr_customer_dim as
(
    select * from {{ ref('mysedw_integration__edw_vw_my_dstrbtr_customer_dim') }}
),
edw_vw_my_sellout_sales_fact as
(
    select * from {{ ref('mysedw_integration__edw_vw_my_sellout_sales_fact') }}
),
itg_my_gt_outlet_exclusion as
(
    select * from {{ source('mysitg_integration', 'itg_my_gt_outlet_exclusion') }}
),
edw_vw_my_pos_sales_fact as
(
    select * from {{ ref('mysedw_integration__edw_vw_my_pos_sales_fact') }}
),
itg_my_customer_dim as
(
   select * from {{ ref('mysitg_integration__itg_my_customer_dim') }} 
),
edw_vw_my_customer_dim as
(
    select * from {{ ref('mysedw_integration__edw_vw_my_customer_dim') }}
),
itg_my_dstrbtrr_dim as
(
   select * from {{ ref('mysitg_integration__itg_my_dstrbtrr_dim') }}    
),
edw_vw_my_material_dim as
(
    select * from {{ ref('mysedw_integration__edw_vw_my_material_dim') }}
),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
curr_dim as
(
    select
        a.cntry_key,
        a.cntry_nm,
        a.rate_type,
        a.from_ccy,
        a.to_ccy,
        a.valid_date,
        a.jj_year,
        min(cast(a.jj_mnth_id as text)) as start_period,
        max(cast(a.jj_mnth_id as text)) as end_mnth_id,
        a.exch_rate
    from edw_vw_my_curr_dim as a
    group by
        a.cntry_key,
        a.cntry_nm,
        a.rate_type,
        a.from_ccy,
        a.to_ccy,
        a.valid_date,
        a.jj_year,
        a.exch_rate
),
curr_max_period as
(
    select max(cast(a.jj_mnth_id as text)) as max_period from edw_vw_my_curr_dim as a
),
veocurd as
(
    select
        d.cntry_key,
        d.cntry_nm,
        d.rate_type,
        d.from_ccy,
        d.to_ccy,
        d.valid_date,
        d.jj_year,
        d.start_period,
        case
        when d.end_mnth_id = b.max_period
        then cast(cast('209912' as varchar) as text)
        else d.end_mnth_id
        end as end_period,
        d.exch_rate
  from curr_dim d,curr_max_period b
),
veotd as
(
    select distinct
        edw_vw_os_time_dim.cal_year as "year",
        edw_vw_os_time_dim.cal_qrtr_no as qrtr_no,
        edw_vw_os_time_dim.cal_mnth_id as mnth_id,
        edw_vw_os_time_dim.cal_mnth_no as mnth_no,
        edw_vw_os_time_dim.cal_mnth_nm as mnth_nm,
        edw_vw_os_time_dim.cal_date,
        edw_vw_os_time_dim.cal_date_id
    from edw_vw_os_time_dim
),
evodcd as
(
    select
        edw_vw_my_dstrbtr_customer_dim.cntry_cd,
        edw_vw_my_dstrbtr_customer_dim.cntry_nm,
        edw_vw_my_dstrbtr_customer_dim.dstrbtr_grp_cd,
        edw_vw_my_dstrbtr_customer_dim.dstrbtr_soldto_code,
        edw_vw_my_dstrbtr_customer_dim.sap_soldto_code,
        edw_vw_my_dstrbtr_customer_dim.cust_cd,
        edw_vw_my_dstrbtr_customer_dim.cust_nm
    from edw_vw_my_dstrbtr_customer_dim
),
t2 as
(
      select
        cast('Gt Sellout' as varchar) as data_src,
        veotd.mnth_id,
        cast(t1.bill_date as date) as bill_date,
        t1.dstrbtr_grp_cd,
        t1.cust_cd,
        evodcd.sap_soldto_code,
        t1.sap_matl_num,
        (t1.sls_qty_pc) as sls_qty_pc,
        (t1.ret_qty_pc) as ret_qty_pc,
        (t1.jj_grs_trd_sls) as jj_grs_trd_sls,
        (t1.jj_ret_val) as jj_ret_val
      from  veotd, edw_vw_my_sellout_sales_fact as t1
      left join  evodcd
        on ltrim(cast(evodcd.cust_cd as text), cast(cast('0' as varchar) as text)) = ltrim(cast(t1.cust_cd as text), cast(cast('0' as varchar) as text))
        and ltrim(cast(evodcd.dstrbtr_grp_cd as text), cast(cast('0' as varchar) as text)) = ltrim(cast(t1.dstrbtr_grp_cd as text), cast(cast('0' as varchar) as text))
        and ltrim(cast(evodcd.sap_soldto_code as text), cast(cast('0' as varchar) as text)) = ltrim(cast(t1.dstrbtr_soldto_code as text), cast(cast('0' as varchar) as text))
      where
        veotd.cal_date  = t1.bill_date
        and  not (
          coalesce(
            ltrim(cast(t1.dstrbtr_soldto_code as text), cast(cast('0' as varchar) as text)),
            cast(cast('0' as varchar) as text)
          ) || coalesce(trim(cast(t1.cust_cd as text)), cast(cast('0' as varchar) as text))  in (
            select distinct
              cast(coalesce(itg_my_gt_outlet_exclusion.dstrbtr_cd, cast('0' as varchar)) as text) || cast(coalesce(itg_my_gt_outlet_exclusion.outlet_cd, cast('0' as varchar)) as text)
            from itg_my_gt_outlet_exclusion
          )
        )
),
b1 as
(
        select distinct
          edw_vw_os_time_dim."year" || LPAD(edw_vw_os_time_dim.wk::TEXT, 2, '0') as yr_wk,
          edw_vw_os_time_dim.wk,
          edw_vw_os_time_dim.mnth_id,
          edw_vw_os_time_dim."year",
          edw_vw_os_time_dim.qrtr_no,
          edw_vw_os_time_dim.mnth_no
        from edw_vw_os_time_dim
),
b2 as 
(
    SELECT DISTINCT
        edw_vw_os_time_dim.mnth_id,
        edw_vw_os_time_dim."year",
        edw_vw_os_time_dim.qrtr_no,
        edw_vw_os_time_dim.mnth_no
    FROM edw_vw_os_time_dim
),
veosf as
(
  SELECT
    *
  FROM (
    SELECT
      t2.*,
      imdd.lvl3 AS dstrbtr_lvl3
    FROM t2
    LEFT JOIN itg_my_dstrbtrr_dim AS imdd
      ON LTRIM(CAST(imdd.cust_id AS TEXT), CAST(CAST('0' AS VARCHAR) AS TEXT)) 
      = LTRIM(CAST(t2.sap_soldto_code AS TEXT), CAST(CAST('0' AS VARCHAR) AS TEXT))
    UNION ALL
    SELECT
      data_src,
      mnth_id,
      CASE
        WHEN mnth_id IS NULL
        THEN NULL
        ELSE CAST((LEFT(mnth_id, 4) || '-' || SUBSTRING(mnth_id, 5, 6) || '-01') AS DATE)
      END AS bill_date,
      dstrbtr_grp_cd,
      cust_cd,
      sap_soldto_code,
      sap_matl_num,
      jj_qty_pc,
      ret_qty_pc,
      jj_gts,
      jj_ret_val,
      dstrbtr_lvl3
    FROM (
      SELECT
        CAST('SIPOS' AS VARCHAR) AS data_src,
        CASE
          WHEN a.jj_yr_week_no IS NOT  NULL
          OR CAST(a.jj_yr_week_no AS TEXT) <> CAST(CAST('' AS VARCHAR) AS TEXT)
          THEN b1."year"
          ELSE b2."year"
        END AS "year",
        CASE
          WHEN a.jj_yr_week_no IS NOT  NULL
          OR (CAST(a.jj_yr_week_no AS TEXT) <> CAST(CAST('' AS VARCHAR) AS TEXT))
          THEN b1.qrtr_no
          ELSE b2.qrtr_no
        END AS qrtr,
        CASE
          WHEN a.jj_yr_week_no IS NOT  NULL
          OR (CAST(a.jj_yr_week_no AS TEXT) <> CAST(CAST('' AS VARCHAR) AS TEXT))
          THEN CAST(b1.mnth_id AS INT)
          ELSE CAST(b2.mnth_id AS INT)
        END AS mnth_id,
        CASE
          WHEN a.jj_yr_week_no IS NOT  NULL
          OR (CAST(a.jj_yr_week_no AS TEXT) <> CAST(CAST('' AS VARCHAR) AS TEXT))
          THEN b1.mnth_no
          ELSE b2.mnth_no
        END AS mnth_no,
        imcd.dstrbtr_grp_cd,
        a.cust_cd,
        a.cust_cd AS sap_soldto_code,
        a.sap_matl_num,
        a.jj_qty_pc,
        CAST(0 AS DECIMAL(24, 6)) AS ret_qty_pc,
        a.jj_gts,
        CAST(0 AS DECIMAL(38, 12)) AS jj_ret_val,
        NULL AS dstrbtr_lvl3
      FROM edw_vw_my_pos_sales_fact AS a
      LEFT JOIN itg_my_customer_dim AS imcd
        ON LTRIM(CAST(imcd.cust_id AS TEXT), CAST(CAST('0' AS VARCHAR) AS TEXT)) = UPPER(TRIM(SUBSTRING(CAST(a.cust_cd AS TEXT), 0, 7)))
      LEFT JOIN b1
        ON CASE
                WHEN a.jj_yr_week_no IS NOT NULL OR CAST(a.jj_yr_week_no AS TEXT) <> CAST(CAST('' AS VARCHAR) AS TEXT)
                THEN CAST(a.jj_yr_week_no AS TEXT) = b1.yr_wk
            ELSE CAST(NULL AS BOOLEAN)
            END
      LEFT JOIN b2
        ON CASE
                WHEN a.jj_yr_week_no IS NULL OR CAST(a.jj_yr_week_no AS TEXT) = CAST(CAST('' AS VARCHAR) AS TEXT)
                THEN CAST(a.jj_mnth_id AS TEXT) = b2.mnth_id
                ELSE CAST(NULL AS BOOLEAN)
            END
    )t2
  )
  WHERE
    jj_grs_trd_sls > 0
),
veocd as
(
  SELECT
    edw_vw_my_customer_dim.sap_cust_id,
    edw_vw_my_customer_dim.sap_cntry_nm,
    edw_vw_my_customer_dim.sap_prnt_cust_key,
    edw_vw_my_customer_dim.sap_prnt_cust_desc,
    edw_vw_my_customer_dim.sap_cust_chnl_key,
    edw_vw_my_customer_dim.sap_cust_chnl_desc,
    edw_vw_my_customer_dim.sap_cust_sub_chnl_key,
    edw_vw_my_customer_dim.sap_sub_chnl_desc,
    edw_vw_my_customer_dim.sap_go_to_mdl_key,
    edw_vw_my_customer_dim.sap_go_to_mdl_desc,
    edw_vw_my_customer_dim.sap_bnr_key,
    edw_vw_my_customer_dim.sap_bnr_desc,
    edw_vw_my_customer_dim.sap_bnr_frmt_key,
    edw_vw_my_customer_dim.sap_bnr_frmt_desc,
    edw_vw_my_customer_dim.retail_env,
    CASE
        WHEN SAP_PRNT_CUST_KEY = 'PC0004' THEN 'Not Applicable'
        ELSE TRIM(NVL (NULLIF(T2.REGION, ''), 'NA'))
    END AS REGION,
    CASE
        WHEN SAP_PRNT_CUST_KEY = 'PC0004' THEN 'Not Applicable'
        ELSE TRIM(NVL (NULLIF(T2.ZONE_OR_AREA, ''), 'NA'))
    END AS ZONE_OR_AREA
  FROM edw_vw_my_customer_dim, (
    SELECT
      cust_id,
      (
        lvl1 || '-' || region
      ) AS region,
      SUBSTRING(
        REPLACE(REPLACE(lvl3, '(', '- '), ')', ''),
        CASE
          WHEN POSITION('-', REPLACE(REPLACE(lvl3, '(', '- '), ')', '')) + 1 = 1
          THEN 999
          ELSE POSITION('-', REPLACE(REPLACE(lvl3, '(', '- '), ')', '')) + 1
        END
      ) AS ZONE_OR_AREA
    FROM itg_my_dstrbtrr_dim
  ) AS T2
  WHERE LTRIM(SAP_CUST_ID, '0') = T2.CUST_ID(+)
),
veomd as
(
  SELECT DISTINCT edw_vw_my_material_dim.cntry_key,
            edw_vw_my_material_dim.sap_matl_num,
            edw_vw_my_material_dim.sap_mat_desc,
            edw_vw_my_material_dim.gph_prod_frnchse,
            edw_vw_my_material_dim.gph_prod_brnd,
            edw_vw_my_material_dim.gph_prod_sub_brnd,
            edw_vw_my_material_dim.gph_prod_vrnt,
            edw_vw_my_material_dim.gph_prod_ctgry,
            edw_vw_my_material_dim.gph_prod_subctgry,
            edw_vw_my_material_dim.gph_prod_sgmnt,
            edw_vw_my_material_dim.gph_prod_subsgmnt,
            edw_vw_my_material_dim.gph_prod_put_up_desc,
            EMD.pka_product_key AS pka_product_key,
            EMD.pka_product_key_description AS pka_product_key_description,
            EMD.pka_product_key AS product_key,
            EMD.pka_product_key_description AS product_key_description,
            EMD.pka_size_desc AS pka_size_desc
        FROM edw_vw_my_material_dim
            LEFT JOIN (
                SELECT * 
                FROM edw_material_dim
            ) EMD ON edw_vw_my_material_dim.sap_matl_num = LTRIM(EMD.MATL_NUM, '0')
),        
final as
(
    SELECT
        'Malaysia'::varchar(8) as cntry_nm,
        TRIM(COALESCE(NULLIF(veosf.dstrbtr_grp_cd, ''), 'NA'))::varchar(20) as dstrbtr_grp_cd,
        TRIM(COALESCE(NULLIF(veosf.dstrbtr_lvl3, ''), 'NA'))::varchar(40) as dstrbtr_lvl3,
        TRIM(COALESCE(NULLIF(LTRIM(veocd.sap_prnt_cust_key), ''), 'NA'))::varchar(12) as sap_prnt_cust_key,
        TRIM(COALESCE(NULLIF(veomd.pka_size_desc, ''), 'NA'))::varchar(30) as pka_size_desc,
        TRIM(COALESCE(NULLIF(veomd.gph_prod_brnd, ''), 'NA'))::varchar(30) as global_prod_brand,
        TRIM(COALESCE(NULLIF(veomd.gph_prod_vrnt, ''), 'NA'))::varchar(100) as global_prod_variant,
        TRIM(COALESCE(NULLIF(veomd.gph_prod_ctgry, ''), 'NA'))::varchar(50) as global_prod_category,
        TRIM(COALESCE(NULLIF(veomd.gph_prod_sgmnt, ''), 'NA'))::varchar(50) as global_prod_segment,
        TRIM(COALESCE(NULLIF(veomd.pka_product_key, ''), 'NA'))::varchar(68) as pka_product_key,
        veocurd.to_ccy::varchar(5) as to_ccy,
        MIN(veosf.bill_date)::date as min_date
    FROM veocurd, veosf
        LEFT JOIN veocd
        ON LTRIM(CAST(veocd.sap_cust_id AS TEXT), CAST(CAST('0' AS VARCHAR) AS TEXT)) = LTRIM(CAST(veosf.dstrbtr_grp_cd AS TEXT), CAST(CAST('0' AS VARCHAR) AS TEXT))
        LEFT JOIN  veomd
        ON LTRIM(CAST(veomd.sap_matl_num AS TEXT), CAST(CAST('0' AS VARCHAR) AS TEXT)) = LTRIM(CAST(veosf.sap_matl_num AS TEXT), CAST(CAST('0' AS VARCHAR) AS TEXT))
        LEFT JOIN itg_my_dstrbtrr_dim AS imdd
        ON LTRIM(CAST(imdd.cust_id AS TEXT), CAST(CAST('0' AS VARCHAR) AS TEXT)) = LTRIM(CAST(veosf.sap_soldto_code AS TEXT), CAST(CAST('0' AS VARCHAR) AS TEXT))
        WHERE
        CAST(CAST(veosf.mnth_id AS VARCHAR) AS TEXT) >= veocurd.start_period
        AND CAST(CAST(veosf.mnth_id AS VARCHAR) AS TEXT) <= veocurd.end_period
        AND veocurd.to_ccy = 'MYR'
        AND LEFT(veosf.bill_date, 4) > (DATE_PART(YEAR, current_timestamp()) - 6)
    GROUP BY
    veosf.dstrbtr_grp_cd,
    veosf.dstrbtr_lvl3,
    veocd.sap_prnt_cust_key,
    veocurd.to_ccy,
    veomd.gph_prod_frnchse,
    veomd.pka_size_desc,
    veomd.gph_prod_brnd,
    veomd.gph_prod_vrnt,
    veomd.gph_prod_ctgry,
    veomd.gph_prod_sgmnt,
    veomd.pka_product_key
)
select * from final