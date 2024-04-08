with edw_vw_th_sellin_sales_fact as(
  select * from {{ ref('thaedw_integration__edw_vw_th_sellin_sales_fact') }}
), 
itg_th_ciw_account_lookup as(
  select * from {{ ref('thaitg_integration__itg_th_ciw_account_lookup') }}
), 
edw_customer_sales_dim as(
  select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
), 
edw_vw_os_time_dim as(
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
), 
edw_vw_th_customer_dim as(
  select * from {{ ref('thaedw_integration__edw_vw_th_customer_dim') }}
), 
edw_company_dim as(
  select * from {{ ref('aspedw_integration__edw_company_dim') }}
), 
edw_vw_th_material_dim as(
  select * from {{ ref('thaedw_integration__edw_vw_th_material_dim') }}
), 
edw_vw_th_dstrbtr_material_dim as(
  select * from {{ ref('thaedw_integration__edw_vw_th_dstrbtr_material_dim') }}
), 
sellin_fact as(
    SELECT 
        coalesce(trim(edw_vw_th_sellin_sales_fact.item_cd),'') as item_cd,
        coalesce(trim(edw_vw_th_sellin_sales_fact.cust_id),'') as cust_id,
        coalesce(trim(edw_vw_th_sellin_sales_fact.sls_org),'') as sls_org,
        coalesce(trim(edw_vw_th_sellin_sales_fact.sls_grp),'') AS sap_sls_grp_cd,
        coalesce(trim(sls_grp_lkp.sls_grp_desc),'') AS sap_sls_grp_desc,
        coalesce(trim(edw_vw_th_sellin_sales_fact.sls_ofc),'') AS sap_sls_office_cd,
        coalesce(trim(sls_ofc_lkp.sls_ofc_desc),'') AS sap_sls_office_desc,
        coalesce(trim(edw_vw_th_sellin_sales_fact.plnt),'') as plnt,
        coalesce(trim(edw_vw_th_sellin_sales_fact.acct_no),'') as acct_no,
        coalesce(trim(edw_vw_th_sellin_sales_fact.cust_grp),'') as cust_grp,
        coalesce(trim(edw_vw_th_sellin_sales_fact.cust_sls),'') as cust_sls,
        coalesce(trim(edw_vw_th_sellin_sales_fact.pstng_per),'') as pstng_per,
        coalesce(trim(edw_vw_th_sellin_sales_fact.dstr_chnl),'') as dstr_chnl,
        coalesce(trim(edw_vw_th_sellin_sales_fact.jj_mnth_id),'') as jj_mnth_id,
        coalesce(trim(itg_th_ciw_account_lookup.area),'') as area,
        coalesce(trim(itg_th_ciw_account_lookup.category),'') as category,
        coalesce(trim(itg_th_ciw_account_lookup.account_name),'') as account_name,
        "max" (
            (
            edw_vw_th_sellin_sales_fact.pstng_dt
            ):: TEXT
        ) AS max_pstng_dt, 
        sum(
            edw_vw_th_sellin_sales_fact.base_val
        ) AS base_val, 
        sum(
            edw_vw_th_sellin_sales_fact.sls_qty
        ) AS sls_qty, 
        sum(
            edw_vw_th_sellin_sales_fact.ret_qty
        ) AS ret_qty, 
        sum(
            edw_vw_th_sellin_sales_fact.sls_less_rtn_qty
        ) AS sls_less_rtn_qty, 
        sum(
            edw_vw_th_sellin_sales_fact.gts_val
        ) AS gts_val, 
        sum(
            edw_vw_th_sellin_sales_fact.ret_val
        ) AS ret_val, 
        sum(
            edw_vw_th_sellin_sales_fact.gts_less_rtn_val
        ) AS gts_less_rtn_val, 
        sum(
            edw_vw_th_sellin_sales_fact.tp_val
        ) AS tp_val, 
        sum(
            edw_vw_th_sellin_sales_fact.nts_val
        ) AS nts_val, 
        sum(
            edw_vw_th_sellin_sales_fact.nts_qty
        ) AS nts_qty, 
        (
            sum(
            edw_vw_th_sellin_sales_fact.base_val
            ) - sum(
            edw_vw_th_sellin_sales_fact.nts_val
            )
        ) AS ciw_account_value 
    FROM 
    (
        (
        (
            edw_vw_th_sellin_sales_fact 
            LEFT JOIN itg_th_ciw_account_lookup ON (
            (
                (
                edw_vw_th_sellin_sales_fact.acct_no
                ) = (
                itg_th_ciw_account_lookup.account_num
                )
            )
            )
        ) 
        LEFT JOIN (
            SELECT 
            DISTINCT edw_customer_sales_dim.sls_ofc AS sap_sls_office_cd, 
            edw_customer_sales_dim.sls_ofc_desc 
            FROM 
            edw_customer_sales_dim 
            WHERE 
            (
                (
                (
                    (edw_customer_sales_dim.sls_org):: TEXT = ('2400' :: varchar):: TEXT
                ) 
                OR (
                    (edw_customer_sales_dim.sls_org):: TEXT = ('2500' :: varchar):: TEXT
                )
                ) 
                AND (
                CASE WHEN (
                    (edw_customer_sales_dim.sls_ofc):: TEXT = ('' :: varchar):: TEXT
                ) THEN NULL :: varchar ELSE edw_customer_sales_dim.sls_ofc END IS NOT NULL
                )
            )
        ) sls_ofc_lkp ON (
            (
            (sls_ofc_lkp.sap_sls_office_cd):: TEXT = (
                edw_vw_th_sellin_sales_fact.sls_ofc
            ):: TEXT
            )
        )
        ) 
        LEFT JOIN (
        SELECT 
            DISTINCT edw_customer_sales_dim.sls_grp, 
            edw_customer_sales_dim.sls_grp_desc 
        FROM 
            edw_customer_sales_dim 
        WHERE 
            (
            (
                (
                (edw_customer_sales_dim.sls_org)= ('2400' :: varchar)
                ) 
                OR (
                (edw_customer_sales_dim.sls_org) = ('2500' :: varchar)
                )
            ) 
            AND (
                CASE WHEN (
                (edw_customer_sales_dim.sls_grp) = ('' :: varchar)
                ) THEN NULL :: varchar ELSE edw_customer_sales_dim.sls_grp END IS NOT NULL
            )
            )
        ) sls_grp_lkp ON (
        (
            (sls_grp_lkp.sls_grp) = (
            edw_vw_th_sellin_sales_fact.sls_grp
            )
        )
        )
    ) 
    WHERE 
    (
        (
        edw_vw_th_sellin_sales_fact.cntry_nm
        ) = ('TH' :: varchar)
    ) 
    GROUP BY 
    coalesce(trim(edw_vw_th_sellin_sales_fact.item_cd),''),
    coalesce(trim(edw_vw_th_sellin_sales_fact.cust_id),''),
    coalesce(trim(edw_vw_th_sellin_sales_fact.sls_org),''),
    coalesce(trim(edw_vw_th_sellin_sales_fact.sls_grp),''),
    coalesce(trim(sls_grp_lkp.sls_grp_desc),''),
    coalesce(trim(edw_vw_th_sellin_sales_fact.sls_ofc),''),
    coalesce(trim(sls_ofc_lkp.sls_ofc_desc),''),
    coalesce(trim(edw_vw_th_sellin_sales_fact.plnt),''),
    coalesce(trim(edw_vw_th_sellin_sales_fact.acct_no),''),
    coalesce(trim(edw_vw_th_sellin_sales_fact.cust_grp),''),
    coalesce(trim(edw_vw_th_sellin_sales_fact.cust_sls),''),
    coalesce(trim(edw_vw_th_sellin_sales_fact.pstng_per),''),
    coalesce(trim(edw_vw_th_sellin_sales_fact.dstr_chnl),''),
    coalesce(trim(edw_vw_th_sellin_sales_fact.jj_mnth_id),''),
    coalesce(trim(itg_th_ciw_account_lookup.area),''),
    coalesce(trim(itg_th_ciw_account_lookup.category),''),
    coalesce(trim(itg_th_ciw_account_lookup.account_name),'')
), 
time as(
  SELECT 
    DISTINCT edw_vw_os_time_dim."year", 
    edw_vw_os_time_dim.qrtr, 
    edw_vw_os_time_dim.mnth_id, 
    edw_vw_os_time_dim.mnth_no 
  FROM 
    edw_vw_os_time_dim 
  WHERE 
    (
      edw_vw_os_time_dim."year" > YEAR(
        CURRENT_TIMESTAMP()
      ) -3
    )
), 
cust as(
  SELECT 
    sellin_cust.sap_cust_id, 
    sellin_cust.sap_cust_nm, 
    sellin_cust.sap_sls_org, 
    sellin_cust.sap_cmp_id, 
    sellin_cust.sap_cntry_cd, 
    sellin_cust.sap_cntry_nm, 
    sellin_cust.sap_addr, 
    sellin_cust.sap_region, 
    sellin_cust.sap_state_cd, 
    sellin_cust.sap_city, 
    sellin_cust.sap_post_cd, 
    sellin_cust.sap_chnl_cd, 
    sellin_cust.sap_chnl_desc, 
    sellin_cust.sap_sls_office_cd, 
    sellin_cust.sap_sls_office_desc, 
    sellin_cust.sap_sls_grp_cd, 
    sellin_cust.sap_sls_grp_desc, 
    sellin_cust.sap_prnt_cust_key, 
    sellin_cust.sap_prnt_cust_desc, 
    sellin_cust.sap_cust_chnl_key, 
    sellin_cust.sap_cust_chnl_desc, 
    sellin_cust.sap_cust_sub_chnl_key, 
    sellin_cust.sap_sub_chnl_desc, 
    sellin_cust.sap_go_to_mdl_key, 
    sellin_cust.sap_go_to_mdl_desc AS go_to_model_description, 
    sellin_cust.sap_bnr_key, 
    sellin_cust.sap_bnr_desc AS banner_description, 
    sellin_cust.sap_bnr_frmt_key, 
    sellin_cust.sap_bnr_frmt_desc, 
    sellin_cust.retail_env 
  FROM 
    edw_vw_th_customer_dim sellin_cust 
), 
cmp as(
  SELECT 
    edw_company_dim.co_cd, 
    edw_company_dim.company_nm 
  FROM 
    edw_company_dim 
  WHERE 
    (
      (edw_company_dim.ctry_key) = ('TH' :: varchar)
    )
), 
sellin_mat as(
  SELECT 
    DISTINCT edw_vw_th_material_dim.sap_matl_num, 
    edw_vw_th_material_dim.sap_mat_desc, 
    edw_vw_th_material_dim.gph_region, 
    edw_vw_th_material_dim.gph_prod_frnchse, 
    edw_vw_th_material_dim.gph_prod_brnd, 
    edw_vw_th_material_dim.gph_prod_vrnt, 
    edw_vw_th_material_dim.gph_prod_sgmnt, 
    edw_vw_th_material_dim.gph_prod_put_up_desc, 
    edw_vw_th_material_dim.gph_prod_sub_brnd AS prod_sub_brand, 
    edw_vw_th_material_dim.gph_prod_subsgmnt AS prod_subsegment, 
    edw_vw_th_material_dim.gph_prod_ctgry AS prod_category, 
    edw_vw_th_material_dim.gph_prod_subctgry AS prod_subcategory 
  FROM 
    edw_vw_th_material_dim 
), 
sellout_mat as(
  SELECT 
    DISTINCT edw_vw_th_dstrbtr_material_dim.dstrbtr_matl_num, 
    edw_vw_th_dstrbtr_material_dim.is_npi, 
    edw_vw_th_dstrbtr_material_dim.npi_str_period, 
    edw_vw_th_dstrbtr_material_dim.npi_end_period, 
    edw_vw_th_dstrbtr_material_dim.is_reg, 
    edw_vw_th_dstrbtr_material_dim.is_promo, 
    edw_vw_th_dstrbtr_material_dim.promo_strt_period, 
    edw_vw_th_dstrbtr_material_dim.promo_end_period, 
    edw_vw_th_dstrbtr_material_dim.is_mcl, 
    edw_vw_th_dstrbtr_material_dim.is_hero 
  FROM 
    edw_vw_th_dstrbtr_material_dim 
  
), 
mat as(
  SELECT 
    sellin_mat.sap_matl_num, 
    sellin_mat.sap_mat_desc, 
    sellin_mat.gph_region, 
    sellin_mat.gph_prod_frnchse, 
    sellin_mat.gph_prod_brnd, 
    sellin_mat.gph_prod_vrnt, 
    sellin_mat.gph_prod_sgmnt, 
    sellin_mat.gph_prod_put_up_desc, 
    sellin_mat.prod_sub_brand, 
    sellin_mat.prod_subsegment, 
    sellin_mat.prod_category, 
    sellin_mat.prod_subcategory, 
    sellout_mat.is_npi, 
    sellout_mat.npi_str_period, 
    sellout_mat.npi_end_period, 
    sellout_mat.is_reg, 
    sellout_mat.is_promo, 
    sellout_mat.promo_strt_period, 
    sellout_mat.promo_end_period, 
    sellout_mat.is_mcl, 
    sellout_mat.is_hero 
  FROM 
    sellin_mat 
    LEFT JOIN sellout_mat ON (
      (
        (sellin_mat.sap_matl_num):: TEXT = (sellout_mat.dstrbtr_matl_num):: TEXT
      )
    )
), 
transformed as(
  SELECT 
    (time."year"):: varchar AS year_jnj, 
    time.qrtr AS year_quarter_jnj, 
    time.mnth_id AS year_month_jnj, 
    time.mnth_no AS month_number_jnj, 
    sellin_fact.cust_id AS customer_id, 
    cust.sap_cust_nm AS sap_customer_name, 
    sellin_fact.sls_org AS sap_sales_org, 
    cust.sap_cmp_id AS sap_company_id, 
    cmp.company_nm AS sap_company_name, 
    cust.sap_addr AS sap_address, 
    cust.sap_region, 
    cust.sap_city, 
    cust.sap_post_cd AS sap_post_code, 
    cust.sap_chnl_desc AS sap_channel_description, 
    sellin_fact.sap_sls_office_cd AS sap_sales_office_code, 
    sellin_fact.sap_sls_office_desc AS sap_sales_office_description, 
    sellin_fact.sap_sls_grp_cd AS sap_sales_group_code, 
    sellin_fact.sap_sls_grp_desc AS sap_sales_group_description, 
    cust.sap_cust_id, 
    cust.sap_cust_nm, 
    cust.sap_sls_org, 
    cust.sap_cntry_cd, 
    cust.sap_cntry_nm, 
    cust.sap_addr, 
    cust.sap_state_cd, 
    cust.sap_chnl_desc, 
    cust.sap_prnt_cust_key, 
    cust.sap_prnt_cust_desc, 
    cust.sap_cust_chnl_key, 
    cust.sap_cust_chnl_desc, 
    cust.sap_cust_sub_chnl_key, 
    cust.sap_sub_chnl_desc, 
    cust.sap_go_to_mdl_key, 
    cust.go_to_model_description, 
    cust.sap_bnr_key, 
    cust.banner_description, 
    cust.sap_bnr_frmt_key, 
    cust.sap_bnr_frmt_desc, 
    cust.retail_env, 
    sellin_fact.plnt AS plant, 
    sellin_fact.acct_no AS account_number, 
    sellin_fact.cust_grp AS customer_group, 
    sellin_fact.cust_sls AS customer_sales, 
    sellin_fact.pstng_per AS posting_per, 
    sellin_fact.dstr_chnl AS distributor_channel, 
    sellin_fact.item_cd AS item_code, 
    mat.sap_mat_desc AS item_description, 
    mat.gph_prod_frnchse AS franchise, 
    mat.gph_prod_brnd AS brand, 
    mat.gph_prod_vrnt AS variant, 
    mat.gph_prod_sgmnt AS segment, 
    mat.gph_prod_put_up_desc AS put_up, 
    mat.prod_sub_brand, 
    mat.prod_subsegment, 
    mat.prod_category, 
    mat.prod_subcategory, 
    mat.is_npi AS npi_indicator, 
    mat.npi_str_period AS npi_start_date, 
    mat.npi_end_period AS npi_end_date, 
    mat.is_reg AS reg_indicator, 
    mat.is_hero AS hero_indicator, 
    sellin_fact.max_pstng_dt, 
    sellin_fact.area, 
    sellin_fact.category, 
    sellin_fact.account_name, 
    sellin_fact.base_val AS base_value, 
    sellin_fact.sls_qty AS sales_quantity, 
    sellin_fact.ret_qty AS return_quantity, 
    sellin_fact.sls_less_rtn_qty AS sales_less_return_quantity, 
    sellin_fact.gts_val AS gross_trade_sales_value, 
    sellin_fact.ret_val AS return_value, 
    sellin_fact.gts_less_rtn_val AS gross_trade_sales_less_return_value, 
    sellin_fact.tp_val AS tp_value, 
    sellin_fact.nts_val AS net_trade_sales_value, 
    sellin_fact.nts_qty AS net_trade_sales_quantity, 
    sellin_fact.ciw_account_value 
  FROM 
    (
      (
        (
          (
            sellin_fact 
            JOIN time ON (
              (
                (sellin_fact.jj_mnth_id) = time.mnth_id
              )
            )
          ) 
          LEFT JOIN cust ON (
            (
              (sellin_fact.cust_id) = (cust.sap_cust_id)
            )
          )
        ) 
        LEFT JOIN cmp ON (
          (
            (cust.sap_cmp_id) = (cmp.co_cd)
          )
        )
      ) 
      LEFT JOIN mat ON (
        (
          (sellin_fact.item_cd) = (mat.sap_matl_num)
        )
      )
    )
) 
select * from transformed
