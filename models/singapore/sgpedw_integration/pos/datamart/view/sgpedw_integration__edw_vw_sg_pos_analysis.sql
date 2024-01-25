with edw_vw_sg_time_dim as(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_vw_sg_curr_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_sg_curr_dim') }}
),
edw_vw_sg_pos_sales_fact as
(
    select * from {{ ref('sgpedw_integration__edw_vw_sg_pos_sales_fact') }}
),
edw_vw_sg_material_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_sg_material_dim') }}
),
itg_sg_constant_key_value as
(
    select * from {{ source('sgpitg_integration', 'itg_sg_constant_key_value') }}
),
edw_vw_sg_customer_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_sg_customer_dim') }}
),
evotd as
(
    SELECT DISTINCT
        "year" AS jj_year,
        qrtr AS jj_qtr,
        mnth_id AS jj_mnth_id,
        mnth_no AS jj_mnth_no
    FROM edw_vw_sg_time_dim
),
evcd as
(
    SELECT
        cntry_key,
        cntry_nm,
        rate_type,
        from_ccy,
        to_ccy,
        valid_date,
        jj_year,
        jj_mnth_id,
        exch_rate
    FROM edw_vw_sg_curr_dim
),
evopsf as
(
    SELECT
        cntry_cd,
        cntry_nm,
        pos_dt,
        jj_yr_week_no,
        jj_mnth_id,
        cust_cd,
        item_cd,
        item_desc,
        sap_matl_num,
        bar_cd,
        master_code,
        cust_brnch_cd,
        pos_qty,
        pos_gts,
        pos_item_prc,
        pos_tax,
        pos_nts,
        conv_factor,
        jj_qty_pc,
        jj_item_prc_per_pc,
        jj_gts,
        jj_vat_amt,
        jj_nts
    FROM edw_vw_sg_pos_sales_fact 
),
evomd as
(
    SELECT 
        DISTINCT
        sap_matl_num,
        sap_mat_desc,
        gph_region,
        gph_reg_frnchse,
        gph_reg_frnchse_grp,
        gph_prod_frnchse,
        gph_prod_brnd,
        gph_prod_sub_brnd,
        gph_prod_vrnt,
        gph_prod_needstate,
        gph_prod_ctgry,
        gph_prod_subctgry,
        gph_prod_sgmnt,
        gph_prod_subsgmnt,
        gph_prod_put_up_cd,
        gph_prod_put_up_desc,
        gph_prod_size,
        gph_prod_size_uom
    FROM edw_vw_sg_material_dim
),
isckv as
(
    SELECT
        itg_sg_constant_key_value.key AS cust_cd,
        itg_sg_constant_key_value.value AS cust_name
        FROM itg_sg_constant_key_value
    WHERE   itg_sg_constant_key_value.data_category_cd 
    = CAST((CAST((9) AS DECIMAL)) AS DECIMAL(18, 0))
),
evocd as
(
    SELECT 
        DISTINCT
        retail_env,
        sap_prnt_cust_key,
        sap_prnt_cust_desc,
        sap_bnr_key,
        sap_bnr_desc,
        sap_bnr_frmt_key,
        sap_bnr_frmt_desc
    FROM edw_vw_sg_customer_dim                  
),

final as
(
    SELECT
        'SG' AS sap_cntry_cd,
        'Singapore' AS sap_cntry_nm,
        evotd.jj_year,
        evotd.jj_qtr,
        evotd.jj_mnth_id,
        evotd.jj_mnth_no,
        evopsf.jj_yr_week_no,
        evopsf.cust_cd,
        evocd.retail_env,
        evocd.sap_prnt_cust_key,
        evocd.sap_prnt_cust_desc,
        evocd.sap_bnr_key,
        evocd.sap_bnr_desc,
        evocd.sap_bnr_frmt_key,
        evocd.sap_bnr_frmt_desc,
        evopsf.item_cd,
        evopsf.item_desc,
        evopsf.sap_matl_num,
        evomd.sap_mat_desc,
        evopsf.bar_cd,
        evopsf.master_code,
        evopsf.cust_brnch_cd,
        evomd.gph_region,
        evomd.gph_reg_frnchse,
        evomd.gph_reg_frnchse_grp,
        evomd.gph_prod_frnchse,
        evomd.gph_prod_brnd,
        evomd.gph_prod_sub_brnd,
        evomd.gph_prod_vrnt,
        evomd.gph_prod_needstate,
        evomd.gph_prod_ctgry,
        evomd.gph_prod_subctgry,
        evomd.gph_prod_sgmnt,
        evomd.gph_prod_subsgmnt,
        evomd.gph_prod_put_up_cd,
        evomd.gph_prod_put_up_desc,
        evomd.gph_prod_size,
        evomd.gph_prod_size_uom,
        evcd.to_ccy AS currency,
        evopsf.pos_qty,
        (evopsf.pos_gts * CAST((evcd.exch_rate) AS DOUBLE)) AS pos_gross_val,
        (evopsf.pos_nts * CAST((evcd.exch_rate) AS DOUBLE)) AS pos_net_val,
        evopsf.jj_qty_pc,
        (evopsf.jj_gts * evcd.exch_rate) AS pos_gross_jj_val,
        (evopsf.jj_nts * evcd.exch_rate) AS pos_net_jj_val
    FROM    evotd,evcd,evopsf
    LEFT JOIN evomd
    ON CAST((evopsf.sap_matl_num) AS TEXT) = CAST((evomd.sap_matl_num) AS TEXT),
    isckv
    LEFT JOIN   evocd
    ON CAST((isckv.cust_name) AS TEXT) = CAST((evocd.sap_bnr_frmt_desc) AS TEXT)
    WHERE   
        CAST((evopsf.jj_mnth_id) AS TEXT) = evotd.jj_mnth_id
        AND CAST((evopsf.cust_cd) AS TEXT) = CAST((isckv.cust_cd) AS TEXT)
        AND CAST((evopsf.jj_mnth_id) AS TEXT) = CAST((evcd.jj_mnth_id) AS TEXT)
)
select * from final