with edw_copa_trans_fact as (
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_copa_trans_fact_adjustment as (
    select * from {{ source('aspedw_integration','edw_copa_trans_fact_adjustment') }}
),
edw_customer_sales_dim as (
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_material_dim as (
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_profit_center_dim as (
    select * from {{ ref('aspedw_integration__edw_profit_center_dim') }}
),
edw_customer_base_dim as (
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
v_intrm_crncy_exch as (
    select * from {{ ref('ntaedw_integration__v_intrm_crncy_exch') }}
),
edw_company_dim as (
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_customer_attr_flat_dim as (
    select * from {{ ref('aspedw_integration__edw_customer_attr_flat_dim') }}
),
edw_material_sales_dim as (
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_product_attr_dim as (
    select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
),
a as 
(
    SELECT edw_copa_trans_fact.co_cd,
        edw_copa_trans_fact.cntl_area,
        edw_copa_trans_fact.prft_ctr,
        edw_copa_trans_fact.sls_org,
        edw_copa_trans_fact.matl_num,
        edw_copa_trans_fact.cust_num,
        edw_copa_trans_fact.div,
        edw_copa_trans_fact.plnt,
        edw_copa_trans_fact.chrt_acct,
        edw_copa_trans_fact.acct_num,
        edw_copa_trans_fact.dstr_chnl,
        edw_copa_trans_fact.fisc_yr_var,
        edw_copa_trans_fact.vers,
        edw_copa_trans_fact.bw_delta_upd_mode,
        edw_copa_trans_fact.bill_typ,
        edw_copa_trans_fact.sls_ofc,
        edw_copa_trans_fact.ctry_key,
        edw_copa_trans_fact.sls_deal,
        edw_copa_trans_fact.sls_grp,
        edw_copa_trans_fact.sls_emp_hist,
        edw_copa_trans_fact.sls_dist,
        edw_copa_trans_fact.cust_grp,
        edw_copa_trans_fact.cust_sls,
        edw_copa_trans_fact.buss_area,
        edw_copa_trans_fact.val_type_rpt,
        edw_copa_trans_fact.mercia_ref,
        edw_copa_trans_fact.caln_day,
        edw_copa_trans_fact.caln_yr_mo,
        edw_copa_trans_fact.fisc_yr,
        edw_copa_trans_fact.pstng_per,
        edw_copa_trans_fact.fisc_yr_per,
        edw_copa_trans_fact.b3_base_prod,
        edw_copa_trans_fact.b4_var,
        edw_copa_trans_fact.b5_put_up,
        edw_copa_trans_fact.b1_mega_brnd,
        edw_copa_trans_fact.b2_brnd,
        edw_copa_trans_fact.reg,
        edw_copa_trans_fact.prod_minor,
        edw_copa_trans_fact.prod_maj,
        edw_copa_trans_fact.prod_fran,
        edw_copa_trans_fact.fran,
        edw_copa_trans_fact.gran_grp,
        edw_copa_trans_fact.oper_grp,
        edw_copa_trans_fact.sls_prsn_resp,
        edw_copa_trans_fact.matl_sls,
        edw_copa_trans_fact.prod_hier,
        edw_copa_trans_fact.mgmt_entity,
        edw_copa_trans_fact.fx_amt_cntl_area_crncy,
        edw_copa_trans_fact.amt_cntl_area_crncy,
        edw_copa_trans_fact.crncy_key,
        edw_copa_trans_fact.amt_obj_crncy,
        edw_copa_trans_fact.obj_crncy_co_obj,
        edw_copa_trans_fact.grs_amt_trans_crncy,
        edw_copa_trans_fact.crncy_key_trans_crncy,
        edw_copa_trans_fact.qty,
        edw_copa_trans_fact.uom,
        edw_copa_trans_fact.sls_vol,
        edw_copa_trans_fact.un_sls_vol,
        edw_copa_trans_fact.acct_hier_desc,
        edw_copa_trans_fact.acct_hier_shrt_desc,
        edw_copa_trans_fact.crt_dttm,
        edw_copa_trans_fact.updt_dttm
    FROM edw_copa_trans_fact
    UNION ALL
    SELECT edw_copa_trans_fact_adjustment.co_cd,
        edw_copa_trans_fact_adjustment.cntl_area,
        edw_copa_trans_fact_adjustment.prft_ctr,
        edw_copa_trans_fact_adjustment.sls_org,
        edw_copa_trans_fact_adjustment.matl_num,
        edw_copa_trans_fact_adjustment.cust_num,
        edw_copa_trans_fact_adjustment.div,
        edw_copa_trans_fact_adjustment.plnt,
        edw_copa_trans_fact_adjustment.chrt_acct,
        edw_copa_trans_fact_adjustment.acct_num,
        edw_copa_trans_fact_adjustment.dstr_chnl,
        edw_copa_trans_fact_adjustment.fisc_yr_var,
        edw_copa_trans_fact_adjustment.vers,
        edw_copa_trans_fact_adjustment.bw_delta_upd_mode,
        edw_copa_trans_fact_adjustment.bill_typ,
        edw_copa_trans_fact_adjustment.sls_ofc,
        edw_copa_trans_fact_adjustment.ctry_key,
        edw_copa_trans_fact_adjustment.sls_deal,
        edw_copa_trans_fact_adjustment.sls_grp,
        edw_copa_trans_fact_adjustment.sls_emp_hist,
        edw_copa_trans_fact_adjustment.sls_dist,
        edw_copa_trans_fact_adjustment.cust_grp,
        edw_copa_trans_fact_adjustment.cust_sls,
        edw_copa_trans_fact_adjustment.buss_area,
        edw_copa_trans_fact_adjustment.val_type_rpt,
        edw_copa_trans_fact_adjustment.mercia_ref,
        edw_copa_trans_fact_adjustment.caln_day,
        edw_copa_trans_fact_adjustment.caln_yr_mo,
        edw_copa_trans_fact_adjustment.fisc_yr,
        edw_copa_trans_fact_adjustment.pstng_per,
        edw_copa_trans_fact_adjustment.fisc_yr_per,
        edw_copa_trans_fact_adjustment.b3_base_prod,
        edw_copa_trans_fact_adjustment.b4_var,
        edw_copa_trans_fact_adjustment.b5_put_up,
        edw_copa_trans_fact_adjustment.b1_mega_brnd,
        edw_copa_trans_fact_adjustment.b2_brnd,
        edw_copa_trans_fact_adjustment.reg,
        edw_copa_trans_fact_adjustment.prod_minor,
        edw_copa_trans_fact_adjustment.prod_maj,
        edw_copa_trans_fact_adjustment.prod_fran,
        edw_copa_trans_fact_adjustment.fran,
        edw_copa_trans_fact_adjustment.gran_grp,
        edw_copa_trans_fact_adjustment.oper_grp,
        edw_copa_trans_fact_adjustment.sls_prsn_resp,
        edw_copa_trans_fact_adjustment.matl_sls,
        edw_copa_trans_fact_adjustment.prod_hier,
        edw_copa_trans_fact_adjustment.mgmt_entity,
        edw_copa_trans_fact_adjustment.fx_amt_cntl_area_crncy,
        edw_copa_trans_fact_adjustment.amt_cntl_area_crncy,
        edw_copa_trans_fact_adjustment.crncy_key,
        edw_copa_trans_fact_adjustment.amt_obj_crncy,
        edw_copa_trans_fact_adjustment.obj_crncy_co_obj,
        edw_copa_trans_fact_adjustment.grs_amt_trans_crncy,
        edw_copa_trans_fact_adjustment.crncy_key_trans_crncy,
        edw_copa_trans_fact_adjustment.qty,
        edw_copa_trans_fact_adjustment.uom,
        edw_copa_trans_fact_adjustment.sls_vol,
        edw_copa_trans_fact_adjustment.un_sls_vol,
        edw_copa_trans_fact_adjustment.acct_hier_desc,
        edw_copa_trans_fact_adjustment.acct_hier_shrt_desc,
        edw_copa_trans_fact_adjustment.crt_dttm,
        edw_copa_trans_fact_adjustment.updt_dttm
    FROM edw_copa_trans_fact_adjustment
),
final as
(   
    SELECT 
        a.co_cd,
        a.cntl_area,
        a.prft_ctr,
        a.sls_org,
        a.matl_num,
        a.cust_num,
        a.div,
        a.plnt,
        a.chrt_acct,
        a.acct_num,
        a.dstr_chnl,
        a.fisc_yr_var,
        a.vers,
        a.bw_delta_upd_mode,
        a.bill_typ,
        a.sls_ofc,
        d.ctry_nm,
        d.ctry_key,
        a.sls_deal,
        a.sls_grp,
        a.sls_emp_hist,
        a.sls_dist,
        a.cust_grp,
        a.cust_sls,
        a.buss_area,
        a.val_type_rpt,
        a.mercia_ref,
        a.caln_day,
        a.caln_yr_mo,
        a.fisc_yr,
        a.pstng_per,
        a.fisc_yr_per,
        a.b3_base_prod,
        a.b4_var,
        a.b5_put_up,
        a.b1_mega_brnd,
        a.b2_brnd,
        a.reg,
        a.prod_minor,
        a.prod_maj,
        a.prod_fran,
        a.fran,
        a.gran_grp,
        a.oper_grp,
        a.sls_prsn_resp,
        a.matl_sls,
        a.prod_hier,
        a.mgmt_entity,
        a.fx_amt_cntl_area_crncy,
        a.amt_cntl_area_crncy,
        a.crncy_key,
        a.amt_obj_crncy,
        a.obj_crncy_co_obj,
        a.grs_amt_trans_crncy,
        a.crncy_key_trans_crncy,
        a.qty,
        a.uom,
        a.sls_vol,
        a.un_sls_vol,
        current_timestamp()::timestamp without time zone AS vld_from,
        b.sls_grp AS cust_sls_grp,
        (h.sls_grp)::character varying(40) AS sls_grp_desc,
        b.sls_ofc AS cust_sls_ofc,
        b.sls_ofc_desc,
        c.matl_desc,
        c.mega_brnd_desc,
        c.brnd_desc,
        c.varnt_desc,
        c.base_prod_desc,
        c.put_up_desc,
        COALESCE(h.channel, 'Others'::character varying) AS channel,
        e.med_desc,
        f.cust_nm AS edw_cust_nm,
        g.from_crncy,
        g.to_crncy,
        g.ex_rt_typ,
        g.ex_rt,
        a.acct_hier_desc,
        a.acct_hier_shrt_desc,
        d.company_nm,
        i.ean_num,
        j.prod_hier_l1 AS prod_hier_lvl1,
        j.prod_hier_l2 AS prod_hier_lvl2,
        j.prod_hier_l3 AS prod_hier_lvl3,
        j.prod_hier_l4 AS prod_hier_lvl4,
        j.prod_hier_l5 AS prod_hier_lvl5,
        j.prod_hier_l6 AS prod_hier_lvl6,
        j.prod_hier_l7 AS prod_hier_lvl7,
        j.prod_hier_l8 AS prod_hier_lvl8,
        j.prod_hier_l9 AS prod_hier_lvl9,
        h.store_type
    FROM a
        LEFT JOIN edw_customer_sales_dim b ON 
        (
            (
                (
                    (
                        ((a.cust_num)::text = (b.cust_num)::text)
                        AND ((a.sls_org)::text = (b.sls_org)::text)
                    )
                    AND ((a.dstr_chnl)::text = (b.dstr_chnl)::text)
                )
                AND ((a.div)::text = (b.div)::text)
            )
        )
        LEFT JOIN 
        (
            SELECT DISTINCT edw_material_dim.matl_desc,
                edw_material_dim.mega_brnd_desc,
                edw_material_dim.brnd_desc,
                edw_material_dim.varnt_desc,
                edw_material_dim.base_prod_desc,
                edw_material_dim.put_up_desc,
                edw_material_dim.matl_num
            FROM edw_material_dim
        ) c ON (((a.matl_num)::text = (c.matl_num)::text))
        LEFT JOIN edw_profit_center_dim e ON 
        (
            (
                ltrim(
                    (a.prft_ctr)::text,
                    ((0)::character varying)::text
                ) = ltrim(
                    (e.prft_ctr)::text,
                    ((0)::character varying)::text
                )
            )
        )
        LEFT JOIN edw_customer_base_dim f ON 
        (
            (
                ltrim(
                    (a.cust_num)::text,
                    ((0)::character varying)::text
                ) = ltrim(
                    (f.cust_num)::text,
                    ((0)::character varying)::text
                )
            )
        )
        LEFT JOIN v_intrm_crncy_exch g ON 
        (
            (
                (a.obj_crncy_co_obj)::text = (g.from_crncy)::text
            )
        )
        JOIN 
        (
            SELECT edw_company_dim.co_cd,
                edw_company_dim.ctry_nm,
                edw_company_dim.ctry_key,
                edw_company_dim.company_nm
            FROM edw_company_dim
            WHERE (
                    (
                        (
                            (edw_company_dim.ctry_key)::text = ('TW'::character varying)::text
                        )
                        OR (
                            (edw_company_dim.ctry_key)::text = ('HK'::character varying)::text
                        )
                    )
                    OR (
                        (edw_company_dim.ctry_key)::text = ('KR'::character varying)::text
                    )
                )
        ) d ON (((a.co_cd)::text = (d.co_cd)::text))
        LEFT JOIN 
        (
            SELECT DISTINCT edw_customer_attr_flat_dim.aw_remote_key AS sold_to_prty,
                edw_customer_attr_flat_dim.channel,
                edw_customer_attr_flat_dim.sls_grp,
                edw_customer_attr_flat_dim.store_typ AS store_type
            FROM edw_customer_attr_flat_dim
            WHERE (
                    (edw_customer_attr_flat_dim.trgt_type)::text = ('flat'::character varying)::text
                )
        ) h ON 
        (
            (
                ltrim(
                    (b.cust_num)::text,
                    ((0)::character varying)::text
                ) = ltrim(
                    (h.sold_to_prty)::text,
                    ((0)::character varying)::text
                )
            )
        )
        LEFT JOIN 
        (
            SELECT edw_material_sales_dim.sls_org,
                edw_material_sales_dim.dstr_chnl,
                edw_material_sales_dim.matl_num AS matl,
                edw_material_sales_dim.ean_num
            FROM edw_material_sales_dim
            WHERE (
                    (edw_material_sales_dim.ean_num)::text <> (''::character varying)::text
                )
        ) i ON 
        (
            (
                (
                    (
                        ltrim(
                            (a.matl_num)::text,
                            ((0)::character varying)::text
                        ) = ltrim((i.matl)::text, ((0)::character varying)::text)
                    )
                    AND (
                        (COALESCE(a.sls_org, '#'::character varying))::text = (COALESCE(i.sls_org, '#'::character varying))::text
                    )
                )
                AND (
                    (COALESCE(a.dstr_chnl, '#'::character varying))::text = (COALESCE(i.dstr_chnl, '#'::character varying))::text
                )
            )
        )
        LEFT JOIN edw_product_attr_dim j ON 
        (
            (
                ((i.ean_num)::text = (j.aw_remote_key)::text)
                AND ((a.ctry_key)::text = (j.cntry)::text)
            )
        )
    WHERE 
        (
            (
                (
                    (
                        (
                            (a.obj_crncy_co_obj)::text = ('USD'::character varying)::text
                        )
                        OR (
                            (a.obj_crncy_co_obj)::text = ('SGD'::character varying)::text
                        )
                    )
                    OR (
                        (a.obj_crncy_co_obj)::text = ('HKD'::character varying)::text
                    )
                )
                OR (
                    (a.obj_crncy_co_obj)::text = ('TWD'::character varying)::text
                )
            )
            OR (
                (a.obj_crncy_co_obj)::text = ('KRW'::character varying)::text
            )
        )
)
select * from final
