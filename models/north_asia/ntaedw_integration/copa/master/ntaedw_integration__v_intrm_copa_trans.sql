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
        iff(rtrim(a.co_cd)='',null,rtrim(a.co_cd)) as co_cd,
        iff(rtrim(a.cntl_area)='',null,rtrim(a.cntl_area)) as cntl_area,
        iff(rtrim(a.prft_ctr)='',null,rtrim(a.prft_ctr)) as prft_ctr,
        iff(rtrim(a.sls_org)='',null,rtrim(a.sls_org)) as sls_org,
        iff(rtrim(a.matl_num)='',null,rtrim(a.matl_num)) as matl_num,
        iff(rtrim(a.cust_num)='',null,rtrim(a.cust_num)) as cust_num,
        iff(rtrim(a.div)='',null,rtrim(a.div)) as div,
        iff(rtrim(a.plnt)='',null,rtrim(a.plnt)) as plnt,
        iff(rtrim(a.chrt_acct)='',null,rtrim(a.chrt_acct)) as chrt_acct,
        iff(rtrim(a.acct_num)='',null,rtrim(a.acct_num)) as acct_num,
        iff(rtrim(a.dstr_chnl)='',null,rtrim(a.dstr_chnl)) as dstr_chnl,
        iff(rtrim(a.fisc_yr_var)='',null,rtrim(a.fisc_yr_var)) as fisc_yr_var,
        iff(rtrim(a.vers)='',null,rtrim(a.vers)) as vers,
        iff(rtrim(a.bw_delta_upd_mode)='',null,rtrim(a.bw_delta_upd_mode)) as bw_delta_upd_mode,
        iff(rtrim(a.bill_typ)='',null,rtrim(a.bill_typ)) as bill_typ,
        iff(rtrim(a.sls_ofc)='',null,rtrim(a.sls_ofc)) as sls_ofc,
        iff(rtrim(d.ctry_nm)='',null,rtrim(d.ctry_nm)) as ctry_nm,
        iff(rtrim(d.ctry_key)='',null,rtrim(d.ctry_key)) as ctry_key,
        iff(rtrim(a.sls_deal)='',null,rtrim(a.sls_deal)) as sls_deal,
        iff(rtrim(a.sls_grp)='',null,rtrim(a.sls_grp)) as sls_grp,
        iff(rtrim(a.sls_emp_hist)='',null,rtrim(a.sls_emp_hist)) as sls_emp_hist,
        iff(rtrim(a.sls_dist)='',null,rtrim(a.sls_dist)) as sls_dist,
        iff(rtrim(a.cust_grp)='',null,rtrim(a.cust_grp)) as cust_grp,
        iff(rtrim(a.cust_sls)='',null,rtrim(a.cust_sls)) as cust_sls,
        iff(rtrim(a.buss_area)='',null,rtrim(a.buss_area)) as buss_area,
        iff(rtrim(a.val_type_rpt)='',null,rtrim(a.val_type_rpt)) as val_type_rpt,
        iff(rtrim(a.mercia_ref)='',null,rtrim(a.mercia_ref)) as mercia_ref,
        iff(rtrim(a.caln_day)='',null,rtrim(a.caln_day)) as caln_day,
        iff(rtrim(a.caln_yr_mo)='',null,rtrim(a.caln_yr_mo)) as caln_yr_mo,
        iff(rtrim(a.fisc_yr)='',null,rtrim(a.fisc_yr)) as fisc_yr,
        iff(rtrim(a.pstng_per)='',null,rtrim(a.pstng_per)) as pstng_per,
        iff(rtrim(a.fisc_yr_per)='',null,rtrim(a.fisc_yr_per)) as fisc_yr_per,
        iff(rtrim(a.b3_base_prod)='',null,rtrim(a.b3_base_prod)) as b3_base_prod,
        iff(rtrim(a.b4_var)='',null,rtrim(a.b4_var)) as b4_var,
        iff(rtrim(a.b5_put_up)='',null,rtrim(a.b5_put_up)) as b5_put_up,
        iff(rtrim(a.b1_mega_brnd)='',null,rtrim(a.b1_mega_brnd)) as b1_mega_brnd,
        iff(rtrim(a.b2_brnd)='',null,rtrim(a.b2_brnd)) as b2_brnd,
        iff(rtrim(a.reg)='',null,rtrim(a.reg)) as reg,
        iff(rtrim(a.prod_minor)='',null,rtrim(a.prod_minor)) as prod_minor,
        iff(rtrim(a.prod_maj)='',null,rtrim(a.prod_maj)) as prod_maj,
        iff(rtrim(a.prod_fran)='',null,rtrim(a.prod_fran)) as prod_fran,
        iff(rtrim(a.fran)='',null,rtrim(a.fran)) as fran,
        iff(rtrim(a.gran_grp)='',null,rtrim(a.gran_grp)) as gran_grp,
        iff(rtrim(a.oper_grp)='',null,rtrim(a.oper_grp)) as oper_grp,
        iff(rtrim(a.sls_prsn_resp)='',null,rtrim(a.sls_prsn_resp)) as sls_prsn_resp,
        iff(rtrim(a.matl_sls)='',null,rtrim(a.matl_sls)) as matl_sls,
        iff(rtrim(a.prod_hier)='',null,rtrim(a.prod_hier)) as prod_hier,
        iff(rtrim(a.mgmt_entity)='',null,rtrim(a.mgmt_entity)) as mgmt_entity,
        iff(rtrim(a.fx_amt_cntl_area_crncy)='',null,rtrim(a.fx_amt_cntl_area_crncy)) as fx_amt_cntl_area_crncy,
        iff(rtrim(a.amt_cntl_area_crncy)='',null,rtrim(a.amt_cntl_area_crncy)) as amt_cntl_area_crncy,
        iff(rtrim(a.crncy_key)='',null,rtrim(a.crncy_key)) as crncy_key,
        iff(rtrim(a.amt_obj_crncy)='',null,rtrim(a.amt_obj_crncy)) as amt_obj_crncy,
        iff(rtrim(a.obj_crncy_co_obj)='',null,rtrim(a.obj_crncy_co_obj)) as obj_crncy_co_obj,
        iff(rtrim(a.grs_amt_trans_crncy)='',null,rtrim(a.grs_amt_trans_crncy)) as grs_amt_trans_crncy,
        iff(rtrim(a.crncy_key_trans_crncy)='',null,rtrim(a.crncy_key_trans_crncy)) as crncy_key_trans_crncy,
        iff(rtrim(a.qty)='',null,rtrim(a.qty)) as qty,
        iff(rtrim(a.uom)='',null,rtrim(a.uom)) as uom,
        iff(rtrim(a.sls_vol)='',null,rtrim(a.sls_vol)) as sls_vol,
        iff(rtrim(a.un_sls_vol)='',null,rtrim(a.un_sls_vol)) as un_sls_vol,
        current_timestamp()::timestamp without time zone AS vld_from,
        iff(rtrim(b.sls_grp)='',null,rtrim(b.sls_grp)) AS cust_sls_grp,
        iff(rtrim(h.sls_grp)='',null,rtrim(h.sls_grp))::character varying(40) AS sls_grp_desc,
        iff(rtrim(b.sls_ofc)='',null,rtrim(b.sls_ofc)) AS cust_sls_ofc,
        iff(rtrim(b.sls_ofc_desc)='',null,rtrim(b.sls_ofc_desc)) as sls_ofc_desc,
        iff(rtrim(c.matl_desc)='',null,rtrim(c.matl_desc)) as matl_desc,
        iff(rtrim(c.mega_brnd_desc)='',null,rtrim(c.mega_brnd_desc)) as mega_brnd_desc,
        iff(rtrim(c.brnd_desc)='',null,rtrim(c.brnd_desc)) as brnd_desc,
        iff(rtrim(c.varnt_desc)='',null,rtrim(c.varnt_desc)) as varnt_desc,
        iff(rtrim(c.base_prod_desc)='',null,rtrim(c.base_prod_desc)) as base_prod_desc,
        iff(rtrim(c.put_up_desc)='',null,rtrim(c.put_up_desc)) as put_up_desc,
        COALESCE(h.channel, 'Others'::character varying) AS channel,
        iff(rtrim(e.med_desc)='',null,rtrim(e.med_desc)) AS med_desc,
        iff(rtrim(f.cust_nm)='',null,rtrim(f.cust_nm)) AS edw_cust_nm,
        iff(rtrim(g.from_crncy)='',null,rtrim(g.from_crncy)) as from_crncy,
        iff(rtrim(g.to_crncy)='',null,rtrim(g.to_crncy)) as to_crncy,
        iff(rtrim(g.ex_rt_typ)='',null,rtrim(g.ex_rt_typ)) as ex_rt_typ,
        iff(rtrim(g.ex_rt)='',null,rtrim(g.ex_rt)) as ex_rt,
        iff(rtrim(a.acct_hier_desc)='',null,rtrim(a.acct_hier_desc)) as acct_hier_desc,
        iff(rtrim(a.acct_hier_shrt_desc)='',null,rtrim(a.acct_hier_shrt_desc)) as acct_hier_shrt_desc,
        iff(rtrim(d.company_nm)='',null,rtrim(d.company_nm)) as company_nm,
        iff(rtrim(i.ean_num)='',null,rtrim(i.ean_num)) as ean_num,
        iff(rtrim(j.prod_hier_l1)='',null,rtrim(j.prod_hier_l1)) AS prod_hier_lvl1,
        iff(rtrim(j.prod_hier_l2)='',null,rtrim(j.prod_hier_l2)) AS prod_hier_lvl2,
        iff(rtrim(j.prod_hier_l3)='',null,rtrim(j.prod_hier_l3)) AS prod_hier_lvl3,
        iff(rtrim(j.prod_hier_l4)='',null,rtrim(j.prod_hier_l4)) AS prod_hier_lvl4,
        iff(rtrim(j.prod_hier_l5)='',null,rtrim(j.prod_hier_l5)) AS prod_hier_lvl5,
        iff(rtrim(j.prod_hier_l6)='',null,rtrim(j.prod_hier_l6)) AS prod_hier_lvl6,
        iff(rtrim(j.prod_hier_l7)='',null,rtrim(j.prod_hier_l7)) AS prod_hier_lvl7,
        iff(rtrim(j.prod_hier_l8)='',null,rtrim(j.prod_hier_l8)) AS prod_hier_lvl8,
        iff(rtrim(j.prod_hier_l9)='',null,rtrim(j.prod_hier_l9)) AS prod_hier_lvl9,
        iff(rtrim(h.store_type)='',null,rtrim(h.store_type)) as store_type
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
