with edw_ims_fact as (
    select * from {{ ref('ntaedw_integration__edw_ims_fact') }}
),
wks_parameter_gt_sellout as (
    select * from {{ ref('ntawks_integration__wks_parameter_gt_sellout') }}
),
v_intrm_reg_crncy_exch_fiscper as (
    select * from {{ ref('aspedw_integration__v_intrm_reg_crncy_exch_fiscper') }}
),
edw_intrm_calendar as (
    select * from {{ ref('ntaedw_integration__edw_intrm_calendar') }}
),
edw_customer_sales_dim as (
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_customer_attr_flat_dim as (
    select * from aspedw_integration.edw_customer_attr_flat_dim
),
edw_product_attr_dim as (
    select * from aspedw_integration.edw_product_attr_dim
),
txn as (
    SELECT edw_ims_fact.ims_txn_dt,
        edw_ims_fact.dstr_cd,
        edw_ims_fact.dstr_nm,
        edw_ims_fact.cust_cd,
        edw_ims_fact.cust_nm,
        edw_ims_fact.prod_nm,
        edw_ims_fact.ean_num,
        edw_ims_fact.ctry_cd,
        edw_ims_fact.crncy_cd,
        edw_ims_fact.sap_code,
        edw_ims_fact.sku_type,
        edw_ims_fact.unit_prc,
        edw_ims_fact.sub_customer_code,
        edw_ims_fact.sub_customer_name,
        sum(edw_ims_fact.sls_amt) AS sls_amt,
        sum(edw_ims_fact.sls_qty) AS sls_qty,
        edw_ims_fact.sales_priority,
        edw_ims_fact.sales_stores,
        edw_ims_fact.sales_rate
    FROM edw_ims_fact
    WHERE (
            (
                (
                    (edw_ims_fact.ctry_cd)::text = ('KR'::character varying)::text
                )
                AND (
                    date_part(
                        year,
                        (edw_ims_fact.ims_txn_dt)::timestamp without time zone
                    ) > (
                        date_part(
                            year,
                            current_timestamp()::timestamp without time zone
                        ) - (3)::double precision
                    )
                )
            )
            AND (
                upper((edw_ims_fact.dstr_nm)::text) IN (
                    SELECT DISTINCT upper((wks_parameter_gt_sellout.dstr_nm)::text) AS upper
                    FROM wks_parameter_gt_sellout
                    WHERE (
                            (
                                upper((wks_parameter_gt_sellout.country_cd)::text) = ('KR'::character varying)::text
                            )
                            AND (
                                upper((wks_parameter_gt_sellout.parameter_name)::text) = ('GT_SELLOUT'::character varying)::text
                            )
                        )
                )
            )
        )
    GROUP BY edw_ims_fact.ims_txn_dt,
        edw_ims_fact.dstr_cd,
        edw_ims_fact.dstr_nm,
        edw_ims_fact.cust_cd,
        edw_ims_fact.cust_nm,
        edw_ims_fact.prod_nm,
        edw_ims_fact.ean_num,
        edw_ims_fact.ctry_cd,
        edw_ims_fact.crncy_cd,
        edw_ims_fact.sap_code,
        edw_ims_fact.sku_type,
        edw_ims_fact.unit_prc,
        edw_ims_fact.sub_customer_code,
        edw_ims_fact.sub_customer_name,
        edw_ims_fact.sales_priority,
        edw_ims_fact.sales_stores,
        edw_ims_fact.sales_rate
),
cal as (
    SELECT edw_intrm_calendar.cal_day,
        edw_intrm_calendar.fisc_yr,
        edw_intrm_calendar.fisc_wk_num,
        edw_intrm_calendar.fisc_per
    FROM edw_intrm_calendar
),
exrt as (
    SELECT v_intrm_crncy_exch.ex_rt_typ,
        v_intrm_crncy_exch.from_crncy,
        v_intrm_crncy_exch.vld_from,
        v_intrm_crncy_exch.fisc_per,
        v_intrm_crncy_exch.to_crncy,
        v_intrm_crncy_exch.ex_rt
    FROM v_intrm_reg_crncy_exch_fiscper v_intrm_crncy_exch
    WHERE (
            (
                (v_intrm_crncy_exch.from_crncy)::text = ('KRW'::character varying)::text
            )
            AND (
                (
                    (
                        (v_intrm_crncy_exch.to_crncy)::text = ('USD'::character varying)::text
                    )
                    OR (
                        (v_intrm_crncy_exch.to_crncy)::text = ('KRW'::character varying)::text
                    )
                )
                OR (
                    (v_intrm_crncy_exch.to_crncy)::text = ('SGD'::character varying)::text
                )
            )
        )
),
prod as (
    SELECT DISTINCT (edw_product_attr_dim.ean)::character varying(100) AS ean_num,
        edw_product_attr_dim.cntry,
        edw_product_attr_dim.sap_matl_num,
        edw_product_attr_dim.prod_hier_l1,
        edw_product_attr_dim.prod_hier_l2,
        edw_product_attr_dim.prod_hier_l3,
        edw_product_attr_dim.prod_hier_l4,
        edw_product_attr_dim.prod_hier_l5,
        edw_product_attr_dim.prod_hier_l6,
        edw_product_attr_dim.prod_hier_l7,
        edw_product_attr_dim.prod_hier_l8,
        edw_product_attr_dim.prod_hier_l9,
        edw_product_attr_dim.lcl_prod_nm
    FROM edw_product_attr_dim edw_product_attr_dim
),
cust as (
    SELECT DISTINCT edw_customer_attr_flat_dim.aw_remote_key,
        edw_customer_attr_flat_dim.cntry,
        edw_customer_attr_flat_dim.sold_to_party,
        edw_customer_attr_flat_dim.cust_nm,
        edw_customer_attr_flat_dim.store_typ,
        edw_customer_attr_flat_dim.channel,
        edw_customer_attr_flat_dim.sls_grp,
        edw_customer_attr_flat_dim.sls_ofc,
        edw_customer_attr_flat_dim.sls_ofc_desc,
        edw_customer_attr_flat_dim.sls_grp_cd
    FROM edw_customer_attr_flat_dim
    WHERE (
            (edw_customer_attr_flat_dim.cntry)::text = ('Korea'::character varying)::text
        )
),
sls_grp_lkp as (
    SELECT DISTINCT edw_customer_sales_dim.cust_num,
        edw_customer_sales_dim.sls_grp,
        edw_customer_sales_dim.sls_grp_desc
    FROM edw_customer_sales_dim
),
final as (
    SELECT txn.ims_txn_dt,
        txn.dstr_cd,
        txn.dstr_nm,
        txn.cust_cd,
        txn.cust_nm,
        txn.prod_nm,
        txn.ean_num,
        txn.ctry_cd,
        txn.crncy_cd,
        txn.sap_code,
        txn.sku_type,
        txn.unit_prc,
        txn.sls_amt,
        txn.sls_qty,
        cal.cal_day,
        cal.fisc_yr,
        cal.fisc_wk_num,
        cal.fisc_per,
        exrt.ex_rt_typ,
        exrt.from_crncy,
        exrt.to_crncy,
        exrt.vld_from,
        exrt.ex_rt,
        prod.sap_matl_num,
        prod.prod_hier_l1,
        prod.prod_hier_l2,
        prod.prod_hier_l3,
        prod.prod_hier_l4,
        prod.prod_hier_l5,
        prod.prod_hier_l6,
        prod.prod_hier_l7,
        prod.prod_hier_l8,
        prod.prod_hier_l9,
        cust.store_typ,
        cust.channel,
        sls_grp_lkp.sls_grp_desc AS sls_grp,
        prod.lcl_prod_nm,
        txn.sub_customer_code,
        txn.sub_customer_name,
        txn.sales_priority,
        txn.sales_stores,
        txn.sales_rate
    FROM txn
        LEFT JOIN cal ON ((txn.ims_txn_dt = cal.cal_day))
        LEFT JOIN exrt ON ((txn.crncy_cd)::text = (exrt.from_crncy)::text)
        AND (cal.fisc_per = exrt.fisc_per)
        LEFT JOIN prod ON ((txn.ctry_cd)::text = (prod.cntry)::text)
        AND ((txn.ean_num)::text = (prod.ean_num)::text)
        LEFT JOIN cust ON ((txn.cust_cd)::text = (cust.aw_remote_key)::text)
        LEFT JOIN sls_grp_lkp ON (txn.cust_cd)::text = ltrim(
            (sls_grp_lkp.cust_num)::text,
            ((0)::character varying)::text
        )
)
select * from final