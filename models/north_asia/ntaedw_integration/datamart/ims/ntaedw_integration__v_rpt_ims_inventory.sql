with edw_ims_inventory_fact as
(
    select * from {{ ref('ntaedw_integration__edw_ims_inventory_fact') }}
),
itg_tw_ims_dstr_prod_price_map as
(
    select * from  ntaitg_integration.itg_tw_ims_dstr_prod_price_map
),
v_intrm_calendar_ims as
(
    select * from {{ ref('ntaedw_integration__v_intrm_calendar_ims') }}
),
edw_product_attr_dim as
(
    select * from snapaspedw_integration.edw_product_attr_dim
),
edw_customer_attr_flat_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_attr_flat_dim') }}
),
itg_ims_dstr_cust_attr as
(
    select * from snapntaitg_integration.itg_ims_dstr_cust_attr
),
v_dly_ims_txn_msl as
(
    select * from {{ ref('ntaedw_integration__v_dly_ims_txn_msl') }}
),
v_intrm_crncy_exch as
(
    select * from {{ ref('ntaedw_integration__v_intrm_crncy_exch') }}
),
inv_data as
(
    SELECT
        inv_data.invnt_dt,
        inv_data.dstr_cd,
        inv_data.dstr_nm,
        inv_data.cust_nm,
        inv_data.prod_cd,
        inv_data.prod_nm,
        inv_data.ean_num,
        inv_data.uom,
        inv_data.invnt_qty,
        inv_data.invnt_amt,
        inv_data.avg_prc_amt,
        inv_data.safety_stock,
        inv_data.bad_invnt_qty,
        inv_data.book_invnt_qty,
        inv_data.convs_amt,
        inv_data.prch_disc_amt,
        inv_data.end_invnt_qty,
        inv_data.batch_no,
        inv_data.chn_uom,
        inv_data.sls_rep_cd,
        inv_data.sls_rep_nm,
        inv_data.ctry_cd,
        inv_data.crncy_cd,
        inv_data.crt_dttm,
        inv_data.updt_dttm,
        inv_data.sell_out_price_manual,
        inv_data.storage_name,
        inv_data.area
    FROM
        (
            SELECT
                ims_inv_fact.invnt_dt,
                ims_inv_fact.dstr_cd,
                ims_inv_fact.dstr_nm,
                ims_inv_fact.cust_nm,
                ims_inv_fact.prod_cd,
                ims_inv_fact.prod_nm,
                ims_inv_fact.ean_num,
                ims_inv_fact.uom,
                ims_inv_fact.invnt_qty,
                ims_inv_fact.invnt_amt,
                ims_inv_fact.avg_prc_amt,
                ims_inv_fact.safety_stock,
                ims_inv_fact.bad_invnt_qty,
                ims_inv_fact.book_invnt_qty,
                ims_inv_fact.convs_amt,
                ims_inv_fact.prch_disc_amt,
                ims_inv_fact.end_invnt_qty,
                ims_inv_fact.batch_no,
                ims_inv_fact.chn_uom,
                ims_inv_fact.sls_rep_cd,
                ims_inv_fact.sls_rep_nm,
                ims_inv_fact.ctry_cd,
                ims_inv_fact.crncy_cd,
                ims_inv_fact.crt_dttm,
                ims_inv_fact.updt_dttm,
                pmf.sell_out_price_manual,
                ims_inv_fact.storage_name,
                ims_inv_fact.area
            FROM edw_ims_inventory_fact ims_inv_fact
            JOIN itg_tw_ims_dstr_prod_price_map pmf ON
            ims_inv_fact.dstr_cd::text = pmf.dstr_cd::text
            AND ims_inv_fact.prod_cd::text = pmf.dstr_prod_cd::text
            AND ltrim(ims_inv_fact.ean_num::text, '0') = ltrim(pmf.ean_cd::text, '0')
            AND ims_inv_fact.invnt_dt >= pmf.promotion_start_date
            AND ims_inv_fact.invnt_dt <= pmf.promotion_end_date
            AND (pmf.dstr_cd IS NOT NULL OR pmf.dstr_cd::text <> '')
            AND (pmf.dstr_prod_cd IS NOT NULL OR pmf.dstr_prod_cd::text <> '')
            AND (pmf.ean_cd IS NOT NULL OR pmf.ean_cd::text <> '')
        ) inv_data
),
not_in_inv_data_cte as 
(
    SELECT
        inv.invnt_dt,
        inv.dstr_cd,
        inv.dstr_nm,
        inv.cust_nm,
        inv.prod_cd,
        inv.prod_nm,
        inv.ean_num,
        inv.uom,
        inv.invnt_qty,
        inv.invnt_amt,
        inv.avg_prc_amt,
        inv.safety_stock,
        inv.bad_invnt_qty,
        inv.book_invnt_qty,
        inv.convs_amt,
        inv.prch_disc_amt,
        inv.end_invnt_qty,
        inv.batch_no,
        inv.chn_uom,
        inv.sls_rep_cd,
        inv.sls_rep_nm,
        inv.ctry_cd,
        inv.crncy_cd,
        inv.crt_dttm,
        inv.updt_dttm,
        pm.sell_out_price_manual,
        inv.storage_name,
        inv.area
    FROM edw_ims_inventory_fact inv
    LEFT JOIN 
    (
        SELECT itg_tw_ims_dstr_prod_price_map.dstr_cd,
            itg_tw_ims_dstr_prod_price_map.dstr_nm,
            itg_tw_ims_dstr_prod_price_map.ean_cd,
            itg_tw_ims_dstr_prod_price_map.dstr_prod_cd,
            itg_tw_ims_dstr_prod_price_map.dstr_prod_nm,
            itg_tw_ims_dstr_prod_price_map.sell_out_price_manual,
            itg_tw_ims_dstr_prod_price_map.promotion_start_date,
            itg_tw_ims_dstr_prod_price_map.promotion_end_date,
            itg_tw_ims_dstr_prod_price_map.crt_dttm,
            itg_tw_ims_dstr_prod_price_map.updt_dttm
        FROM itg_tw_ims_dstr_prod_price_map
        WHERE 
        (itg_tw_ims_dstr_prod_price_map.dstr_cd IS NULL OR itg_tw_ims_dstr_prod_price_map.dstr_cd::text = '')
        AND (itg_tw_ims_dstr_prod_price_map.dstr_prod_cd IS NULL OR itg_tw_ims_dstr_prod_price_map.dstr_prod_cd::text = '')

    ) pm ON ltrim(inv.ean_num::text, '0') = ltrim(pm.ean_cd::text, '0')
    AND inv.invnt_dt >= pm.promotion_start_date
    AND inv.invnt_dt <= pm.promotion_end_date
    WHERE
        
        (
            (inv.invnt_dt::character varying || inv.dstr_cd::text || inv.prod_cd::text || inv.ean_num::text)
            NOT IN (
                SELECT inv_data.invnt_dt::character varying || inv_data.dstr_cd::text || inv_data.prod_cd::text || inv_data.ean_num::text
                FROM inv_data
            )
        )
),
a as
(
    SELECT * FROM inv_data
    UNION ALL
    SELECT * FROM not_in_inv_data_cte
),
f as 
(
    SELECT 
        DISTINCT ean::character varying(100) AS ean_num,
        cntry,
        sap_matl_num,
        prod_hier_l1,
        prod_hier_l2,
        prod_hier_l3,
        prod_hier_l4,
        prod_hier_l5,
        prod_hier_l6,
        prod_hier_l7,
        prod_hier_l8,
        prod_hier_l9,
        lcl_prod_nm
    FROM edw_product_attr_dim
),
h as
(
    SELECT 
        edw_customer_attr_flat_dim.sold_to_party,
        MAX(edw_customer_attr_flat_dim.sls_grp::text) AS sls_grp,
        MAX(edw_customer_attr_flat_dim.store_typ::text) AS store_typ
    FROM edw_customer_attr_flat_dim
    GROUP BY edw_customer_attr_flat_dim.sold_to_party
),
k as 
(
    SELECT 
        DISTINCT dstr_cd,
        "replace"(
                        "replace"(
                            "replace"(
                                "replace"(
                                    (dstr_cust_cd)::text,
                                    ('Indirect'::character varying)::text,
                                    (''::character varying)::text
                                ),
                                ('Direct'::character varying)::text,
                                (''::character varying)::text
                            ),
                            ('Indi'::character varying)::text,
                            (''::character varying)::text
                        ),
                        ('Dire'::character varying)::text,
                        (''::character varying)::text
                    ) AS dstr_cust_cd,
        dstr_cust_clsn1,
        dstr_cust_clsn2
    FROM itg_ims_dstr_cust_attr
),
final as 
(   
    SELECT
        a.invnt_dt,
        a.dstr_cd,
        a.dstr_cd::text || ' - '::character varying || a.dstr_nm::text AS dstr_nm,
        a.cust_nm AS cust_name,
        COALESCE(k.dstr_cust_clsn1, 'Not Available'::character varying) AS channel1,
        COALESCE(k.dstr_cust_clsn2, 'Not Available'::character varying) AS channel2,
        b.fisc_per,
        b.cal_wk AS fisc_wk,
        b.no_of_wks,
        b.fisc_wk_num,
        a.prod_cd,
        a.prod_nm,
        a.ean_num,
        SUM(a.invnt_qty) AS invnt_qty,
        SUM(a.invnt_amt) AS invnt_amt,
        COALESCE(COALESCE(a.sls_rep_nm, a.sls_rep_cd), 'Not Available'::character varying) AS sls_rep_nm,
        a.sls_rep_cd,
        CASE
            WHEN a.ctry_cd::text = 'KR'::character varying::text THEN 'South Korea'::character varying
            WHEN a.ctry_cd::text = 'TW'::character varying::text THEN 'Taiwan'::character varying
            WHEN a.ctry_cd::text = 'HK'::character varying::text THEN 'Hong Kong'::character varying
            ELSE NULL::character varying
        END AS ctry_nm,
        a.crncy_cd AS from_crncy,
        g.to_crncy,
        a.chn_uom,
        g.ex_rt,
        CASE
            WHEN a.ctry_cd::text = 'TW'::character varying::text AND (f.prod_hier_l1 IS NULL OR f.prod_hier_l1::text = '') THEN 'Taiwan'::character varying
            WHEN a.ctry_cd::text = 'KR'::character varying::text AND (f.prod_hier_l1 IS NULL OR f.prod_hier_l1::text = '') THEN 'Korea'::character varying
            WHEN a.ctry_cd::text = 'HK'::character varying::text AND (f.prod_hier_l1 IS NULL OR f.prod_hier_l1::text = '') THEN 'HK'::character varying
            ELSE COALESCE(f.prod_hier_l1, '#'::character varying)
        END AS prod_hier_l1,
        COALESCE(f.prod_hier_l2, '#'::character varying) AS prod_hier_l2,
        COALESCE(f.prod_hier_l3, '#'::character varying) AS prod_hier_l3,
        COALESCE(f.prod_hier_l4, '#'::character varying) AS prod_hier_l4,
        COALESCE(f.prod_hier_l5, '#'::character varying) AS prod_hier_l5,
        COALESCE(f.prod_hier_l6, '#'::character varying) AS prod_hier_l6,
        COALESCE(f.prod_hier_l7, '#'::character varying) AS prod_hier_l7,
        COALESCE(f.prod_hier_l8, '#'::character varying) AS prod_hier_l8,
        COALESCE(f.prod_hier_l9, '#'::character varying) AS prod_hier_l9,
        f.sap_matl_num,
        COALESCE(f.lcl_prod_nm, '#'::character varying) AS lcl_prod_nm,
        COALESCE(h.sls_grp, 'Not Available'::character varying) AS sls_grp,
        CASE
            WHEN h.store_typ IS NULL OR h.store_typ::character varying::text = ''::character varying::text THEN 'Not Available'::character varying
            ELSE h.store_typ
        END AS store_typ,
        CASE
            WHEN a.prod_cd::text like'1U%'::character varying::text OR a.prod_cd::text like'COUNTER TOP%'::character varying::text OR a.prod_cd::text like'DUMPBIN\012\012%'::character varying::text OR a.prod_cd IS NULL OR a.prod_cd::text = ''::character varying::text THEN 'non sellable products'::character varying
            ELSE 'sellable products'::character varying
        END AS non_sellable_product,
        a.sell_out_price_manual AS sell_in_price_manual,
        a.storage_name,
        a.area
    FROM a
        LEFT JOIN v_intrm_calendar_ims b ON b.cal_day = a.invnt_dt
        LEFT JOIN f ON f.ean_num::text = a.ean_num::text AND f.cntry::text = a.ctry_cd::text
        LEFT JOIN h ON a.dstr_cd::text = h.sold_to_party::text
        LEFT JOIN k ON k.dstr_cd::text = a.dstr_cd::text AND k.dstr_cust_cd = a.cust_nm::text
        LEFT JOIN v_intrm_crncy_exch g ON a.crncy_cd::text = g.from_crncy::text
    WHERE date_part(year, a.invnt_dt::timestamp without time zone) > 
        (date_part(year, convert_timezone('UTC', current_timestamp())::timestamp without time zone) - 3::double precision)
    GROUP BY 
        a.invnt_dt,
        a.dstr_cd,
        a.dstr_nm,
        a.cust_nm,
        k.dstr_cust_clsn1,
        k.dstr_cust_clsn2,
        b.fisc_per,
        b.cal_wk,
        b.no_of_wks,
        b.fisc_wk_num,
        a.prod_cd,
        a.prod_nm,
        a.ean_num,
        a.invnt_amt,
        a.invnt_qty,
        a.sls_rep_cd,
        a.sls_rep_nm,
        a.ctry_cd,
        a.crncy_cd,
        g.to_crncy,
        a.chn_uom,
        g.ex_rt,
        f.prod_hier_l1,
        f.prod_hier_l2,
        f.prod_hier_l3,
        f.prod_hier_l4,
        f.prod_hier_l5,
        f.prod_hier_l6,
        f.prod_hier_l7,
        f.prod_hier_l8,
        f.prod_hier_l9,
        f.sap_matl_num,
        f.lcl_prod_nm,
        h.sls_grp,
        h.store_typ,
        a.sell_out_price_manual,
        a.storage_name,
        a.area
)
select * from final