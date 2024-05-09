with 
vw_jjbr_curr_exch_dim as 
(
    select * from {{ ref('pcfedw_integration__vw_jjbr_curr_exch_dim') }}
),
edw_time_dim as 
(
    select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
itg_px_master as 
(
    select * from {{ ref('pcfitg_integration__itg_px_master') }}
),
edw_vw_terms_master as 
(
    select * from {{ ref('pcfedw_integration__edw_vw_terms_master') }}
),
vw_customer_dim as 
(
    select * from {{ ref('pcfedw_integration__vw_customer_dim') }}
),
vw_material_dim as 
(
    select * from {{ ref('pcfedw_integration__vw_material_dim') }}
),
itg_px_weekly_sell as 
(
    select * from {{ ref('pcfitg_integration__itg_px_weekly_sell') }}
),
edw_px_listprice as 
(
    select * from {{ ref('pcfedw_integration__edw_px_listprice') }}
),
dly_sls_cust_attrb_lkp as 
(
    select * from {{ ref('pcfedw_integration__dly_sls_cust_attrb_lkp') }}
),
vw_sap_std_cost as 
(
    select * from {{ ref('pcfedw_integration__vw_sap_std_cost') }}
),
edw_vw_mds_cogs_rate_dim as 
(
    select * from {{ ref('pcfedw_integration__edw_vw_mds_cogs_rate_dim') }}
),
itg_query_parameters as 
(
    select * from {{ source('pcfitg_integration', 'itg_query_parameters') }}
),
final as
(
SELECT a.cmp_id,
    a.country,
    a.cust_no,
    a.promo_number,
    a.px_jj_year,
    a.px_jj_mnth,
    a.matl_id,
    a.sku_name,
    a.hierarchy_longname,
    a.promo_name,
    a.promo_start_date::date as promo_start_date,
    a.promo_stop_date::date as promo_stop_date,
    a.local_ccy,
    a.cust_nm,
    a.channel_desc,
    a.sales_office_desc,
    a.sales_grp_desc,
    a.matl_desc,
    a.mega_brnd_desc,
    a.brnd_desc,
    a.variant_desc,
    a.fran_desc,
    a.grp_fran_desc,
    a.prod_fran_desc,
    a.prod_hier_desc,
    a.prod_major_desc,
    a.prod_minor_desc,
    a.px_base_qty,
    a.px_base_gts,
    a.px_promo_qty,
    a.px_promo_gts,
    a.px_total_qty,
    a.px_total_gts,
    COALESCE(
        (
            (a.px_base_qty)::numeric * (
                COALESCE(b.std_cost_aud, (0)::numeric) * ((1)::numeric / d.exch_rate)
            )
        ),
        (0)::numeric
    ) AS px_base_cogs,
    COALESCE(
        (
            (a.px_promo_qty)::numeric * (
                COALESCE(b.std_cost_aud, (0)::numeric) * ((1)::numeric / d.exch_rate)
            )
        ),
        (0)::numeric
    ) AS px_promo_cogs,
    COALESCE(
        (
            (a.px_total_qty)::numeric * (
                COALESCE(b.std_cost_aud, (0)::numeric) * ((1)::numeric / d.exch_rate)
            )
        ),
        (0)::numeric
    ) AS px_total_cogs,
    a.px_base_terms,
    a.px_promo_terms,
    a.px_total_terms,
    a.case_qty,
    a.total_plan_spend,
    a.total_paid,
    a.committed_spend,
    e.cogs_per_unit AS finance_cogs
FROM vw_jjbr_curr_exch_dim d,
    (
        (
            (
                SELECT derived_table2.cmp_id,
                    derived_table2.country,
                    derived_table2.cust_no,
                    derived_table2.promo_number,
                    derived_table2.px_jj_year,
                    derived_table2.px_jj_mnth,
                    derived_table2.matl_id,
                    derived_table2.promo_start_date,
                    derived_table2.promo_stop_date,
                    derived_table2.hierarchy_longname,
                    derived_table2.promo_name,
                    derived_table2.sku_name,
                    derived_table2.local_ccy,
                    derived_table2.cust_nm,
                    derived_table2.channel_desc,
                    derived_table2.sales_office_desc,
                    derived_table2.sales_grp_desc,
                    derived_table2.matl_desc,
                    derived_table2.mega_brnd_desc,
                    derived_table2.brnd_desc,
                    derived_table2.variant_desc,
                    derived_table2.fran_desc,
                    derived_table2.grp_fran_desc,
                    derived_table2.prod_fran_desc,
                    derived_table2.prod_hier_desc,
                    derived_table2.prod_major_desc,
                    derived_table2.prod_minor_desc,
                    "max"(derived_table2.case_qty) AS case_qty,
                    "max"(derived_table2.total_plan_spend) AS total_plan_spend,
                    "max"(derived_table2.total_paid) AS total_paid,
                    "max"(derived_table2.committed_spend) AS committed_spend,
                    "max"(derived_table2.px_base_qty) AS px_base_qty,
                    "max"(derived_table2.px_base_gts) AS px_base_gts,
                    "max"(derived_table2.px_promo_qty) AS px_promo_qty,
                    "max"(derived_table2.px_promo_gts) AS px_promo_gts,
                    "max"(derived_table2.px_total_qty) AS px_total_qty,
                    "max"(derived_table2.px_total_gts) AS px_total_gts,
                    sum(derived_table2.px_base_terms) AS px_base_terms,
                    sum(derived_table2.px_promo_terms) AS px_promo_terms,
                    sum(derived_table2.px_total_terms) AS px_total_terms
                FROM (
                        SELECT epf.cmp_id,
                            epf.country,
                            epf.cust_no,
                            epf.promo_number,
                            etd.jj_year AS px_jj_year,
                            etd.jj_mnth_id AS px_jj_mnth,
                            epf.matl_id,
                            epf.promo_start_date,
                            epf.promo_stop_date,
                            epf.hierarchy_longname,
                            epf.promo_name,
                            epf.sku_name,
                            epf.local_ccy,
                            epf.cust_nm,
                            epf.channel_desc,
                            epf.sales_office_desc,
                            epf.sales_grp_desc,
                            epf.matl_desc,
                            epf.mega_brnd_desc,
                            epf.brnd_desc,
                            epf.variant_desc,
                            epf.fran_desc,
                            epf.grp_fran_desc,
                            epf.prod_fran_desc,
                            epf.prod_hier_desc,
                            epf.prod_major_desc,
                            epf.prod_minor_desc,
                            epf.case_qty,
                            epf.total_plan_spend,
                            epf.total_paid,
                            epf.committed_spend,
                            COALESCE(epf.normal_qty, (0)::bigint) AS px_base_qty,
                            COALESCE(
                                (
                                    (epf.normal_qty)::double precision * epl.lp_price
                                ),
                                (0)::double precision
                            ) AS px_base_gts,
                            COALESCE(epf.promotional_qty, (0)::bigint) AS px_promo_qty,
                            COALESCE(
                                (
                                    (epf.promotional_qty)::double precision * epl.lp_price
                                ),
                                (0)::double precision
                            ) AS px_promo_gts,
                            COALESCE(epf.estimate_qty, (0)::bigint) AS px_total_qty,
                            COALESCE(
                                (
                                    (epf.estimate_qty)::double precision * epl.lp_price
                                ),
                                (0)::double precision
                            ) AS px_total_gts,
                            COALESCE(
                                (
                                    (
                                        (epf.normal_qty)::double precision * epl.lp_price
                                    ) * (epf.terms_percentage)::double precision
                                ),
                                (0)::double precision
                            ) AS px_base_terms,
                            COALESCE(
                                (
                                    (
                                        (epf.promotional_qty)::double precision * epl.lp_price
                                    ) * (epf.terms_percentage)::double precision
                                ),
                                (0)::double precision
                            ) AS px_promo_terms,
                            COALESCE(
                                (
                                    (
                                        (epf.estimate_qty)::double precision * epl.lp_price
                                    ) * (epf.terms_percentage)::double precision
                                ),
                                (0)::double precision
                            ) AS px_total_terms
                        FROM edw_time_dim etd,
                            (
                                (
                                    SELECT pmf.ac_attribute AS cust_no,
                                        pmf.p_promonumber AS promo_number,
                                        (pmf.p_startdate) AS promo_start_date,
                                        (pmf.p_stopdate) AS promo_stop_date,
                                        pmf.hierarchy_longname,
                                        pmf.activity_longname AS promo_name,
                                        pmf.sku_longname AS sku_name,
                                        pmf.sku_stockcode AS matl_id,
                                        vcd.curr_cd AS local_ccy,
                                        pmf.estimate_qty AS case_qty,
                                        pws.planspend_total AS total_plan_spend,
                                        pws.paid_total AS total_paid,
                                        CASE
                                            WHEN (
                                                upper((pws.promotionitemstatus)::text) = 'CLOSED'::text
                                            ) THEN pws.paid_total
                                            ELSE CASE
                                                WHEN (pws.planspend_total > pws.paid_total) THEN pws.planspend_total
                                                ELSE pws.paid_total
                                            END
                                        END AS committed_spend,
                                        pmf.normal_qty,
                                        pmf.promotional_qty,
                                        pmf.estimate_qty,
                                        px_terms.terms_percentage,
                                        vcd.cmp_id,
                                        vcd.country,
                                        vcd.cust_nm,
                                        vcd.channel_desc,
                                        vcd.sales_office_desc,
                                        vcd.sales_grp_desc,
                                        vmd.matl_desc,
                                        vmd.mega_brnd_desc,
                                        vmd.brnd_desc,
                                        vmd.variant_desc,
                                        vmd.fran_desc,
                                        vmd.grp_fran_desc,
                                        vmd.prod_fran_desc,
                                        vmd.prod_hier_desc,
                                        vmd.prod_mjr_desc AS prod_major_desc,
                                        vmd.prod_mnr_desc AS prod_minor_desc
                                    FROM (
                                            (
                                                (
                                                    (
                                                        (
                                                            SELECT itg_px_master.ac_code,
                                                                itg_px_master.ac_longname,
                                                                itg_px_master.ac_attribute,
                                                                itg_px_master.p_promonumber,
                                                                itg_px_master.p_startdate,
                                                                itg_px_master.p_stopdate,
                                                                itg_px_master.promo_length,
                                                                itg_px_master.p_buystartdatedef,
                                                                itg_px_master.p_buystopdatedef,
                                                                itg_px_master.buyperiod_length,
                                                                itg_px_master.hierarchy_rowid,
                                                                itg_px_master.hierarchy_longname,
                                                                itg_px_master.activity_longname,
                                                                itg_px_master.confirmed_switch,
                                                                itg_px_master.closed_switch,
                                                                itg_px_master.sku_longname,
                                                                itg_px_master.sku_stockcode,
                                                                itg_px_master.sku_profitcentre,
                                                                itg_px_master.sku_attribute,
                                                                sum(itg_px_master.case_deal) AS case_deal,
                                                                sum(itg_px_master.planspend_total) AS planspend_total,
                                                                sum(itg_px_master.paid_total) AS paid_total,
                                                                sum(itg_px_master.p_deleted) AS p_deleted,
                                                                itg_px_master.transaction_attribute,
                                                                itg_px_master.promotionrowid,
                                                                sum(itg_px_master.case_quantity) AS estimate_qty,
                                                                avg(itg_px_master.normal_qty) AS normal_qty,
                                                                (
                                                                    sum(COALESCE(itg_px_master.case_quantity, 0)) - avg(COALESCE(itg_px_master.normal_qty, 0))
                                                                ) AS promotional_qty
                                                            FROM itg_px_master
                                                            GROUP BY itg_px_master.ac_code,
                                                                itg_px_master.ac_longname,
                                                                itg_px_master.ac_attribute,
                                                                itg_px_master.p_promonumber,
                                                                itg_px_master.p_startdate,
                                                                itg_px_master.p_stopdate,
                                                                itg_px_master.promo_length,
                                                                itg_px_master.p_buystartdatedef,
                                                                itg_px_master.p_buystopdatedef,
                                                                itg_px_master.buyperiod_length,
                                                                itg_px_master.hierarchy_rowid,
                                                                itg_px_master.hierarchy_longname,
                                                                itg_px_master.activity_longname,
                                                                itg_px_master.confirmed_switch,
                                                                itg_px_master.closed_switch,
                                                                itg_px_master.sku_longname,
                                                                itg_px_master.sku_stockcode,
                                                                itg_px_master.sku_profitcentre,
                                                                itg_px_master.sku_attribute,
                                                                itg_px_master.transaction_attribute,
                                                                itg_px_master.promotionrowid
                                                        ) pmf
                                                        LEFT JOIN edw_vw_terms_master px_terms ON (
                                                            (
                                                                (
                                                                    ltrim((px_terms.cust_id)::text, '0'::text) = ltrim((pmf.ac_attribute)::text, '0'::text)
                                                                )
                                                            )
                                                        )
                                                    )
                                                    LEFT JOIN vw_customer_dim vcd ON (
                                                        (
                                                            (
                                                                (pmf.ac_attribute)::text = ltrim((vcd.cust_no)::text, '0'::text)
                                                            )
                                                        )
                                                    )
                                                )
                                                LEFT JOIN vw_material_dim vmd ON (
                                                    (
                                                        (
                                                            (pmf.sku_stockcode)::text = ltrim((vmd.matl_id)::text, '0'::text)
                                                        )
                                                    )
                                                )
                                            )
                                            LEFT JOIN (
                                                SELECT itg_px_weekly_sell.promotionrowid,
                                                    itg_px_weekly_sell.sku_stockcode,
                                                    itg_px_weekly_sell.promotionitemstatus,
                                                    sum(itg_px_weekly_sell.paid_total) AS paid_total,
                                                    sum(itg_px_weekly_sell.planspend_total) AS planspend_total
                                                FROM itg_px_weekly_sell
                                                GROUP BY itg_px_weekly_sell.promotionrowid,
                                                    itg_px_weekly_sell.sku_stockcode,
                                                    itg_px_weekly_sell.promotionitemstatus
                                            ) pws ON (
                                                (
                                                    (
                                                        COALESCE(pmf.promotionrowid, -999999) = COALESCE(pws.promotionrowid, -999999)
                                                    )
                                                    AND (
                                                        (
                                                            COALESCE(pmf.sku_stockcode, '-999999'::character varying)
                                                        )::text = (
                                                            COALESCE(pws.sku_stockcode, '-999999'::character varying)
                                                        )::text
                                                    )
                                                )
                                            )
                                        )
                                ) epf
                                LEFT JOIN (
                                    SELECT derived_table1.sku_stockcode,
                                        derived_table1.cmp_id,
                                        derived_table1.lp_price
                                    FROM (
                                            SELECT DISTINCT lp.sku_stockcode,
                                                lkp.cmp_id,
                                                lp.lp_price,
                                                rank() OVER(
                                                    PARTITION BY lp.sku_stockcode,
                                                    lkp.cmp_id
                                                    ORDER BY lp.lp_startdate DESC
                                                ) AS rank_number
                                            FROM edw_px_listprice lp,
                                                (
                                                    SELECT DISTINCT dly_sls_cust_attrb_lkp.cmp_id,
                                                        dly_sls_cust_attrb_lkp.sls_org
                                                    FROM dly_sls_cust_attrb_lkp
                                                ) lkp
                                            WHERE (
                                                    (
                                                        (lp.sales_org = (lkp.sls_org)::character(20))
                                                        AND (
                                                            (lp.lp_startdate) <= ((current_timestamp()::text)::timestamp without time zone)
                                                        )
                                                    )
                                                    AND (
                                                        (
                                                            (lp.lp_stopdate) > ((current_timestamp()::text)::timestamp without time zone)
                                                        )
                                                        OR ((lp.lp_stopdate) IS NULL)
                                                    )
                                                )
                                        ) derived_table1
                                    WHERE (derived_table1.rank_number = 1)
                                ) epl ON (
                                    (
                                        ((epf.matl_id)::text = (epl.sku_stockcode)::text)
                                        AND ((epf.cmp_id)::text = (epl.cmp_id)::text)
                                    )
                                )
                            )
                        WHERE (
                                (etd.cal_date) = (
                                    (epf.promo_start_date)::timestamp without time zone
                                )
                            )
                    ) derived_table2
                GROUP BY derived_table2.cmp_id,
                    derived_table2.country,
                    derived_table2.cust_no,
                    derived_table2.promo_number,
                    derived_table2.px_jj_year,
                    derived_table2.px_jj_mnth,
                    derived_table2.matl_id,
                    derived_table2.promo_start_date,
                    derived_table2.promo_stop_date,
                    derived_table2.hierarchy_longname,
                    derived_table2.promo_name,
                    derived_table2.sku_name,
                    derived_table2.local_ccy,
                    derived_table2.cust_nm,
                    derived_table2.channel_desc,
                    derived_table2.sales_office_desc,
                    derived_table2.sales_grp_desc,
                    derived_table2.matl_desc,
                    derived_table2.mega_brnd_desc,
                    derived_table2.brnd_desc,
                    derived_table2.variant_desc,
                    derived_table2.fran_desc,
                    derived_table2.grp_fran_desc,
                    derived_table2.prod_fran_desc,
                    derived_table2.prod_hier_desc,
                    derived_table2.prod_major_desc,
                    derived_table2.prod_minor_desc
            ) a
            LEFT JOIN vw_sap_std_cost b ON (
                (
                    (
                        (a.matl_id)::text = ltrim((b.matnr)::text, '0'::text)
                    )
                    AND ((a.cmp_id)::text = (b.cmp_no)::text)
                )
            )
        )
        LEFT JOIN edw_vw_mds_cogs_rate_dim e ON (
            (
                ((a.matl_id)::text = (e.matl_id)::text)
                AND (a.px_jj_mnth = e.jj_mnth_id)
                AND (a.local_ccy = (e.crncy)::text)
            )
        )
    )
WHERE (
        (
            (
                ((d.to_ccy)::text = 'AUD'::text)
                AND (a.px_jj_mnth = d.jj_mnth_id)
            )
            AND (a.local_ccy = (d.from_ccy)::text)
        )
        AND (
            (a.px_jj_year)::double precision >= (
                date_part(
                    YEAR,
                    (current_timestamp())
                ) - (
                    (
                        SELECT (itg_query_parameters.parameter_value)::integer AS parameter_value
                        FROM itg_query_parameters
                        WHERE (
                                (
                                    (itg_query_parameters.parameter_name)::text = 'PROMO_ROI_TDE_DATA_RETENTION_YEARS'::text
                                )
                                AND (
                                    (itg_query_parameters.country_code)::text = 'ANZ'::text
                                )
                            )
                    )
                )::double precision
            )
        )
    )
)
select * from final