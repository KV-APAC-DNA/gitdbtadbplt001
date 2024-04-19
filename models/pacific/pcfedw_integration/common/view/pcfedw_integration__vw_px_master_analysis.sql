with px_combined_ciw_fact as
(
    select * from {{ ref('pcfedw_integration__px_combined_ciw_fact') }}
),
edw_px_gl_trans_lkp as
(
    select * from {{ ref('pcfedw_integration__edw_px_gl_trans_lkp') }}
),
edw_px_master_fact as
(
    select * from {{ ref('pcfedw_integration__edw_px_master_fact') }}
),
vw_customer_dim as
(
    select * from {{ ref('pcfedw_integration__vw_customer_dim') }}
),
vw_material_dim as
(
    select * from {{ ref('pcfedw_integration__vw_material_dim') }}
),
edw_time_dim as
(
    select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
final as
(
    SELECT etd.cal_date,
        etd.time_id,
        etd.jj_wk,
        etd.jj_mnth,
        etd.jj_mnth_shrt,
        etd.jj_mnth_long,
        etd.jj_qrtr,
        etd.jj_year,
        etd.cal_mnth_id,
        etd.jj_mnth_id,
        etd.cal_mnth,
        etd.cal_qrtr,
        etd.cal_year,
        etd.jj_mnth_tot,
        etd.jj_mnth_day,
        etd.cal_mnth_nm,
        vcd.cust_no,
        vcd.cmp_id,
        vcd.channel_cd,
        vcd.channel_desc,
        vcd.ctry_key,
        vcd.country,
        vcd.state_cd,
        vcd.post_cd,
        vcd.cust_suburb,
        vcd.cust_nm,
        vcd.sls_org,
        vcd.cust_del_flag,
        vcd.sales_office_cd,
        vcd.sales_office_desc,
        vcd.sales_grp_cd,
        vcd.sales_grp_desc,
        vcd.mercia_ref,
        vcd.curr_cd,
        vmd.matl_id,
        vmd.matl_desc,
        vmd.mega_brnd_cd,
        vmd.mega_brnd_desc,
        vmd.brnd_cd,
        vmd.brnd_desc,
        vmd.base_prod_cd,
        vmd.base_prod_desc,
        vmd.variant_cd,
        vmd.variant_desc,
        vmd.fran_cd,
        vmd.fran_desc,
        vmd.grp_fran_cd,
        vmd.grp_fran_desc,
        vmd.matl_type_cd,
        vmd.matl_type_desc,
        vmd.prod_fran_cd,
        vmd.prod_fran_desc,
        vmd.prod_hier_cd,
        vmd.prod_hier_desc,
        vmd.prod_mjr_cd,
        vmd.prod_mjr_desc,
        vmd.prod_mnr_cd,
        vmd.prod_mnr_desc,
        vmd.mercia_plan,
        vmd.putup_cd,
        vmd.putup_desc,
        vmd.bar_cd,
        vmd.updt_dt,
        epmf.ac_code,
        epmf.ac_longname,
        epmf.p_promonumber,
        epmf.p_startdate,
        epmf.p_stopdate,
        epmf.promo_length,
        epmf.promotionforecastweek,
        epmf.p_buystartdatedef,
        epmf.p_buystopdatedef,
        epmf.buyperiod_length,
        epmf.hierarchy_rowid,
        epmf.hierarchy_longname,
        epmf.activity_longname,
        CASE
            WHEN (epmf.confirmed_switch = 1) THEN 'Confirmed'::character varying
            WHEN (epmf.confirmed_switch = 0) THEN 'Unconfirmed'::character varying
            ELSE 'Pending'::character varying
        END AS confirmed_switch,
        CASE
            WHEN (epmf.closed_switch = 1) THEN 'Closed'::character varying
            WHEN (epmf.closed_switch = 0) THEN 'Open'::character varying
            ELSE NULL::character varying
        END AS closed_switch,
        epmf.sku_longname,
        epmf.sku_profitcentre,
        epmf.sku_attribute,
        epmf.gltt_rowid,
        epmf.transaction_longname,
        epmf.case_deal,
        epmf.case_quantity,
        epmf.planspend_total,
        epmf.paid_total,
        (
            CASE
                WHEN (epmf.closed_switch = 1) THEN epmf.paid_total
                ELSE CASE
                    WHEN (epmf.planspend_total > epmf.paid_total) THEN epmf.planspend_total
                    ELSE epmf.paid_total
                END
            END - epmf.paid_total
        ) AS open_total,
        CASE
            WHEN (epmf.closed_switch = 1) THEN epmf.paid_total
            ELSE CASE
                WHEN (epmf.planspend_total > epmf.paid_total) THEN epmf.planspend_total
                ELSE epmf.paid_total
            END
        END AS committed_spend,
        CASE
            WHEN (epmf.p_deleted = 1) THEN 'Yes'::character varying
            WHEN (epmf.p_deleted = 0) THEN 'No'::character varying
            ELSE NULL::character varying
        END AS p_deleted,
        epmf.local_ccy,
        epmf.aud_rate,
        epmf.sgd_rate,
        epgm.sap_account,
        cpf.sap_accnt_nm,
        epgm.promax_measure,
        epgm.promax_bucket,
        epmf.promotionrowid
    FROM
        (
            SELECT
                DISTINCT px_combined_ciw_fact.sap_accnt,
                px_combined_ciw_fact.sap_accnt_nm
            FROM px_combined_ciw_fact
        ) cpf,
        edw_px_gl_trans_lkp epgm,
        edw_px_master_fact epmf
        LEFT JOIN vw_customer_dim vcd
        ON (epmf.cust_id)::text = ltrim((vcd.cust_no)::text,('0'::character varying)::text)
        LEFT JOIN vw_material_dim vmd
        ON (epmf.matl_id)::text = ltrim((vmd.matl_id)::text,('0'::character varying)::text)
        LEFT JOIN edw_time_dim etd
        ON to_date(epmf.promotionforecastweek) = to_date(etd.cal_date)
    WHERE
        (epmf.gltt_rowid = epgm.row_id)
        AND ((epgm.sap_account)::text = (cpf.sap_accnt)::text)
)
select * from final
