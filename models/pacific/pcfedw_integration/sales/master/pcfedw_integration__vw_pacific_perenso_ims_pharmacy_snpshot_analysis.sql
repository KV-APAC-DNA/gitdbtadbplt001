with
edw_time_dim as
(
    select * from snappcfedw_integration.edw_time_dim
),--used as source
vw_jjbr_curr_exch_dim as
(
    select * from snappcfedw_integration.vw_jjbr_curr_exch_dim
),
vw_bwar_curr_exch_dim as
(
    select * from snappcfedw_integration.vw_bwar_curr_exch_dim
),
edw_perenso_order_fact as
(
    select * from snappcfedw_integration.edw_perenso_order_fact
),
edw_perenso_prod_dim as
(
    select * from snappcfedw_integration.edw_perenso_prod_dim
),
edw_perenso_account_dim_snapshot as
(
    select * from snappcfedw_integration.edw_perenso_account_dim_snapshot
),
final as
( 
select * from 
(
(
    (
        SELECT epof.delvry_dt,
            epad.snapshot_dt,
            etd.jj_year,
            etd.jj_qrtr,
            etd.jj_mnth_id,
            etd.jj_mnth_shrt,
            epof.acct_key,
            epad.acct_banner_division,
            epad.acct_banner,
            epad.acct_display_name,
            CASE
                WHEN (
                    upper((epad.acct_ind_groc_state)::text) <> 'NOT ASSIGNED'::text
                ) THEN epad.acct_ind_groc_state
                WHEN (
                    upper((epad.acct_au_pharma_state)::text) <> 'NOT ASSIGNED'::text
                ) THEN epad.acct_au_pharma_state
                WHEN (
                    upper((epad.acct_nz_pharma_state)::text) <> 'NOT ASSIGNED'::text
                ) THEN epad.acct_nz_pharma_state
                ELSE 'NOT ASSIGNED'::character varying
            END AS acct_tsm,
            CASE
                WHEN (
                    upper((epad.acct_ind_groc_territory)::text) <> 'NOT ASSIGNED'::text
                ) THEN epad.acct_ind_groc_territory
                WHEN (
                    upper((epad.acct_au_pharma_territory)::text) <> 'NOT ASSIGNED'::text
                ) THEN epad.acct_au_pharma_territory
                WHEN (
                    upper((epad.acct_nz_pharma_territory)::text) <> 'NOT ASSIGNED'::text
                ) THEN epad.acct_nz_pharma_territory
                ELSE 'NOT ASSIGNED'::character varying
            END AS acct_terriroty,
            eppd.prod_id AS prod_sapbw_code,
            eppd.prod_desc,
            eppd.prod_jj_brand,
            eppd.prod_ean,
            eppd.prod_jj_franchise,
            eppd.prod_jj_category,
            epof.unit_qty,
            (epof.entered_qty * epof.nis) AS nis,
            ((epof.entered_qty * epof.nis) * jjbr.exch_rate) AS aud_nis,
            ((epof.entered_qty * epof.nis) * bwar.exch_rate) AS usd_nis
        FROM (
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
                    etdw.jj_mnth_wk,
                    etdc.cal_wk,
                    etdcm.cal_mnth_wk
                FROM edw_time_dim etd,
                    (
                        SELECT etd.jj_year,
                            etd.jj_mnth_id,
                            etd.jj_wk,
                            row_number() OVER(
                                PARTITION BY etd.jj_year,
                                etd.jj_mnth_id
                                ORDER BY etd.jj_year,
                                    etd.jj_mnth_id,
                                    etd.jj_wk
                            ) AS jj_mnth_wk
                        FROM (
                                SELECT DISTINCT etd.jj_year,
                                    etd.jj_mnth_id,
                                    etd.jj_wk
                                FROM edw_time_dim etd
                            ) etd
                    ) etdw,
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
                            CASE
                                WHEN (
                                    (
                                        row_number() OVER(
                                            PARTITION BY etd.cal_year
                                            ORDER BY (etd.cal_date::date)
                                        ) % (7)::bigint
                                    ) = 0
                                ) THEN (
                                    row_number() OVER(
                                        PARTITION BY etd.cal_year
                                        ORDER BY (etd.cal_date::date)
                                    ) / 7
                                )
                                ELSE (
                                    (
                                        row_number() OVER(
                                            PARTITION BY etd.cal_year
                                            ORDER BY (etd.cal_date::date)
                                        ) / 7
                                    ) + 1
                                )
                            END AS cal_wk
                        FROM edw_time_dim etd
                    ) etdc,
                    (
                        SELECT etdcw.cal_year,
                            etdcw.cal_mnth_id,
                            etdcw.cal_wk,
                            row_number() OVER(
                                PARTITION BY etdcw.cal_year,
                                etdcw.cal_mnth_id
                                ORDER BY etdcw.cal_year,
                                    etdcw.cal_mnth_id,
                                    etdcw.cal_wk
                            ) AS cal_mnth_wk
                        FROM (
                                SELECT DISTINCT etdc.cal_year,
                                    etdc.cal_mnth_id,
                                    etdc.cal_wk
                                FROM (
                                        SELECT etd.cal_year,
                                            etd.cal_mnth_id,
                                            CASE
                                                WHEN (
                                                    (
                                                        row_number() OVER(
                                                            PARTITION BY etd.cal_year
                                                            ORDER BY (etd.cal_date::date)
                                                        ) % (7)::bigint
                                                    ) = 0
                                                ) THEN (
                                                    row_number() OVER(
                                                        PARTITION BY etd.cal_year
                                                        ORDER BY (etd.cal_date::date)
                                                    ) / 7
                                                )
                                                ELSE (
                                                    (
                                                        row_number() OVER(
                                                            PARTITION BY etd.cal_year
                                                            ORDER BY (etd.cal_date::date)
                                                        ) / 7
                                                    ) + 1
                                                )
                                            END AS cal_wk
                                        FROM edw_time_dim etd
                                    ) etdc
                            ) etdcw
                    ) etdcm
                WHERE (
                        (
                            (
                                (
                                    (
                                        (
                                            (etd.jj_year = etdw.jj_year)
                                            AND (etd.jj_mnth_id = etdw.jj_mnth_id)
                                        )
                                        AND (etd.jj_wk = etdw.jj_wk)
                                    )
                                    AND ((etd.cal_date::date) = (etdc.cal_date::date))
                                )
                                AND (etdc.cal_wk = etdcm.cal_wk)
                            )
                            AND (etdc.cal_year = etdcm.cal_year)
                        )
                        AND (etdc.cal_mnth_id = etdcm.cal_mnth_id)
                    )
            ) etd,
            (
                SELECT vw_jjbr_curr_exch_dim.rate_type,
                    vw_jjbr_curr_exch_dim.from_ccy,
                    vw_jjbr_curr_exch_dim.to_ccy,
                    vw_jjbr_curr_exch_dim.jj_mnth_id,
                    vw_jjbr_curr_exch_dim.exch_rate
                FROM vw_jjbr_curr_exch_dim
                WHERE (
                        (vw_jjbr_curr_exch_dim.to_ccy)::text = 'AUD'::text
                    )
            ) jjbr,
            (
                SELECT vw_bwar_curr_exch_dim.rate_type,
                    vw_bwar_curr_exch_dim.from_ccy,
                    vw_bwar_curr_exch_dim.to_ccy,
                    vw_bwar_curr_exch_dim.jj_mnth_id,
                    vw_bwar_curr_exch_dim.exch_rate
                FROM vw_bwar_curr_exch_dim
                WHERE (
                        (vw_bwar_curr_exch_dim.to_ccy)::text = 'USD'::text
                    )
            ) bwar,
            (
                (
                    (
                        SELECT edw_perenso_order_fact.order_key,
                            edw_perenso_order_fact.order_type_key,
                            edw_perenso_order_fact.order_type_desc,
                            edw_perenso_order_fact.acct_key,
                            edw_perenso_order_fact.order_date,
                            edw_perenso_order_fact.order_header_status_key,
                            edw_perenso_order_fact.charge,
                            edw_perenso_order_fact.confirmation,
                            edw_perenso_order_fact.diary_item_key,
                            edw_perenso_order_fact.work_item_key,
                            edw_perenso_order_fact.account_order_no,
                            edw_perenso_order_fact.delvry_instns,
                            edw_perenso_order_fact.batch_key,
                            edw_perenso_order_fact.line_key,
                            edw_perenso_order_fact.prod_key,
                            edw_perenso_order_fact.unit_qty,
                            edw_perenso_order_fact.entered_qty,
                            edw_perenso_order_fact.entered_unit_key,
                            edw_perenso_order_fact.list_price,
                            edw_perenso_order_fact.nis,
                            edw_perenso_order_fact.rrp,
                            edw_perenso_order_fact.credit_line_key,
                            edw_perenso_order_fact.credited,
                            edw_perenso_order_fact.disc_key,
                            edw_perenso_order_fact.branch_key,
                            edw_perenso_order_fact.dist_acct,
                            edw_perenso_order_fact.delvry_dt,
                            edw_perenso_order_fact.order_batch_status_key,
                            edw_perenso_order_fact.suffix,
                            edw_perenso_order_fact.sent_dt,
                            edw_perenso_order_fact.deal_key,
                            edw_perenso_order_fact.deal_desc,
                            edw_perenso_order_fact.start_date,
                            edw_perenso_order_fact.end_date,
                            edw_perenso_order_fact.short_desc,
                            edw_perenso_order_fact.discount_desc,
                            edw_perenso_order_fact.order_header_status,
                            edw_perenso_order_fact.order_batch_status,
                            edw_perenso_order_fact.order_currency_cd
                        FROM edw_perenso_order_fact
                        WHERE (
                                (
                                    (
                                        (
                                            date_part(
                                                year,
                                                (
                                                    (edw_perenso_order_fact.delvry_dt)
                                                )
                                            )
                                        )::text || lpad(
                                            (
                                                date_part(
                                                    month,
                                                    (
                                                        (edw_perenso_order_fact.delvry_dt)
                                                    )
                                                )
                                            )::text,
                                            2,
                                            (0)::text
                                        )
                                    ) > '202001'::text
                                )
                                AND (
                                    upper((edw_perenso_order_fact.order_type_desc)::text) = 'SHIPPED ORDERS'::text
                                )
                            )
                    ) epof
                    LEFT JOIN edw_perenso_prod_dim eppd ON (((epof.prod_key = eppd.prod_key)))
                )
                LEFT JOIN (
                    SELECT edw_perenso_account_dim_snapshot.acct_id,
                        edw_perenso_account_dim_snapshot.acct_display_name,
                        edw_perenso_account_dim_snapshot.acct_type_desc,
                        edw_perenso_account_dim_snapshot.acct_street_1,
                        edw_perenso_account_dim_snapshot.acct_street_2,
                        edw_perenso_account_dim_snapshot.acct_street_3,
                        edw_perenso_account_dim_snapshot.acct_suburb,
                        edw_perenso_account_dim_snapshot.acct_postcode,
                        edw_perenso_account_dim_snapshot.acct_phone_number,
                        edw_perenso_account_dim_snapshot.acct_fax_number,
                        edw_perenso_account_dim_snapshot.acct_email,
                        edw_perenso_account_dim_snapshot.acct_country,
                        edw_perenso_account_dim_snapshot.acct_region,
                        edw_perenso_account_dim_snapshot.acct_state,
                        edw_perenso_account_dim_snapshot.acct_banner_country,
                        edw_perenso_account_dim_snapshot.acct_banner_division,
                        edw_perenso_account_dim_snapshot.acct_banner_type,
                        edw_perenso_account_dim_snapshot.acct_banner,
                        edw_perenso_account_dim_snapshot.acct_type,
                        edw_perenso_account_dim_snapshot.acct_sub_type,
                        edw_perenso_account_dim_snapshot.acct_grade,
                        edw_perenso_account_dim_snapshot.acct_nz_pharma_country,
                        edw_perenso_account_dim_snapshot.acct_nz_pharma_state,
                        edw_perenso_account_dim_snapshot.acct_nz_pharma_territory,
                        edw_perenso_account_dim_snapshot.acct_nz_groc_country,
                        edw_perenso_account_dim_snapshot.acct_nz_groc_state,
                        edw_perenso_account_dim_snapshot.acct_nz_groc_territory,
                        edw_perenso_account_dim_snapshot.acct_ssr_country,
                        edw_perenso_account_dim_snapshot.acct_ssr_state,
                        edw_perenso_account_dim_snapshot.acct_ssr_team_leader,
                        edw_perenso_account_dim_snapshot.acct_ssr_territory,
                        edw_perenso_account_dim_snapshot.acct_ssr_sub_territory,
                        edw_perenso_account_dim_snapshot.acct_ind_groc_country,
                        edw_perenso_account_dim_snapshot.acct_ind_groc_state,
                        edw_perenso_account_dim_snapshot.acct_ind_groc_territory,
                        edw_perenso_account_dim_snapshot.acct_ind_groc_sub_territory,
                        edw_perenso_account_dim_snapshot.acct_au_pharma_country,
                        edw_perenso_account_dim_snapshot.acct_au_pharma_state,
                        edw_perenso_account_dim_snapshot.acct_au_pharma_territory,
                        edw_perenso_account_dim_snapshot.acct_au_pharma_ssr_country,
                        edw_perenso_account_dim_snapshot.acct_au_pharma_ssr_state,
                        edw_perenso_account_dim_snapshot.acct_au_pharma_ssr_territory,
                        edw_perenso_account_dim_snapshot.acct_store_code,
                        edw_perenso_account_dim_snapshot.acct_fax_opt_out,
                        edw_perenso_account_dim_snapshot.acct_email_opt_out,
                        edw_perenso_account_dim_snapshot.acct_contact_method,
                        edw_perenso_account_dim_snapshot.snapshot_mnth,
                        edw_perenso_account_dim_snapshot.snapshot_dt
                    FROM edw_perenso_account_dim_snapshot
                    WHERE (
                            (edw_perenso_account_dim_snapshot.snapshot_mnth)::text > '202001'::text
                        )
                ) epad ON (
                    (
                        (epof.acct_key = epad.acct_id)
                        AND (
                            (
                                (
                                    date_part(
                                        year,
                                        ((epof.delvry_dt))
                                    )
                                )::text || lpad(
                                    (
                                        date_part(
                                            month,
                                            ((epof.delvry_dt))
                                        )
                                    )::text,
                                    2,
                                    (0)::text
                                )
                            ) = (epad.snapshot_mnth)::text
                        )
                    )
                )
            )
        WHERE (
                (
                    (
                        (
                            (
                                ((epof.delvry_dt)::timestamp without time zone) = (etd.cal_date)::date
                            )
                            AND (etd.jj_mnth_id = jjbr.jj_mnth_id)
                        )
                        AND (
                            (epof.order_currency_cd)::text = (jjbr.from_ccy)::text
                        )
                    )
                    AND (etd.jj_mnth_id = bwar.jj_mnth_id)
                )
                AND (
                    (epof.order_currency_cd)::text = (bwar.from_ccy)::text
                )
            )
        UNION ALL
        SELECT epof.delvry_dt,
            epad.snapshot_dt,
            etd.jj_year,
            etd.jj_qrtr,
            etd.jj_mnth_id,
            etd.jj_mnth_shrt,
            epof.acct_key,
            epad.acct_banner_division,
            epad.acct_banner,
            epad.acct_display_name,
            CASE
                WHEN (
                    upper((epad.acct_ind_groc_state)::text) <> 'NOT ASSIGNED'::text
                ) THEN epad.acct_ind_groc_state
                WHEN (
                    upper((epad.acct_au_pharma_state)::text) <> 'NOT ASSIGNED'::text
                ) THEN epad.acct_au_pharma_state
                WHEN (
                    upper((epad.acct_nz_pharma_state)::text) <> 'NOT ASSIGNED'::text
                ) THEN epad.acct_nz_pharma_state
                ELSE 'NOT ASSIGNED'::character varying
            END AS acct_tsm,
            CASE
                WHEN (
                    upper((epad.acct_ind_groc_territory)::text) <> 'NOT ASSIGNED'::text
                ) THEN epad.acct_ind_groc_territory
                WHEN (
                    upper((epad.acct_au_pharma_territory)::text) <> 'NOT ASSIGNED'::text
                ) THEN epad.acct_au_pharma_territory
                WHEN (
                    upper((epad.acct_nz_pharma_territory)::text) <> 'NOT ASSIGNED'::text
                ) THEN epad.acct_nz_pharma_territory
                ELSE 'NOT ASSIGNED'::character varying
            END AS acct_terriroty,
            eppd.prod_id AS prod_sapbw_code,
            eppd.prod_desc,
            eppd.prod_jj_brand,
            eppd.prod_ean,
            eppd.prod_jj_franchise,
            eppd.prod_jj_category,
            epof.unit_qty,
            (epof.entered_qty * epof.nis) AS nis,
            NULL AS aud_nis,
            NULL AS usd_nis
        FROM (
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
                    etdw.jj_mnth_wk,
                    etdc.cal_wk,
                    etdcm.cal_mnth_wk
                FROM edw_time_dim etd,
                    (
                        SELECT etd.jj_year,
                            etd.jj_mnth_id,
                            etd.jj_wk,
                            row_number() OVER(
                                PARTITION BY etd.jj_year,
                                etd.jj_mnth_id
                                ORDER BY etd.jj_year,
                                    etd.jj_mnth_id,
                                    etd.jj_wk
                            ) AS jj_mnth_wk
                        FROM (
                                SELECT DISTINCT etd.jj_year,
                                    etd.jj_mnth_id,
                                    etd.jj_wk
                                FROM edw_time_dim etd
                            ) etd
                    ) etdw,
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
                            CASE
                                WHEN (
                                    (
                                        row_number() OVER(
                                            PARTITION BY etd.cal_year
                                            ORDER BY (etd.cal_date::date)
                                        ) % (7)::bigint
                                    ) = 0
                                ) THEN (
                                    row_number() OVER(
                                        PARTITION BY etd.cal_year
                                        ORDER BY (etd.cal_date::date)
                                    ) / 7
                                )
                                ELSE (
                                    (
                                        row_number() OVER(
                                            PARTITION BY etd.cal_year
                                            ORDER BY (etd.cal_date::date)
                                        ) / 7
                                    ) + 1
                                )
                            END AS cal_wk
                        FROM edw_time_dim etd
                    ) etdc,
                    (
                        SELECT etdcw.cal_year,
                            etdcw.cal_mnth_id,
                            etdcw.cal_wk,
                            row_number() OVER(
                                PARTITION BY etdcw.cal_year,
                                etdcw.cal_mnth_id
                                ORDER BY etdcw.cal_year,
                                    etdcw.cal_mnth_id,
                                    etdcw.cal_wk
                            ) AS cal_mnth_wk
                        FROM (
                                SELECT DISTINCT etdc.cal_year,
                                    etdc.cal_mnth_id,
                                    etdc.cal_wk
                                FROM (
                                        SELECT etd.cal_year,
                                            etd.cal_mnth_id,
                                            CASE
                                                WHEN (
                                                    (
                                                        row_number() OVER(
                                                            PARTITION BY etd.cal_year
                                                            ORDER BY (etd.cal_date::date)
                                                        ) % (7)::bigint
                                                    ) = 0
                                                ) THEN (
                                                    row_number() OVER(
                                                        PARTITION BY etd.cal_year
                                                        ORDER BY (etd.cal_date::date)
                                                    ) / 7
                                                )
                                                ELSE (
                                                    (
                                                        row_number() OVER(
                                                            PARTITION BY etd.cal_year
                                                            ORDER BY (etd.cal_date::date)
                                                        ) / 7
                                                    ) + 1
                                                )
                                            END AS cal_wk
                                        FROM edw_time_dim etd
                                    ) etdc
                            ) etdcw
                    ) etdcm
                WHERE (
                        (
                            (
                                (
                                    (
                                        (
                                            (etd.jj_year = etdw.jj_year)
                                            AND (etd.jj_mnth_id = etdw.jj_mnth_id)
                                        )
                                        AND (etd.jj_wk = etdw.jj_wk)
                                    )
                                    AND ((etd.cal_date::date) = (etdc.cal_date::date))
                                )
                                AND (etdc.cal_wk = etdcm.cal_wk)
                            )
                            AND (etdc.cal_year = etdcm.cal_year)
                        )
                        AND (etdc.cal_mnth_id = etdcm.cal_mnth_id)
                    )
            ) etd,
            (
                (
                    (
                        SELECT edw_perenso_order_fact.order_key,
                            edw_perenso_order_fact.order_type_key,
                            edw_perenso_order_fact.order_type_desc,
                            edw_perenso_order_fact.acct_key,
                            edw_perenso_order_fact.order_date,
                            edw_perenso_order_fact.order_header_status_key,
                            edw_perenso_order_fact.charge,
                            edw_perenso_order_fact.confirmation,
                            edw_perenso_order_fact.diary_item_key,
                            edw_perenso_order_fact.work_item_key,
                            edw_perenso_order_fact.account_order_no,
                            edw_perenso_order_fact.delvry_instns,
                            edw_perenso_order_fact.batch_key,
                            edw_perenso_order_fact.line_key,
                            edw_perenso_order_fact.prod_key,
                            edw_perenso_order_fact.unit_qty,
                            edw_perenso_order_fact.entered_qty,
                            edw_perenso_order_fact.entered_unit_key,
                            edw_perenso_order_fact.list_price,
                            edw_perenso_order_fact.nis,
                            edw_perenso_order_fact.rrp,
                            edw_perenso_order_fact.credit_line_key,
                            edw_perenso_order_fact.credited,
                            edw_perenso_order_fact.disc_key,
                            edw_perenso_order_fact.branch_key,
                            edw_perenso_order_fact.dist_acct,
                            edw_perenso_order_fact.delvry_dt,
                            edw_perenso_order_fact.order_batch_status_key,
                            edw_perenso_order_fact.suffix,
                            edw_perenso_order_fact.sent_dt,
                            edw_perenso_order_fact.deal_key,
                            edw_perenso_order_fact.deal_desc,
                            edw_perenso_order_fact.start_date,
                            edw_perenso_order_fact.end_date,
                            edw_perenso_order_fact.short_desc,
                            edw_perenso_order_fact.discount_desc,
                            edw_perenso_order_fact.order_header_status,
                            edw_perenso_order_fact.order_batch_status,
                            edw_perenso_order_fact.order_currency_cd
                        FROM edw_perenso_order_fact
                        WHERE (
                                (
                                    (
                                        (
                                            date_part(
                                                year,
                                                (
                                                    (edw_perenso_order_fact.delvry_dt)
                                                )
                                            )
                                        )::text || lpad(
                                            (
                                                date_part(
                                                    month,
                                                    (
                                                        (edw_perenso_order_fact.delvry_dt)
                                                    )
                                                )
                                            )::text,
                                            2,
                                            (0)::text
                                        )
                                    ) > '202001'::text
                                )
                                AND (
                                    upper((edw_perenso_order_fact.order_type_desc)::text) = 'SHIPPED ORDERS'::text
                                )
                            )
                    ) epof
                    LEFT JOIN edw_perenso_prod_dim eppd ON (((epof.prod_key = eppd.prod_key)))
                )
                LEFT JOIN (
                    SELECT edw_perenso_account_dim_snapshot.acct_id,
                        edw_perenso_account_dim_snapshot.acct_display_name,
                        edw_perenso_account_dim_snapshot.acct_type_desc,
                        edw_perenso_account_dim_snapshot.acct_street_1,
                        edw_perenso_account_dim_snapshot.acct_street_2,
                        edw_perenso_account_dim_snapshot.acct_street_3,
                        edw_perenso_account_dim_snapshot.acct_suburb,
                        edw_perenso_account_dim_snapshot.acct_postcode,
                        edw_perenso_account_dim_snapshot.acct_phone_number,
                        edw_perenso_account_dim_snapshot.acct_fax_number,
                        edw_perenso_account_dim_snapshot.acct_email,
                        edw_perenso_account_dim_snapshot.acct_country,
                        edw_perenso_account_dim_snapshot.acct_region,
                        edw_perenso_account_dim_snapshot.acct_state,
                        edw_perenso_account_dim_snapshot.acct_banner_country,
                        edw_perenso_account_dim_snapshot.acct_banner_division,
                        edw_perenso_account_dim_snapshot.acct_banner_type,
                        edw_perenso_account_dim_snapshot.acct_banner,
                        edw_perenso_account_dim_snapshot.acct_type,
                        edw_perenso_account_dim_snapshot.acct_sub_type,
                        edw_perenso_account_dim_snapshot.acct_grade,
                        edw_perenso_account_dim_snapshot.acct_nz_pharma_country,
                        edw_perenso_account_dim_snapshot.acct_nz_pharma_state,
                        edw_perenso_account_dim_snapshot.acct_nz_pharma_territory,
                        edw_perenso_account_dim_snapshot.acct_nz_groc_country,
                        edw_perenso_account_dim_snapshot.acct_nz_groc_state,
                        edw_perenso_account_dim_snapshot.acct_nz_groc_territory,
                        edw_perenso_account_dim_snapshot.acct_ssr_country,
                        edw_perenso_account_dim_snapshot.acct_ssr_state,
                        edw_perenso_account_dim_snapshot.acct_ssr_team_leader,
                        edw_perenso_account_dim_snapshot.acct_ssr_territory,
                        edw_perenso_account_dim_snapshot.acct_ssr_sub_territory,
                        edw_perenso_account_dim_snapshot.acct_ind_groc_country,
                        edw_perenso_account_dim_snapshot.acct_ind_groc_state,
                        edw_perenso_account_dim_snapshot.acct_ind_groc_territory,
                        edw_perenso_account_dim_snapshot.acct_ind_groc_sub_territory,
                        edw_perenso_account_dim_snapshot.acct_au_pharma_country,
                        edw_perenso_account_dim_snapshot.acct_au_pharma_state,
                        edw_perenso_account_dim_snapshot.acct_au_pharma_territory,
                        edw_perenso_account_dim_snapshot.acct_au_pharma_ssr_country,
                        edw_perenso_account_dim_snapshot.acct_au_pharma_ssr_state,
                        edw_perenso_account_dim_snapshot.acct_au_pharma_ssr_territory,
                        edw_perenso_account_dim_snapshot.acct_store_code,
                        edw_perenso_account_dim_snapshot.acct_fax_opt_out,
                        edw_perenso_account_dim_snapshot.acct_email_opt_out,
                        edw_perenso_account_dim_snapshot.acct_contact_method,
                        edw_perenso_account_dim_snapshot.snapshot_mnth,
                        edw_perenso_account_dim_snapshot.snapshot_dt
                    FROM edw_perenso_account_dim_snapshot
                    WHERE (
                            (edw_perenso_account_dim_snapshot.snapshot_mnth)::text > '202001'::text
                        )
                ) epad ON (
                    (
                        (epof.acct_key = epad.acct_id)
                        AND (
                            (
                                (
                                    date_part(
                                        year,
                                        ((epof.delvry_dt))
                                    )
                                )::text || lpad(
                                    (
                                        date_part(
                                            month,
                                            ((epof.delvry_dt))
                                        )
                                    )::text,
                                    2,
                                    (0)::text
                                )
                            ) = (epad.snapshot_mnth)::text
                        )
                    )
                )
            )
        WHERE (
                (
                    ((epof.delvry_dt)::date) = (etd.cal_date::date)
                )
                AND (
                    (epof.order_currency_cd)::text = 'NOT ASSIGNED'::text
                )
            )
    )
    UNION ALL
    SELECT epof.delvry_dt,
        epad.snapshot_dt,
        etd.jj_year,
        etd.jj_qrtr,
        etd.jj_mnth_id,
        etd.jj_mnth_shrt,
        epof.acct_key,
        epad.acct_banner_division,
        epad.acct_banner,
        epad.acct_display_name,
        CASE
            WHEN (
                upper((epad.acct_ind_groc_state)::text) <> 'NOT ASSIGNED'::text
            ) THEN epad.acct_ind_groc_state
            WHEN (
                upper((epad.acct_au_pharma_state)::text) <> 'NOT ASSIGNED'::text
            ) THEN epad.acct_au_pharma_state
            WHEN (
                upper((epad.acct_nz_pharma_state)::text) <> 'NOT ASSIGNED'::text
            ) THEN epad.acct_nz_pharma_state
            ELSE 'NOT ASSIGNED'::character varying
        END AS acct_tsm,
        CASE
            WHEN (
                upper((epad.acct_ind_groc_territory)::text) <> 'NOT ASSIGNED'::text
            ) THEN epad.acct_ind_groc_territory
            WHEN (
                upper((epad.acct_au_pharma_territory)::text) <> 'NOT ASSIGNED'::text
            ) THEN epad.acct_au_pharma_territory
            WHEN (
                upper((epad.acct_nz_pharma_territory)::text) <> 'NOT ASSIGNED'::text
            ) THEN epad.acct_nz_pharma_territory
            ELSE 'NOT ASSIGNED'::character varying
        END AS acct_terriroty,
        eppd.prod_id AS prod_sapbw_code,
        eppd.prod_desc,
        eppd.prod_jj_brand,
        eppd.prod_ean,
        eppd.prod_jj_franchise,
        eppd.prod_jj_category,
        epof.unit_qty,
        (epof.entered_qty * epof.nis) AS nis,
        ((epof.entered_qty * epof.nis) * jjbr.exch_rate) AS aud_nis,
        ((epof.entered_qty * epof.nis) * bwar.exch_rate) AS usd_nis
    FROM (
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
                etdw.jj_mnth_wk,
                etdc.cal_wk,
                etdcm.cal_mnth_wk
            FROM edw_time_dim etd,
                (
                    SELECT etd.jj_year,
                        etd.jj_mnth_id,
                        etd.jj_wk,
                        row_number() OVER(
                            PARTITION BY etd.jj_year,
                            etd.jj_mnth_id
                            ORDER BY etd.jj_year,
                                etd.jj_mnth_id,
                                etd.jj_wk
                        ) AS jj_mnth_wk
                    FROM (
                            SELECT DISTINCT etd.jj_year,
                                etd.jj_mnth_id,
                                etd.jj_wk
                            FROM edw_time_dim etd
                        ) etd
                ) etdw,
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
                        CASE
                            WHEN (
                                (
                                    row_number() OVER(
                                        PARTITION BY etd.cal_year
                                        ORDER BY (etd.cal_date::date)
                                    ) % (7)::bigint
                                ) = 0
                            ) THEN (
                                row_number() OVER(
                                    PARTITION BY etd.cal_year
                                    ORDER BY (etd.cal_date::date)
                                ) / 7
                            )
                            ELSE (
                                (
                                    row_number() OVER(
                                        PARTITION BY etd.cal_year
                                        ORDER BY (etd.cal_date::date)
                                    ) / 7
                                ) + 1
                            )
                        END AS cal_wk
                    FROM edw_time_dim etd
                ) etdc,
                (
                    SELECT etdcw.cal_year,
                        etdcw.cal_mnth_id,
                        etdcw.cal_wk,
                        row_number() OVER(
                            PARTITION BY etdcw.cal_year,
                            etdcw.cal_mnth_id
                            ORDER BY etdcw.cal_year,
                                etdcw.cal_mnth_id,
                                etdcw.cal_wk
                        ) AS cal_mnth_wk
                    FROM (
                            SELECT DISTINCT etdc.cal_year,
                                etdc.cal_mnth_id,
                                etdc.cal_wk
                            FROM (
                                    SELECT etd.cal_year,
                                        etd.cal_mnth_id,
                                        CASE
                                            WHEN (
                                                (
                                                    row_number() OVER(
                                                        PARTITION BY etd.cal_year
                                                        ORDER BY (etd.cal_date::date)
                                                    ) % (7)::bigint
                                                ) = 0
                                            ) THEN (
                                                row_number() OVER(
                                                    PARTITION BY etd.cal_year
                                                    ORDER BY (etd.cal_date::date)
                                                ) / 7
                                            )
                                            ELSE (
                                                (
                                                    row_number() OVER(
                                                        PARTITION BY etd.cal_year
                                                        ORDER BY (etd.cal_date::date)
                                                    ) / 7
                                                ) + 1
                                            )
                                        END AS cal_wk
                                    FROM edw_time_dim etd
                                ) etdc
                        ) etdcw
                ) etdcm
            WHERE (
                    (
                        (
                            (
                                (
                                    (
                                        (etd.jj_year = etdw.jj_year)
                                        AND (etd.jj_mnth_id = etdw.jj_mnth_id)
                                    )
                                    AND (etd.jj_wk = etdw.jj_wk)
                                )
                                AND ((etd.cal_date::date) = (etdc.cal_date::date))
                            )
                            AND (etdc.cal_wk = etdcm.cal_wk)
                        )
                        AND (etdc.cal_year = etdcm.cal_year)
                    )
                    AND (etdc.cal_mnth_id = etdcm.cal_mnth_id)
                )
        ) etd,
        (
            SELECT vw_jjbr_curr_exch_dim.rate_type,
                vw_jjbr_curr_exch_dim.from_ccy,
                vw_jjbr_curr_exch_dim.to_ccy,
                vw_jjbr_curr_exch_dim.jj_mnth_id,
                vw_jjbr_curr_exch_dim.exch_rate
            FROM vw_jjbr_curr_exch_dim
            WHERE (
                    (vw_jjbr_curr_exch_dim.to_ccy)::text = 'AUD'::text
                )
        ) jjbr,
        (
            SELECT vw_bwar_curr_exch_dim.rate_type,
                vw_bwar_curr_exch_dim.from_ccy,
                vw_bwar_curr_exch_dim.to_ccy,
                vw_bwar_curr_exch_dim.jj_mnth_id,
                vw_bwar_curr_exch_dim.exch_rate
            FROM vw_bwar_curr_exch_dim
            WHERE (
                    (vw_bwar_curr_exch_dim.to_ccy)::text = 'USD'::text
                )
        ) bwar,
        (
            (
                (
                    SELECT edw_perenso_order_fact.order_key,
                        edw_perenso_order_fact.order_type_key,
                        edw_perenso_order_fact.order_type_desc,
                        edw_perenso_order_fact.acct_key,
                        edw_perenso_order_fact.order_date,
                        edw_perenso_order_fact.order_header_status_key,
                        edw_perenso_order_fact.charge,
                        edw_perenso_order_fact.confirmation,
                        edw_perenso_order_fact.diary_item_key,
                        edw_perenso_order_fact.work_item_key,
                        edw_perenso_order_fact.account_order_no,
                        edw_perenso_order_fact.delvry_instns,
                        edw_perenso_order_fact.batch_key,
                        edw_perenso_order_fact.line_key,
                        edw_perenso_order_fact.prod_key,
                        edw_perenso_order_fact.unit_qty,
                        edw_perenso_order_fact.entered_qty,
                        edw_perenso_order_fact.entered_unit_key,
                        edw_perenso_order_fact.list_price,
                        edw_perenso_order_fact.nis,
                        edw_perenso_order_fact.rrp,
                        edw_perenso_order_fact.credit_line_key,
                        edw_perenso_order_fact.credited,
                        edw_perenso_order_fact.disc_key,
                        edw_perenso_order_fact.branch_key,
                        edw_perenso_order_fact.dist_acct,
                        edw_perenso_order_fact.delvry_dt,
                        edw_perenso_order_fact.order_batch_status_key,
                        edw_perenso_order_fact.suffix,
                        edw_perenso_order_fact.sent_dt,
                        edw_perenso_order_fact.deal_key,
                        edw_perenso_order_fact.deal_desc,
                        edw_perenso_order_fact.start_date,
                        edw_perenso_order_fact.end_date,
                        edw_perenso_order_fact.short_desc,
                        edw_perenso_order_fact.discount_desc,
                        edw_perenso_order_fact.order_header_status,
                        edw_perenso_order_fact.order_batch_status,
                        edw_perenso_order_fact.order_currency_cd
                    FROM edw_perenso_order_fact
                    WHERE (
                            (
                                (
                                    (
                                        (
                                            date_part(
                                                year,
                                                (
                                                    (edw_perenso_order_fact.delvry_dt)
                                                )
                                            )
                                        )::text || lpad(
                                            (
                                                date_part(
                                                    month,
                                                    (
                                                        (edw_perenso_order_fact.delvry_dt)
                                                    )
                                                )
                                            )::text,
                                            2,
                                            (0)::text
                                        )
                                    ) >= '201801'::text
                                )
                                AND (
                                    (
                                        (
                                            date_part(
                                                year,
                                                (
                                                    (edw_perenso_order_fact.delvry_dt)
                                                )
                                            )
                                        )::text || lpad(
                                            (
                                                date_part(
                                                    month,
                                                    (
                                                        (edw_perenso_order_fact.delvry_dt)
                                                    )
                                                )
                                            )::text,
                                            2,
                                            (0)::text
                                        )
                                    ) <= '202001'::text
                                )
                            )
                            AND (
                                upper((edw_perenso_order_fact.order_type_desc)::text) = 'SHIPPED ORDERS'::text
                            )
                        )
                ) epof
                LEFT JOIN edw_perenso_prod_dim eppd ON (((epof.prod_key = eppd.prod_key)))
            )
            LEFT JOIN (
                SELECT edw_perenso_account_dim_snapshot.acct_id,
                    edw_perenso_account_dim_snapshot.acct_display_name,
                    edw_perenso_account_dim_snapshot.acct_type_desc,
                    edw_perenso_account_dim_snapshot.acct_street_1,
                    edw_perenso_account_dim_snapshot.acct_street_2,
                    edw_perenso_account_dim_snapshot.acct_street_3,
                    edw_perenso_account_dim_snapshot.acct_suburb,
                    edw_perenso_account_dim_snapshot.acct_postcode,
                    edw_perenso_account_dim_snapshot.acct_phone_number,
                    edw_perenso_account_dim_snapshot.acct_fax_number,
                    edw_perenso_account_dim_snapshot.acct_email,
                    edw_perenso_account_dim_snapshot.acct_country,
                    edw_perenso_account_dim_snapshot.acct_region,
                    edw_perenso_account_dim_snapshot.acct_state,
                    edw_perenso_account_dim_snapshot.acct_banner_country,
                    edw_perenso_account_dim_snapshot.acct_banner_division,
                    edw_perenso_account_dim_snapshot.acct_banner_type,
                    edw_perenso_account_dim_snapshot.acct_banner,
                    edw_perenso_account_dim_snapshot.acct_type,
                    edw_perenso_account_dim_snapshot.acct_sub_type,
                    edw_perenso_account_dim_snapshot.acct_grade,
                    edw_perenso_account_dim_snapshot.acct_nz_pharma_country,
                    edw_perenso_account_dim_snapshot.acct_nz_pharma_state,
                    edw_perenso_account_dim_snapshot.acct_nz_pharma_territory,
                    edw_perenso_account_dim_snapshot.acct_nz_groc_country,
                    edw_perenso_account_dim_snapshot.acct_nz_groc_state,
                    edw_perenso_account_dim_snapshot.acct_nz_groc_territory,
                    edw_perenso_account_dim_snapshot.acct_ssr_country,
                    edw_perenso_account_dim_snapshot.acct_ssr_state,
                    edw_perenso_account_dim_snapshot.acct_ssr_team_leader,
                    edw_perenso_account_dim_snapshot.acct_ssr_territory,
                    edw_perenso_account_dim_snapshot.acct_ssr_sub_territory,
                    edw_perenso_account_dim_snapshot.acct_ind_groc_country,
                    edw_perenso_account_dim_snapshot.acct_ind_groc_state,
                    edw_perenso_account_dim_snapshot.acct_ind_groc_territory,
                    edw_perenso_account_dim_snapshot.acct_ind_groc_sub_territory,
                    edw_perenso_account_dim_snapshot.acct_au_pharma_country,
                    edw_perenso_account_dim_snapshot.acct_au_pharma_state,
                    edw_perenso_account_dim_snapshot.acct_au_pharma_territory,
                    edw_perenso_account_dim_snapshot.acct_au_pharma_ssr_country,
                    edw_perenso_account_dim_snapshot.acct_au_pharma_ssr_state,
                    edw_perenso_account_dim_snapshot.acct_au_pharma_ssr_territory,
                    edw_perenso_account_dim_snapshot.acct_store_code,
                    edw_perenso_account_dim_snapshot.acct_fax_opt_out,
                    edw_perenso_account_dim_snapshot.acct_email_opt_out,
                    edw_perenso_account_dim_snapshot.acct_contact_method,
                    edw_perenso_account_dim_snapshot.snapshot_mnth,
                    edw_perenso_account_dim_snapshot.snapshot_dt
                FROM edw_perenso_account_dim_snapshot
                WHERE (
                        (edw_perenso_account_dim_snapshot.snapshot_mnth)::text = '202001'::text
                    )
            ) epad ON (((epof.acct_key = epad.acct_id)))
        )
    WHERE (
            (
                (
                    (
                        (
                            ((epof.delvry_dt)::timestamp without time zone) = (etd.cal_date)
                        )
                        AND (etd.jj_mnth_id = jjbr.jj_mnth_id)
                    )
                    AND (
                        (epof.order_currency_cd)::text = (jjbr.from_ccy)::text
                    )
                )
                AND (etd.jj_mnth_id = bwar.jj_mnth_id)
            )
            AND (
                (epof.order_currency_cd)::text = (bwar.from_ccy)::text
            )
        )
)
UNION ALL
SELECT epof.delvry_dt,
    epad.snapshot_dt,
    etd.jj_year,
    etd.jj_qrtr,
    etd.jj_mnth_id,
    etd.jj_mnth_shrt,
    epof.acct_key,
    epad.acct_banner_division,
    epad.acct_banner,
    epad.acct_display_name,
    CASE
        WHEN (
            upper((epad.acct_ind_groc_state)::text) <> 'NOT ASSIGNED'::text
        ) THEN epad.acct_ind_groc_state
        WHEN (
            upper((epad.acct_au_pharma_state)::text) <> 'NOT ASSIGNED'::text
        ) THEN epad.acct_au_pharma_state
        WHEN (
            upper((epad.acct_nz_pharma_state)::text) <> 'NOT ASSIGNED'::text
        ) THEN epad.acct_nz_pharma_state
        ELSE 'NOT ASSIGNED'::character varying
    END AS acct_tsm,
    CASE
        WHEN (
            upper((epad.acct_ind_groc_territory)::text) <> 'NOT ASSIGNED'::text
        ) THEN epad.acct_ind_groc_territory
        WHEN (
            upper((epad.acct_au_pharma_territory)::text) <> 'NOT ASSIGNED'::text
        ) THEN epad.acct_au_pharma_territory
        WHEN (
            upper((epad.acct_nz_pharma_territory)::text) <> 'NOT ASSIGNED'::text
        ) THEN epad.acct_nz_pharma_territory
        ELSE 'NOT ASSIGNED'::character varying
    END AS acct_terriroty,
    eppd.prod_id AS prod_sapbw_code,
    eppd.prod_desc,
    eppd.prod_jj_brand,
    eppd.prod_ean,
    eppd.prod_jj_franchise,
    eppd.prod_jj_category,
    epof.unit_qty,
    (epof.entered_qty * epof.nis) AS nis,
    NULL AS aud_nis,
    NULL AS usd_nis
FROM (
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
            etdw.jj_mnth_wk,
            etdc.cal_wk,
            etdcm.cal_mnth_wk
        FROM edw_time_dim etd,
            (
                SELECT etd.jj_year,
                    etd.jj_mnth_id,
                    etd.jj_wk,
                    row_number() OVER(
                        PARTITION BY etd.jj_year,
                        etd.jj_mnth_id
                        ORDER BY etd.jj_year,
                            etd.jj_mnth_id,
                            etd.jj_wk
                    ) AS jj_mnth_wk
                FROM (
                        SELECT DISTINCT etd.jj_year,
                            etd.jj_mnth_id,
                            etd.jj_wk
                        FROM edw_time_dim etd
                    ) etd
            ) etdw,
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
                    CASE
                        WHEN (
                            (
                                row_number() OVER(
                                    PARTITION BY etd.cal_year
                                    ORDER BY (etd.cal_date::date)
                                ) % (7)::bigint
                            ) = 0
                        ) THEN (
                            row_number() OVER(
                                PARTITION BY etd.cal_year
                                ORDER BY (etd.cal_date::date)
                            ) / 7
                        )
                        ELSE (
                            (
                                row_number() OVER(
                                    PARTITION BY etd.cal_year
                                    ORDER BY (etd.cal_date::date)
                                ) / 7
                            ) + 1
                        )
                    END AS cal_wk
                FROM edw_time_dim etd
            ) etdc,
            (
                SELECT etdcw.cal_year,
                    etdcw.cal_mnth_id,
                    etdcw.cal_wk,
                    row_number() OVER(
                        PARTITION BY etdcw.cal_year,
                        etdcw.cal_mnth_id
                        ORDER BY etdcw.cal_year,
                            etdcw.cal_mnth_id,
                            etdcw.cal_wk
                    ) AS cal_mnth_wk
                FROM (
                        SELECT DISTINCT etdc.cal_year,
                            etdc.cal_mnth_id,
                            etdc.cal_wk
                        FROM (
                                SELECT etd.cal_year,
                                    etd.cal_mnth_id,
                                    CASE
                                        WHEN (
                                            (
                                                row_number() OVER(
                                                    PARTITION BY etd.cal_year
                                                    ORDER BY (etd.cal_date::date)
                                                ) % (7)::bigint
                                            ) = 0
                                        ) THEN (
                                            row_number() OVER(
                                                PARTITION BY etd.cal_year
                                                ORDER BY (etd.cal_date::date)
                                            ) / 7
                                        )
                                        ELSE (
                                            (
                                                row_number() OVER(
                                                    PARTITION BY etd.cal_year
                                                    ORDER BY (etd.cal_date::date)
                                                ) / 7
                                            ) + 1
                                        )
                                    END AS cal_wk
                                FROM edw_time_dim etd
                            ) etdc
                    ) etdcw
            ) etdcm
        WHERE (
                (
                    (
                        (
                            (
                                (
                                    (etd.jj_year = etdw.jj_year)
                                    AND (etd.jj_mnth_id = etdw.jj_mnth_id)
                                )
                                AND (etd.jj_wk = etdw.jj_wk)
                            )
                            AND ((etd.cal_date::date) = (etdc.cal_date::date))
                        )
                        AND (etdc.cal_wk = etdcm.cal_wk)
                    )
                    AND (etdc.cal_year = etdcm.cal_year)
                )
                AND (etdc.cal_mnth_id = etdcm.cal_mnth_id)
            )
    ) etd,
    (
        (
            (
                SELECT edw_perenso_order_fact.order_key,
                    edw_perenso_order_fact.order_type_key,
                    edw_perenso_order_fact.order_type_desc,
                    edw_perenso_order_fact.acct_key,
                    edw_perenso_order_fact.order_date,
                    edw_perenso_order_fact.order_header_status_key,
                    edw_perenso_order_fact.charge,
                    edw_perenso_order_fact.confirmation,
                    edw_perenso_order_fact.diary_item_key,
                    edw_perenso_order_fact.work_item_key,
                    edw_perenso_order_fact.account_order_no,
                    edw_perenso_order_fact.delvry_instns,
                    edw_perenso_order_fact.batch_key,
                    edw_perenso_order_fact.line_key,
                    edw_perenso_order_fact.prod_key,
                    edw_perenso_order_fact.unit_qty,
                    edw_perenso_order_fact.entered_qty,
                    edw_perenso_order_fact.entered_unit_key,
                    edw_perenso_order_fact.list_price,
                    edw_perenso_order_fact.nis,
                    edw_perenso_order_fact.rrp,
                    edw_perenso_order_fact.credit_line_key,
                    edw_perenso_order_fact.credited,
                    edw_perenso_order_fact.disc_key,
                    edw_perenso_order_fact.branch_key,
                    edw_perenso_order_fact.dist_acct,
                    edw_perenso_order_fact.delvry_dt,
                    edw_perenso_order_fact.order_batch_status_key,
                    edw_perenso_order_fact.suffix,
                    edw_perenso_order_fact.sent_dt,
                    edw_perenso_order_fact.deal_key,
                    edw_perenso_order_fact.deal_desc,
                    edw_perenso_order_fact.start_date,
                    edw_perenso_order_fact.end_date,
                    edw_perenso_order_fact.short_desc,
                    edw_perenso_order_fact.discount_desc,
                    edw_perenso_order_fact.order_header_status,
                    edw_perenso_order_fact.order_batch_status,
                    edw_perenso_order_fact.order_currency_cd
                FROM edw_perenso_order_fact
                WHERE (
                        (
                            (
                                (
                                    (
                                        date_part(
                                            year,
                                            (
                                                (edw_perenso_order_fact.delvry_dt)
                                            )
                                        )
                                    )::text || lpad(
                                        (
                                            date_part(
                                                month,
                                                (
                                                    (edw_perenso_order_fact.delvry_dt)
                                                )
                                            )
                                        )::text,
                                        2,
                                        (0)::text
                                    )
                                ) >= '201801'::text
                            )
                            AND (
                                (
                                    (
                                        date_part(
                                            year,
                                            (
                                                (edw_perenso_order_fact.delvry_dt)
                                            )
                                        )
                                    )::text || lpad(
                                        (
                                            date_part(
                                                month,
                                                (
                                                    (edw_perenso_order_fact.delvry_dt)
                                                )
                                            )
                                        )::text,
                                        2,
                                        (0)::text
                                    )
                                ) <= '202001'::text
                            )
                        )
                        AND (
                            upper((edw_perenso_order_fact.order_type_desc)::text) = 'SHIPPED ORDERS'::text
                        )
                    )
            ) epof
            LEFT JOIN edw_perenso_prod_dim eppd ON (((epof.prod_key = eppd.prod_key)))
        )
        LEFT JOIN (
            SELECT edw_perenso_account_dim_snapshot.acct_id,
                edw_perenso_account_dim_snapshot.acct_display_name,
                edw_perenso_account_dim_snapshot.acct_type_desc,
                edw_perenso_account_dim_snapshot.acct_street_1,
                edw_perenso_account_dim_snapshot.acct_street_2,
                edw_perenso_account_dim_snapshot.acct_street_3,
                edw_perenso_account_dim_snapshot.acct_suburb,
                edw_perenso_account_dim_snapshot.acct_postcode,
                edw_perenso_account_dim_snapshot.acct_phone_number,
                edw_perenso_account_dim_snapshot.acct_fax_number,
                edw_perenso_account_dim_snapshot.acct_email,
                edw_perenso_account_dim_snapshot.acct_country,
                edw_perenso_account_dim_snapshot.acct_region,
                edw_perenso_account_dim_snapshot.acct_state,
                edw_perenso_account_dim_snapshot.acct_banner_country,
                edw_perenso_account_dim_snapshot.acct_banner_division,
                edw_perenso_account_dim_snapshot.acct_banner_type,
                edw_perenso_account_dim_snapshot.acct_banner,
                edw_perenso_account_dim_snapshot.acct_type,
                edw_perenso_account_dim_snapshot.acct_sub_type,
                edw_perenso_account_dim_snapshot.acct_grade,
                edw_perenso_account_dim_snapshot.acct_nz_pharma_country,
                edw_perenso_account_dim_snapshot.acct_nz_pharma_state,
                edw_perenso_account_dim_snapshot.acct_nz_pharma_territory,
                edw_perenso_account_dim_snapshot.acct_nz_groc_country,
                edw_perenso_account_dim_snapshot.acct_nz_groc_state,
                edw_perenso_account_dim_snapshot.acct_nz_groc_territory,
                edw_perenso_account_dim_snapshot.acct_ssr_country,
                edw_perenso_account_dim_snapshot.acct_ssr_state,
                edw_perenso_account_dim_snapshot.acct_ssr_team_leader,
                edw_perenso_account_dim_snapshot.acct_ssr_territory,
                edw_perenso_account_dim_snapshot.acct_ssr_sub_territory,
                edw_perenso_account_dim_snapshot.acct_ind_groc_country,
                edw_perenso_account_dim_snapshot.acct_ind_groc_state,
                edw_perenso_account_dim_snapshot.acct_ind_groc_territory,
                edw_perenso_account_dim_snapshot.acct_ind_groc_sub_territory,
                edw_perenso_account_dim_snapshot.acct_au_pharma_country,
                edw_perenso_account_dim_snapshot.acct_au_pharma_state,
                edw_perenso_account_dim_snapshot.acct_au_pharma_territory,
                edw_perenso_account_dim_snapshot.acct_au_pharma_ssr_country,
                edw_perenso_account_dim_snapshot.acct_au_pharma_ssr_state,
                edw_perenso_account_dim_snapshot.acct_au_pharma_ssr_territory,
                edw_perenso_account_dim_snapshot.acct_store_code,
                edw_perenso_account_dim_snapshot.acct_fax_opt_out,
                edw_perenso_account_dim_snapshot.acct_email_opt_out,
                edw_perenso_account_dim_snapshot.acct_contact_method,
                edw_perenso_account_dim_snapshot.snapshot_mnth,
                edw_perenso_account_dim_snapshot.snapshot_dt
            FROM edw_perenso_account_dim_snapshot
            WHERE (
                    (edw_perenso_account_dim_snapshot.snapshot_mnth)::text = '202001'::text
                )
        ) epad ON (((epof.acct_key = epad.acct_id)))
    )
WHERE (
        (
            ((epof.delvry_dt)::timestamp without time zone) = (etd.cal_date)
        )
        AND (
            (epof.order_currency_cd)::text = 'NOT ASSIGNED'::text
        )
    )
)
)
select * from final