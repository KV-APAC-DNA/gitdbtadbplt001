with 
itg_mds_cogs_master_dim as 
(
    select * from snappcfitg_integration.itg_mds_cogs_master_dim
),
edw_time_dim as 
(
    select * from snappcfedw_integration.edw_time_dim
),
edw_crncy_exch as 
(
    select * from {{ ref('aspedw_integration__edw_crncy_exch') }}
),
temp_a as 
(
    SELECT extd.jj_year,
    extd.jj_mnth_id,
    (ltrim((cogs.sku)::text, (0)::text))::character varying AS matl_id,
    extd.crncy,
    (cogs.nz_cogs_per_unit)::numeric(38, 5) AS sgd_cogs_per_unit,
    extd.ex_rt,
    ((cogs.nz_cogs_per_unit * extd.ex_rt))::numeric(38, 5) AS cogs_per_unit
FROM (
        (
            SELECT itg.jj_year,
                etd.jj_mnth_id,
                itg.sku,
                itg.nz_cogs_per_unit
            FROM (
                    (
                        SELECT itg_mds_cogs_master_dim.jj_year,
                            itg_mds_cogs_master_dim.sku,
                            itg_mds_cogs_master_dim.crncy,
                            itg_mds_cogs_master_dim.au_cogs_per_unit,
                            itg_mds_cogs_master_dim.nz_cogs_per_unit,
                            itg_mds_cogs_master_dim.crt_dttm
                        FROM itg_mds_cogs_master_dim
                        WHERE (
                                itg_mds_cogs_master_dim.nz_cogs_per_unit <> (0)::numeric(31, 2)
                            )
                    ) itg
                    LEFT JOIN (
                        SELECT DISTINCT edw_time_dim.jj_mnth_id,
                            (edw_time_dim.jj_year)::character varying AS jj_year
                        FROM edw_time_dim
                    ) etd ON (((itg.jj_year)::text = (etd.jj_year)::text))
                )
        ) cogs
        JOIN (
            SELECT etd.jj_year,
                etd.jj_mnth_id,
                exch.to_crncy AS crncy,
                exch.ex_rt
            FROM (
                    (
                        SELECT edw_crncy_exch.ex_rt_typ,
                            edw_crncy_exch.from_crncy,
                            edw_crncy_exch.to_crncy,
                            (
                                (
                                    (99999999)::numeric - (edw_crncy_exch.vld_from)::numeric
                                )
                            )::numeric(18, 0) AS valid_date,
                            edw_crncy_exch.ex_rt
                        FROM edw_crncy_exch
                        WHERE (
                                (
                                    ((edw_crncy_exch.from_crncy)::text = 'SGD'::text)
                                    AND ((edw_crncy_exch.to_crncy)::text = 'NZD'::text)
                                )
                                AND ((edw_crncy_exch.ex_rt_typ)::text = 'BWAR'::text)
                            )
                    ) exch
                    JOIN (
                        SELECT edw_time_dim.jj_mnth_id,
                            (edw_time_dim.jj_year)::character varying AS jj_year,
                            edw_time_dim.time_id
                        FROM edw_time_dim
                    ) etd ON ((exch.valid_date = etd.time_id))
                )
        ) extd ON (
            (
                (extd.jj_mnth_id = cogs.jj_mnth_id)
                AND ((extd.jj_year)::text = (cogs.jj_year)::text)
            )
        )
    )
),
temp_b as
(
    SELECT extd.jj_year,
    extd.jj_mnth_id,
    (ltrim((cogs.sku)::text, (0)::text))::character varying AS matl_id,
    extd.crncy,
    (cogs.au_cogs_per_unit)::numeric(38, 5) AS sgd_cogs_per_unit,
    extd.ex_rt,
    ((cogs.au_cogs_per_unit * extd.ex_rt))::numeric(38, 5) AS cogs_per_unit
FROM (
        (
            SELECT itg.jj_year,
                etd.jj_mnth_id,
                itg.sku,
                itg.au_cogs_per_unit
            FROM (
                    (
                        SELECT itg_mds_cogs_master_dim.jj_year,
                            itg_mds_cogs_master_dim.sku,
                            itg_mds_cogs_master_dim.crncy,
                            itg_mds_cogs_master_dim.au_cogs_per_unit,
                            itg_mds_cogs_master_dim.nz_cogs_per_unit,
                            itg_mds_cogs_master_dim.crt_dttm
                        FROM itg_mds_cogs_master_dim
                        WHERE (
                                itg_mds_cogs_master_dim.au_cogs_per_unit <> (0)::numeric(31, 2)
                            )
                    ) itg
                    LEFT JOIN (
                        SELECT DISTINCT edw_time_dim.jj_mnth_id,
                            (edw_time_dim.jj_year)::character varying AS jj_year
                        FROM edw_time_dim
                    ) etd ON (((itg.jj_year)::text = (etd.jj_year)::text))
                )
        ) cogs
        JOIN (
            SELECT a.jj_year,
                mnth.jj_mnth_id,
                a.crncy,
                a.ex_rt
            FROM (
                    (
                        SELECT etd.jj_year,
                            etd.jj_mnth_id,
                            exch.to_crncy AS crncy,
                            exch.ex_rt
                        FROM (
                                (
                                    SELECT edw_crncy_exch.ex_rt_typ,
                                        edw_crncy_exch.from_crncy,
                                        edw_crncy_exch.to_crncy,
                                        (
                                            (
                                                (99999999)::numeric - (edw_crncy_exch.vld_from)::numeric
                                            )
                                        )::numeric(18, 0) AS valid_date,
                                        edw_crncy_exch.ex_rt
                                    FROM edw_crncy_exch
                                    WHERE (
                                            (
                                                ((edw_crncy_exch.from_crncy)::text = 'SGD'::text)
                                                AND ((edw_crncy_exch.to_crncy)::text = 'AUD'::text)
                                            )
                                            AND ((edw_crncy_exch.ex_rt_typ)::text = 'BWAR'::text)
                                        )
                                ) exch
                                JOIN (
                                    SELECT edw_time_dim.jj_mnth_id,
                                        (edw_time_dim.jj_year)::character varying AS jj_year,
                                        edw_time_dim.time_id
                                    FROM edw_time_dim
                                ) etd ON ((exch.valid_date = etd.time_id))
                            )
                    ) a
                    JOIN (
                        SELECT DISTINCT edw_time_dim.jj_mnth_id,
                            (edw_time_dim.jj_year)::character varying AS jj_year
                        FROM edw_time_dim
                    ) mnth ON (((mnth.jj_year)::text = (a.jj_year)::text))
                )
        ) extd ON (
            (
                (extd.jj_mnth_id = cogs.jj_mnth_id)
                AND ((extd.jj_year)::text = (cogs.jj_year)::text)
            )
        )
    )
),
final as
(
    select * from temp_a
    union all
    select * from temp_b
)
select * from final