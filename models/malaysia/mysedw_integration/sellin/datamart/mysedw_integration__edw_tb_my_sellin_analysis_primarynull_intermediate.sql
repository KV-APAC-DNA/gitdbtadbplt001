{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )
}}
--Import CTEs

with edw_vw_my_curr_dim as (
    select * from {{ ref('mysedw_integration__edw_vw_my_curr_dim') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_tb_my_sellin_analysis_primarynull_source as (
    select * from {{ ref('mysedw_integration__edw_tb_my_sellin_analysis_primarynull_source') }}
),


--Logical CTEs
ds_primary_null_cntd as (
                        SELECT 'Primary_null' AS data_src,
                            veossf.*,
                            veocurd.from_ccy AS from_crncy,
                            veocurd.to_ccy AS to_crncy,
                            (cur_period_sgt.mnth_id)::character varying AS cur_period_sgt
                        FROM 
                            (
                                edw_tb_my_sellin_analysis_primarynull_source --18s - xsmall - 14M
                            ) veossf,
                            (
                                SELECT a.mnth_id,
                                    a.cal_date_id
                                FROM edw_vw_os_time_dim a
                                WHERE (
                                        a.cal_date_id = replace(
                                            current_timestamp()::date,
                                            ('-'::character varying)::text,
                                            (''::character varying)::text
                                        )
                                    )
                            ) as cur_period_sgt,
                            (
                                SELECT d.cntry_key,
                                    d.cntry_nm,
                                    d.rate_type,
                                    d.from_ccy,
                                    d.to_ccy,
                                    d.valid_date,
                                    d.jj_year,
                                    d.start_period,
                                    CASE
                                        WHEN (d.end_mnth_id = b.max_period) THEN ('209912'::character varying)::text
                                        ELSE d.end_mnth_id
                                    END AS end_period,
                                    d.exch_rate
                                FROM (
                                        SELECT a.cntry_key,
                                            a.cntry_nm,
                                            a.rate_type,
                                            a.from_ccy,
                                            a.to_ccy,
                                            a.valid_date,
                                            a.jj_year,
                                            min((a.jj_mnth_id)::text) AS start_period,
                                            "max"((a.jj_mnth_id)::text) AS end_mnth_id,
                                            a.exch_rate
                                        FROM EDW_VW_my_CURR_DIM a
                                        WHERE (
                                                (a.cntry_key)::text = ('MY'::character varying)::text
                                            )
                                        GROUP BY a.cntry_key,
                                            a.cntry_nm,
                                            a.rate_type,
                                            a.from_ccy,
                                            a.to_ccy,
                                            a.valid_date,
                                            a.jj_year,
                                            a.exch_rate
                                    ) d,
                                    (
                                        SELECT "max"((a.jj_mnth_id)::text) AS max_period
                                        FROM EDW_VW_my_CURR_DIM a
                                        WHERE (
                                                (a.cntry_key)::text = ('MY'::character varying)::text
                                            )
                                    ) b
                            ) veocurd
                            WHERE (
                                (veossf.jj_mnth_id >= veocurd.start_period)
                                AND (veossf.jj_mnth_id <= veocurd.end_period)
                            )
)



--Final CTE

select * from ds_primary_null_cntd