with edw_perfect_store_rebase_wt as
(
    select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_PERFECT_STORE_REBASE_WT
),
edw_company_dim as
(
    select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_COMPANY_DIM
),
itg_query_parameters as
(
    select * from DEV_DNA_CORE.ASPITG_INTEGRATION.ITG_QUERY_PARAMETERS
),
trans as
(
    select
      to_char(scheduleddate, 'YYYYMM') AS year_month,
      to_char(scheduleddate, 'YYYY') AS year,
      CASE 
       WHEN country LIKE 'China%'
          THEN 'China'
        ELSE cd."cluster"
        END AS "cluster",
      CASE 
        WHEN country = 'China'
          THEN 'China PC'
        WHEN country = 'Japan'
          THEN 'JJKK'
        ELSE country
        END AS market,
      sum(msl_presence) AS msl_presence,
      sum(visits) AS visits,
      sum(msl_presence1) AS msl_presence1,
      sum(visits1) AS visits1,
      CASE 
        WHEN country = 'India'
          AND sum(visits1) <> 0
          THEN sum(msl_presence1) / sum(visits1)
        ELSE sum(msl_presence) / sum(visits)
        END AS ps
    FROM (
      SELECT country,
        kpi,
        scheduleddate,
        latestdate,
        channel,
        actual_value AS MSL_presence,
        ref_value AS visits,
        CASE 
          WHEN country = 'India'
            AND channel <> 'MT'
            AND kpi_ref_wt_val = 1
            THEN kpi_actual_wt_val
          ELSE 0
          END AS MSL_presence1,
        CASE 
          WHEN country = 'India'
            AND channel <> 'MT'
            AND kpi_ref_wt_val = 1
            THEN kpi_ref_val
          ELSE 0
          END AS visits1
      FROM edw_perfect_store_rebase_wt
      WHERE kpi = 'MSL COMPLIANCE'
        AND priority_store_flag = 'Y'
      ) msl
    LEFT JOIN (
      SELECT DISTINCT ctry_group,
        "cluster"
      FROM edw_company_dim
      ) cd ON MSL.country = cd.ctry_group
    WHERE to_char(scheduleddate, 'YYYYMM') <= CASE 
        WHEN extract(day FROM current_timestamp()) < (
            SELECT cast(parameter_value AS INTEGER)
            FROM itg_query_parameters
            WHERE country_code = 'ALL'
              AND parameter_name = 'okr_data_update_date'
            )
          THEN to_char(add_months(current_timestamp(), - 2), 'YYYYMM')
        ELSE to_char(add_months(current_timestamp(), - 1), 'YYYYMM')
        END
    GROUP BY cd."cluster",
      country,
      kpi,
      to_char(scheduleddate, 'YYYYMM'),
      to_char(scheduleddate, 'YYYY')

    UNION ALL

    SELECT to_char(scheduleddate, 'YYYYMM') AS year_month,
      to_char(scheduleddate, 'YYYY') AS year,
      'APAC' AS "cluster",
      NULL AS market,
      sum(actual_value) AS msl_presence,
      sum(ref_value) AS visits,
      0 AS msl_presence1,
      0 AS visits1,
      sum(actual_value) / sum(ref_value) AS ps
    FROM edw_perfect_store_rebase_wt
    WHERE kpi = 'MSL COMPLIANCE'
      AND priority_store_flag = 'Y'
      AND to_char(scheduleddate, 'YYYYMM') <= CASE 
        WHEN extract(day FROM current_timestamp()) < (
            SELECT cast(parameter_value AS INTEGER)
            FROM itg_query_parameters
            WHERE country_code = 'ALL'
              AND parameter_name = 'okr_data_update_date'
            )
          THEN to_char(add_months(current_timestamp(), - 2), 'YYYYMM')
        ELSE to_char(add_months(current_timestamp(), - 1), 'YYYYMM')
        END
    GROUP BY to_char(scheduleddate, 'YYYYMM'),
      to_char(scheduleddate, 'YYYY')

    UNION ALL  --"cluster" level

    SELECT TO_CHAR(scheduleddate, 'YYYYMM') AS year_month,
      TO_CHAR(scheduleddate, 'YYYY') AS year,
      CASE 
        WHEN country LIKE 'China%'
          THEN 'China'
        ELSE cd."cluster"
        END AS "cluster",
      NULL AS market,
      SUM(msl_presence) AS msl_presence,
      SUM(visits) AS visits,
      0 AS msl_presence1,
      0 AS visits1,
      SUM(msl_presence) / SUM(visits) AS ps
    FROM (
      SELECT country,
        kpi,
        scheduleddate,
        latestdate,
        channel,
        actual_value AS MSL_presence,
        ref_value AS visits
      FROM edw_perfect_store_rebase_wt
      WHERE kpi = 'MSL COMPLIANCE'
        AND priority_store_flag = 'Y'
        AND country <> 'India'
      ) msl
    LEFT JOIN (
      SELECT DISTINCT ctry_group,
        "cluster"
      FROM edw_company_dim
      ) cd ON MSL.country = cd.ctry_group
    WHERE TO_CHAR(scheduleddate, 'YYYYMM') <= CASE 
        WHEN extract(day FROM current_timestamp()) < (
            SELECT cast(parameter_value AS INTEGER)
            FROM itg_query_parameters
            WHERE country_code = 'ALL'
              AND parameter_name = 'okr_data_update_date'
            )
          THEN to_char(add_months(current_timestamp(), - 2), 'YYYYMM')
        ELSE to_char(add_months(current_timestamp(), - 1), 'YYYYMM')
        END
    GROUP BY CASE 
        WHEN country LIKE 'China%'
          THEN 'China'
        ELSE cd."cluster"
        END,
      kpi,
      TO_CHAR(scheduleddate, 'YYYYMM'),
      TO_CHAR(scheduleddate, 'YYYY')   
),
final as
(
    select
    year_month::varchar(14) as year_month,
	year::varchar(12) as year,
	"cluster"::varchar(100) as cluster,
	market::varchar(200) as market,
	msl_presence::number(38,4) as msl_presence,
	visits::number(38,4) as visits,
	msl_presence1::number(38,4) as msl_presence1,
	visits1::number(38,4) as visits1,
	ps::NUMBER(38,4) as ps
    from trans
)
select * from final