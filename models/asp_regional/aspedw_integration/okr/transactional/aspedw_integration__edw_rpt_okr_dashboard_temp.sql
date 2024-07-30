with edw_ecommerce_6pai as
(
    select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_ECOMMERCE_6PAI
),
itg_mds_ap_sales_ops_map as
(
    select * from DEV_DNA_CORE.ASPITG_INTEGRATION.ITG_MDS_AP_SALES_OPS_MAP
),
edw_okr_brand_map as
(
    select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_OKR_BRAND_MAP
),
itg_query_parameters as
(
    select * from dev_dna_core.aspitg_integration.itg_query_parameters
),
trans as
(
    SELECT 
      datatype AS data_type,
      kpi,
      year_month,
      fisc_year,
      quarter,
      brand,
      franchise,
      cluster,
      market,
      NULL AS NTS_GRWNG_SHARE_SIZE,
      actual_value,
      NULL AS BP_TARGET,
      NULL AS JU_TARGET,
      NULL AS NU_TARGET,
      NULL AS YTD_ACTUAL,
      NULL AS YTD_BP_TARGET,
      NULL AS YTD_JU_TARGET,
      NULL AS YTD_NU_TARGET
    FROM (
      SELECT 'Actual' AS datatype, -- APAC level
        '6PAI' AS kpi,
        CASE 
          WHEN cast(month AS INTEGER) < 10
            THEN (year || '0' || month)
          ELSE (year || month)
          END AS year_month,
        year AS fisc_year,
        CASE 
          WHEN cast(month AS INTEGER) < 4
            THEN 1
          WHEN cast(month AS INTEGER) BETWEEN 4
              AND 6
            THEN 2
          WHEN cast(month AS INTEGER) BETWEEN 6
              AND 9
            THEN 3
          ELSE 4
          END AS quarter,
        NULL AS brand,
        NULL AS franchise,
        'APAC' AS cluster,
        NULL AS market,
        round(score_non_weighted / 100, 2) AS actual_value
      FROM edw_ecommerce_6pai
      WHERE kpi = 'Share of Search (including paid search)'
        AND market = 'Regional'
        AND detail = 'All'
        AND source = 'Retail Level'
    
      UNION ALL
    
      SELECT 'Actual' AS datatype, ---market level
        '6PAI' AS kpi,
        CASE 
          WHEN cast(month AS INTEGER) < 10
            THEN (year || '0' || month)
          ELSE (year || month)
          END AS year_month,
        year AS fisc_year,
        CASE 
          WHEN cast(month AS INTEGER) < 4
            THEN 1
          WHEN cast(month AS INTEGER) BETWEEN 4
              AND 6
            THEN 2
          WHEN cast(month AS INTEGER) BETWEEN 6
              AND 9
            THEN 3
          ELSE 4
          END AS quarter,
        NULL AS brand,
        NULL AS franchise,
        CASE 
          WHEN market IN ('Japan-DCL', 'China-OTC')
            THEN cluster
          ELSE mkt_map.destination_cluster
          END AS cluster,
        CASE 
          WHEN market = 'China-OTC'
            THEN 'China Self'
          WHEN market = 'HongKong'
            THEN 'Hong Kong'
          WHEN market = 'China'
            THEN 'China PC'
          WHEN market = 'Japan-DCL'
            THEN 'Japan DCL'
          WHEN market = 'Japan'
            THEN 'JJKK'
          ELSE market
          END AS market,
        round((sum(score_weighted) / sum(salesweight) / 100), 2) AS actual_value
      FROM edw_ecommerce_6pai fact
      LEFT JOIN (
        SELECT DISTINCT destination_cluster,
          source_market
        FROM itg_mds_ap_sales_ops_map
        WHERE dataset = 'Market Share QSD'
        ) mkt_map ON upper(mkt_map.source_market) = upper(CASE 
            WHEN market = 'Korea'
              THEN 'South Korea'
            ELSE fact.market
            END)
      WHERE kpi = 'Share of Search (including paid search)'
        AND upper(detail) NOT LIKE '%TOTAL'
        AND source = 'Retail Level'
        AND market NOT IN ('China-Travel-Retail', 'Regional', 'HongKong', 'China-OTC')
        AND score_weighted <> 0
        AND score_weighted IS NOT NULL
      GROUP BY month,
        year,
        mkt_map.destination_cluster,
        cluster,
        market
    
      UNION ALL
    
      SELECT 'Actual' AS datatype, ---market level
        '6PAI' AS kpi,
        CASE 
          WHEN cast(month AS INTEGER) < 10
            THEN (year || '0' || month)
          ELSE (year || month)
          END AS year_month,
        year AS fisc_year,
        CASE 
          WHEN cast(month AS INTEGER) < 4
            THEN 1
          WHEN cast(month AS INTEGER) BETWEEN 4
              AND 6
            THEN 2
          WHEN cast(month AS INTEGER) BETWEEN 6
              AND 9
            THEN 3
          ELSE 4
          END AS quarter,
        NULL AS brand,
        NULL AS franchise,
        'Metropolitan Asia' AS cluster,
        'Hong Kong',
        round(score_non_weighted / 100, 2) AS actual_value
      FROM edw_ecommerce_6pai
      WHERE kpi = 'Share of Search (including paid search)'
        AND upper(detail) LIKE 'HKTVMALL%'
        AND source = 'Retail Level'
        AND market = 'HongKong'
    
      UNION ALL
    
      SELECT 'Actual' AS datatype, ---market level
        '6PAI' AS kpi,
        CASE 
          WHEN cast(month AS INTEGER) < 10
            THEN (year || '0' || month)
          ELSE (year || month)
          END AS year_month,
        year AS fisc_year,
        CASE 
          WHEN cast(month AS INTEGER) < 4
            THEN 1
          WHEN cast(month AS INTEGER) BETWEEN 4
              AND 6
            THEN 2
          WHEN cast(month AS INTEGER) BETWEEN 6
              AND 9
            THEN 3
          ELSE 4
          END AS quarter,
        NULL AS brand,
        NULL AS franchise,
        cluster,
        'China Self' AS market,
        round(score_non_weighted / 100, 2) AS actual_value
      FROM edw_ecommerce_6pai
      WHERE kpi = 'Share of Search (including paid search)'
        AND upper(detail) LIKE '%TOTAL%'
        AND source = 'Retail Level'
        AND market = 'China-OTC'
    
      UNION ALL
    
      SELECT datatype, -- cluster level
        kpi,
        year_month,
        fisc_year,
        quarter,
        brand,
        franchise,
        cluster,
        NULL AS market,
        round(avg(actual_value), 2) AS actual_value
      FROM (
        SELECT 'Actual' AS datatype, ---market level
          '6PAI' AS kpi,
          CASE 
            WHEN cast(month AS INTEGER) < 10
              THEN (year || '0' || month)
            ELSE (year || month)
            END AS year_month,
          year AS fisc_year,
          CASE 
            WHEN cast(month AS INTEGER) < 4
              THEN 1
            WHEN cast(month AS INTEGER) BETWEEN 4
                AND 6
              THEN 2
            WHEN cast(month AS INTEGER) BETWEEN 6
                AND 9
              THEN 3
            ELSE 4
            END AS quarter,
          NULL AS brand,
          NULL AS franchise,
          CASE 
            WHEN market IN ('Japan-DCL', 'China-OTC')
              THEN cluster
            ELSE mkt_map.destination_cluster
            END AS cluster,
          CASE 
            WHEN market = 'China-OTC'
              THEN 'China Self'
            WHEN market = 'HongKong'
              THEN 'Hong Kong'
            WHEN market = 'China'
              THEN 'China PC'
            WHEN market = 'Japan-DCL'
              THEN 'Japan DCL'
            WHEN market = 'Japan'
              THEN 'JJKK'
            ELSE market
            END AS market,
          round((sum(score_weighted) / sum(salesweight) / 100), 2) AS actual_value
        FROM edw_ecommerce_6pai fact
        LEFT JOIN (
          SELECT DISTINCT destination_cluster,
            source_market
          FROM itg_mds_ap_sales_ops_map
          WHERE dataset = 'Market Share QSD'
          ) mkt_map ON upper(mkt_map.source_market) = upper(CASE 
              WHEN market = 'Korea'
                THEN 'South Korea'
              ELSE fact.market
              END)
        WHERE kpi = 'Share of Search (including paid search)'
          AND upper(detail) NOT LIKE '%TOTAL'
          AND source = 'Retail Level'
          AND market NOT IN ('China-Travel-Retail', 'Regional', 'HongKong', 'China-OTC', 'India')
          AND score_weighted <> 0
          AND score_weighted IS NOT NULL
        GROUP BY month,
          year,
          destination_cluster,
          cluster,
          market

        UNION ALL

        SELECT 'Actual' AS datatype, ---market level
          '6PAI' AS kpi,
          CASE 
            WHEN cast(month AS INTEGER) < 10
              THEN (year || '0' || month)
            ELSE (year || month)
            END AS year_month,
          year AS fisc_year,
          CASE 
            WHEN cast(month AS INTEGER) < 4
              THEN 1
            WHEN cast(month AS INTEGER) BETWEEN 4
                AND 6
              THEN 2
            WHEN cast(month AS INTEGER) BETWEEN 6
                AND 9
              THEN 3
            ELSE 4
            END AS quarter,
          NULL AS brand,
          NULL AS franchise,
          'Metropolitan Asia' AS cluster,
          'Hong Kong',
          round(score_non_weighted / 100, 2) AS actual_value
        FROM edw_ecommerce_6pai
        WHERE kpi = 'Share of Search (including paid search)'
          AND upper(detail) LIKE 'HKTVMALL%'
          AND source = 'Retail Level'
          AND market = 'HongKong'

        UNION ALL

        SELECT 'Actual' AS datatype, ---market level
          '6PAI' AS kpi,
          CASE 
            WHEN cast(month AS INTEGER) < 10
              THEN (year || '0' || month)
            ELSE (year || month)
            END AS year_month,
          year AS fisc_year,
          CASE 
            WHEN cast(month AS INTEGER) < 4
              THEN 1
            WHEN cast(month AS INTEGER) BETWEEN 4
                AND 6
              THEN 2
            WHEN cast(month AS INTEGER) BETWEEN 6
                AND 9
              THEN 3
            ELSE 4
            END AS quarter,
          NULL AS brand,
          NULL AS franchise,
          cluster,
          'China Self' AS market,
          round(score_non_weighted / 100, 2) AS actual_value
        FROM edw_ecommerce_6pai
        WHERE kpi = 'Share of Search (including paid search)'
          AND upper(detail) LIKE '%TOTAL%'
          AND source = 'Retail Level'
          AND market = 'China-OTC'
        )
      WHERE Cluster NOT IN ('India')
      GROUP BY datatype, -- cluster level
        kpi,
        year_month,
        fisc_year,
        quarter,
        brand,
        franchise,
        cluster
    
      UNION ALL
    
      SELECT 'Actual' AS datatype, ---brand level
        '6PAI' AS kpi,
        CASE 
          WHEN cast(month AS INTEGER) < 10
            THEN (year || '0' || month)
          ELSE (year || month)
          END AS year_month,
        year AS fisc_year,
        CASE 
          WHEN cast(month AS INTEGER) < 4
            THEN 1
          WHEN cast(month AS INTEGER) BETWEEN 4
              AND 6
            THEN 2
          WHEN cast(month AS INTEGER) BETWEEN 6
              AND 9
            THEN 3
          ELSE 4
          END AS quarter,
        CASE 
          WHEN franchise = 'Johnson''s'
            THEN 'Johnson''s Baby'
          WHEN franchise = 'DCL'
            THEN 'Dr. Ci: Labo'
          ELSE franchise
          END AS brand,
        map.segment AS franchise,
        CASE 
          WHEN market IN ('Japan-DCL', 'China-OTC')
            THEN cluster
          ELSE mkt_map.destination_cluster
          END AS cluster,
        CASE 
          WHEN market = 'China-OTC'
            THEN 'China Self'
          WHEN market = 'China'
            THEN 'China PC'
          WHEN market = 'Japan-DCL'
            THEN 'Japan DCL'
          WHEN market = 'Japan'
            THEN 'JJKK'
          ELSE market
          END AS market,
        score_non_weighted / 100 AS actual_value
      FROM edw_ecommerce_6pai fact
      LEFT JOIN edw_okr_brand_map map ON upper(map.brand) = upper(CASE 
            WHEN fact.franchise = 'Johnson''s'
              THEN 'Johnson''s Baby'
            WHEN fact.franchise = 'DCL'
              THEN 'Dr. Ci: Labo'
            ELSE fact.franchise
            END)
      LEFT JOIN (
        SELECT DISTINCT destination_cluster,
          source_market
        FROM itg_mds_ap_sales_ops_map
        WHERE dataset = 'Market Share QSD'
        ) mkt_map ON upper(mkt_map.source_market) = upper(CASE 
            WHEN market = 'Korea'
              THEN 'South Korea'
            ELSE fact.market
            END)
      WHERE kpi = 'Share of Search (including paid search)'
        AND upper(detail) LIKE '%TOTAL'
        AND source = 'SOS BMC'
        AND market NOT IN ('China-Travel-Retail')
    
      UNION ALL
    
      SELECT 'Actual' AS datatype, ---market and segment level
        '6PAI' AS kpi,
        CASE 
          WHEN cast(month AS INTEGER) < 10
            THEN (year || '0' || month)
          ELSE (year || month)
          END AS year_month,
        year AS fisc_year,
        CASE 
          WHEN cast(month AS INTEGER) < 4
            THEN 1
          WHEN cast(month AS INTEGER) BETWEEN 4
              AND 6
            THEN 2
          WHEN cast(month AS INTEGER) BETWEEN 6
              AND 9
            THEN 3
          ELSE 4
          END AS quarter,
        NULL AS brand,
        map.segment AS franchise,
        CASE 
          WHEN market IN ('Japan-DCL', 'China-OTC')
            THEN cluster
          ELSE mkt_map.destination_cluster
          END AS cluster,
        CASE 
          WHEN market = 'China-OTC'
            THEN 'China Self'
          WHEN market = 'China'
            THEN 'China PC'
          WHEN market = 'Japan-DCL'
            THEN 'Japan DCL'
          WHEN market = 'Japan'
            THEN 'JJKK'
          ELSE market
          END AS market,
        avg(score_non_weighted) / 100 AS actual_value
      FROM edw_ecommerce_6pai fact
      LEFT JOIN edw_okr_brand_map map ON upper(map.brand) = upper(CASE 
            WHEN fact.franchise = 'Johnson''s'
              THEN 'Johnson''s Baby'
            WHEN fact.franchise = 'DCL'
              THEN 'Dr. Ci: Labo'
            ELSE fact.franchise
            END)
      LEFT JOIN (
        SELECT DISTINCT destination_cluster,
          source_market
        FROM itg_mds_ap_sales_ops_map
        WHERE dataset = 'Market Share QSD'
        ) mkt_map ON upper(mkt_map.source_market) = upper(CASE 
            WHEN market = 'Korea'
              THEN 'South Korea'
            ELSE fact.market
            END)
      WHERE kpi = 'Share of Search (including paid search)'
        AND upper(detail) LIKE '%TOTAL'
        AND source = 'SOS BMC'
        AND market NOT IN ('China-Travel-Retail')
      GROUP BY month,
        year,
        market,
        cluster,
        mkt_map.destination_cluster,
        map.segment
    
      UNION ALL
    
      SELECT 'Actual' AS datatype, ---cluster and segment  level
        '6PAI' AS kpi,
        CASE 
          WHEN cast(month AS INTEGER) < 10
            THEN (year || '0' || month)
          ELSE (year || month)
          END AS year_month,
        year AS fisc_year,
        CASE 
          WHEN cast(month AS INTEGER) < 4
            THEN 1
          WHEN cast(month AS INTEGER) BETWEEN 4
              AND 6
            THEN 2
          WHEN cast(month AS INTEGER) BETWEEN 6
              AND 9
            THEN 3
          ELSE 4
          END AS quarter,
        NULL AS brand,
        map.segment AS franchise,
        CASE 
          WHEN market IN ('Japan-DCL', 'China-OTC')
            THEN cluster
          ELSE mkt_map.destination_cluster
          END AS cluster,
        NULL AS market,
        avg(score_non_weighted) / 100 AS actual_value
      FROM edw_ecommerce_6pai fact
      LEFT JOIN edw_okr_brand_map map ON upper(map.brand) = upper(CASE 
            WHEN fact.franchise = 'Johnson''s'
              THEN 'Johnson''s Baby'
            WHEN fact.franchise = 'DCL'
              THEN 'Dr. Ci: Labo'
            ELSE fact.franchise
            END)
      LEFT JOIN (
        SELECT DISTINCT destination_cluster,
          source_market
        FROM itg_mds_ap_sales_ops_map
        WHERE dataset = 'Market Share QSD'
        ) mkt_map ON upper(mkt_map.source_market) = upper(CASE 
            WHEN market = 'Korea'
              THEN 'South Korea'
            ELSE fact.market
            END)
      WHERE kpi = 'Share of Search (including paid search)'
        AND upper(detail) LIKE '%TOTAL'
        AND source = 'SOS BMC'
        AND market NOT IN ('China-Travel-Retail', 'India')
      GROUP BY month,
        year,
        market,
        cluster,
        mkt_map.destination_cluster,
        map.segment
      )
    WHERE year_month <= CASE 
        WHEN extract(day FROM current_timestamp()) < (
            SELECT cast(parameter_value AS INTEGER)
            FROM itg_query_parameters
            WHERE country_code = 'ALL'
              AND parameter_name = 'okr_data_update_date'
            )
          THEN to_char(add_months(current_timestamp(), - 2), 'YYYYMM')
        ELSE to_char(add_months(current_timestamp(), - 1), 'YYYYMM')
        END      
),
final as
(
    select * from {{ ref('aspedw_integration__edw_rpt_okr_dashboard_temp1') }}
    UNION ALL
    select * from {{ ref('aspedw_integration__edw_rpt_okr_dashboard_temp2') }}
    UNION ALL
    select * from {{ ref('aspedw_integration__edw_rpt_okr_dashboard_temp3') }}
    UNION ALL
    select * from {{ ref('aspedw_integration__edw_rpt_okr_dashboard_temp4') }}
    UNION ALL
    select * from {{ ref('aspedw_integration__edw_rpt_okr_dashboard_temp5') }}
    UNION ALL
    select * from {{ ref('aspedw_integration__edw_rpt_okr_dashboard_temp6') }}
    UNION ALL
    select
    data_type::varchar(20) as data_type,
	kpi::varchar(100) as kpi,
	year_month::varchar(10) as year_month,
	fisc_year::varchar(10) as fisc_year,
	quarter::number(38,0) as quarter,
	brand::varchar(200) as brand,
	franchise::varchar(200) as franchise,
	cluster::varchar(100) as cluster,
	market::varchar(100) as market,
	nts_grwng_share_size::number(38,0) as nts_grwng_share_size,
	actual_value::number(38,4) as actual_value,
	bp_target::number(38,4) as bp_target,
	ju_target::number(38,4) as ju_target,
	nu_target::number(38,4) as nu_target,
	ytd_actual::number(38,4) as ytd_actual,	
	ytd_bp_target::number(38,4) as ytd_bp_target,
	ytd_ju_target::number(38,4) as ytd_ju_target,
	ytd_nu_target::number(38,4) as ytd_nu_target
    from trans
)
select * from final