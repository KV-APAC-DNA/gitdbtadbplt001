with edw_rpt_ecomm_oneview as
(
    select * from {{ ref('aspedw_integration__edw_rpt_ecomm_oneview') }}
),
edw_okr_gfo_map as
(
    select * from {{source('aspedw_integration', 'edw_okr_gfo_map')}}
),
trans as
(
    SELECT data_type,
      kpi,
      year_month,
      fisc_year,
      qtr as quarter,
      brand,
      franchise,
      cluster,
      CASE 
        WHEN market IN ('China OTC', 'China Selfcare')
          THEN 'China Self'
        WHEN market IN ('China Skincare', 'China Personal Care')
          THEN 'China PC'
        WHEN market = 'Japan'
          THEN 'JJKK'
        ELSE market
        END market,
      NULL AS NTS_GRWNG_SHARE_SIZE, 
      actual_value,
      bp as bp_target,
      ju as ju_target,
      nu as nu_target,
      NULL AS YTD_ACTUAL,
      NULL AS YTD_BP_TARGET,
      NULL AS YTD_JU_TARGET,
      NULL AS YTD_NU_TARGET
    FROM (
      SELECT 'Target' AS data_type,
        'eCommerce_NTS' AS kpi, -- APAC and year level
        NULL AS year_month,
        fisc_year,
        NULL AS qtr,
        NULL AS brand,
        NULL AS franchise,
        'APAC' AS cluster,
        NULL AS market,
        NULL AS actual_value,
        CASE 
          WHEN additional_information = 'FBP'
            THEN sum(target_value)
          ELSE NULL
          END AS bp,
        CASE 
          WHEN additional_information = 'JU'
            THEN sum(target_value)
          ELSE NULL
          END AS ju,
        CASE 
          WHEN additional_information = 'NU'
            THEN sum(target_value)
          ELSE NULL
          END AS nu
      FROM edw_rpt_ecomm_oneview
      WHERE kpi = 'Target_NTS'
        AND additional_information <> 'Stretch'
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year,
        additional_information
    
      UNION ALL
    
      SELECT 'Target' AS data_type,
        'eCommerce_NTS' AS kpi, -- cluster and year level
        NULL AS year_month,
        fisc_year,
        NULL AS qtr,
        NULL AS brand,
        NULL AS franchise,
        cluster,
        NULL AS market,
        NULL AS actual_value,
        CASE 
          WHEN additional_information = 'FBP'
            THEN sum(target_value)
          ELSE NULL
          END AS bp,
        CASE 
          WHEN additional_information = 'JU'
            THEN sum(target_value)
          ELSE NULL
          END AS ju,
        CASE 
          WHEN additional_information = 'NU'
            THEN sum(target_value)
          ELSE NULL
          END AS nu
      FROM (
        SELECT fisc_year,
          additional_information,
          cluster,
          sum(target_value) AS target_value
        FROM edw_rpt_ecomm_oneview
        WHERE kpi = 'Target_NTS'
          AND additional_information <> 'Stretch'
          AND fisc_year > (date_part(year, current_timestamp()) - 3)
          AND upper(cluster) NOT IN ('PACIFIC', 'INDIA')
        GROUP BY fisc_year,
          additional_information,
          cluster
        )
      GROUP BY fisc_year,
        cluster,
        additional_information
    
      UNION ALL
    
      SELECT 'Target' AS data_type,
        'eCommerce_NTS' AS kpi, -- market and year level
        NULL AS year_month,
        fisc_year,
        NULL AS qtr,
        NULL AS brand,
        NULL AS franchise,
        cluster,
        market,
        NULL AS actual_value,
        CASE 
          WHEN additional_information = 'FBP'
            THEN sum(target_value)
          ELSE NULL
          END AS bp,
        CASE 
          WHEN additional_information = 'JU'
            THEN sum(target_value)
          ELSE NULL
          END AS ju,
        CASE 
          WHEN additional_information = 'NU'
            THEN sum(target_value)
          ELSE NULL
          END AS nu
      FROM edw_rpt_ecomm_oneview
      WHERE kpi = 'Target_NTS'
        AND additional_information <> 'Stretch'
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year,
        cluster,
        market,
        additional_information
    
      UNION ALL ---APAC and month level
    
      SELECT 'Target' AS data_type,
        'eCommerce_NTS' AS kpi,
        (
          fisc_year || CASE 
            WHEN cast(fisc_month AS INT) < 10
              THEN (0 || fisc_month)
            ELSE fisc_month
            END
          ) year_month,
        fisc_year,
        CASE 
          WHEN cast(fisc_month AS INTEGER) BETWEEN 1
              AND 3
            THEN 1
          WHEN cast(fisc_month AS INTEGER) BETWEEN 4
              AND 6
            THEN 2
          WHEN cast(fisc_month AS INTEGER) BETWEEN 7
              AND 9
            THEN 3
          ELSE 4
          END AS qtr,
        NULL AS brand,
        NULL AS franchise,
        'APAC' AS cluster,
        NULL AS market,
        NULL AS actual_value,
        CASE 
          WHEN additional_information = 'FBP'
            THEN sum(target_value)
          ELSE NULL
          END AS bp,
        CASE 
          WHEN additional_information = 'JU'
            THEN sum(target_value)
          ELSE NULL
          END AS ju,
        CASE 
          WHEN additional_information = 'NU'
            THEN sum(target_value)
          ELSE NULL
          END AS nu
      FROM edw_rpt_ecomm_oneview
      WHERE kpi = 'Target_NTS'
        AND additional_information <> 'Stretch'
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year,
        fisc_month,
        additional_information
    
      UNION ALL ---cluster and month level
    
      SELECT 'Target' AS data_type,
        'eCommerce_NTS' AS kpi,
        (
          fisc_year || CASE 
            WHEN cast(fisc_month AS INT) < 10
              THEN (0 || fisc_month)
            ELSE fisc_month
            END
          ) year_month,
        fisc_year,
        CASE 
          WHEN cast(fisc_month AS INTEGER) BETWEEN 1
              AND 3
            THEN 1
          WHEN cast(fisc_month AS INTEGER) BETWEEN 4
              AND 6
            THEN 2
          WHEN cast(fisc_month AS INTEGER) BETWEEN 7
              AND 9
            THEN 3
          ELSE 4
          END AS qtr,
        NULL AS brand,
        NULL AS franchise,
        cluster,
        NULL AS market,
        NULL AS actual_value,
        CASE 
          WHEN additional_information = 'FBP'
            THEN sum(target_value)
          ELSE NULL
          END AS bp,
        CASE 
          WHEN additional_information = 'JU'
            THEN sum(target_value)
          ELSE NULL
          END AS ju,
        CASE 
          WHEN additional_information = 'NU'
            THEN sum(target_value)
          ELSE NULL
          END AS nu
      FROM (
        SELECT fisc_year,
          fisc_month,
          additional_information,
          cluster,
          sum(target_value) AS target_value
        FROM edw_rpt_ecomm_oneview
        WHERE kpi = 'Target_NTS'
          AND additional_information <> 'Stretch'
          AND fisc_year > (date_part(year, current_timestamp()) - 3)
          AND upper(cluster) NOT IN ('PACIFIC', 'INDIA')
        GROUP BY fisc_year,
          fisc_month,
          additional_information,
          cluster
        )
      GROUP BY fisc_year,
        fisc_month,
        cluster,
        additional_information
    
      UNION ALL ---market and month level
    
      SELECT 'Target' AS data_type,
        'eCommerce_NTS' AS kpi,
        (
          fisc_year || CASE 
            WHEN cast(fisc_month AS INT) < 10
              THEN (0 || fisc_month)
            ELSE fisc_month
            END
          ) year_month,
        fisc_year,
        CASE 
          WHEN cast(fisc_month AS INTEGER) BETWEEN 1
              AND 3
            THEN 1
          WHEN cast(fisc_month AS INTEGER) BETWEEN 4
              AND 6
            THEN 2
          WHEN cast(fisc_month AS INTEGER) BETWEEN 7
              AND 9
            THEN 3
          ELSE 4
          END AS qtr,
        NULL AS brand,
        NULL AS franchise,
        cluster,
        market,
        NULL AS actual_value,
        CASE 
          WHEN additional_information = 'FBP'
            THEN sum(target_value)
          ELSE NULL
          END AS bp,
        CASE 
          WHEN additional_information = 'JU'
            THEN sum(target_value)
          ELSE NULL
          END AS ju,
        CASE 
          WHEN additional_information = 'NU'
            THEN sum(target_value)
          ELSE NULL
          END AS nu
      FROM edw_rpt_ecomm_oneview
      WHERE kpi = 'Target_NTS'
        AND additional_information <> 'Stretch'
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year,
        fisc_month,
        cluster,
        market,
        additional_information
    
      UNION ALL
    
      SELECT 'Target' AS data_type, -- APAC , segment and year
        'eCommerce_NTS' AS kpi,
        NULL AS year_month,
        fisc_year,
        NULL AS qtr,
        NULL AS brand,
        gfo AS franchise,
        'APAC' AS cluster,
        NULL AS market,
        NULL AS actual_value,
        CASE 
          WHEN additional_information = 'FBP'
            THEN sum(target_value)
          ELSE NULL
          END AS bp,
        CASE 
          WHEN additional_information = 'JU'
            THEN sum(target_value)
          ELSE NULL
          END AS ju,
        CASE 
          WHEN additional_information = 'NU'
            THEN sum(target_value)
          ELSE NULL
          END AS nu
      FROM edw_rpt_ecomm_oneview fact
      LEFT JOIN edw_okr_gfo_map map ON map.franchise = fact.prod_hier_l1
        AND map.need_state = fact.prod_hier_l2
        AND map.measure = 'eCommerce_NTS'
      WHERE kpi = 'Target_NTS'
        AND additional_information <> 'Stretch'
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year,
        fisc_month,
        additional_information,
        gfo
    
      UNION ALL
    
      SELECT 'Target' AS data_type, -- cluster , segment and year
        'eCommerce_NTS' AS kpi,
        NULL AS year_month,
        fisc_year,
        NULL AS qtr,
        NULL AS brand,
        gfo AS franchise,
        cluster,
        NULL AS market,
        NULL AS actual_value,
        CASE 
          WHEN additional_information = 'FBP'
            THEN sum(target_value)
          ELSE NULL
          END AS bp,
        CASE 
          WHEN additional_information = 'JU'
            THEN sum(target_value)
          ELSE NULL
          END AS ju,
        CASE 
          WHEN additional_information = 'NU'
            THEN sum(target_value)
          ELSE NULL
          END AS nu
      FROM (
        SELECT cluster,
          prod_hier_l1,
          prod_hier_l2,
          fisc_year,
          additional_information,
          sum(target_value) AS target_value
        FROM edw_rpt_ecomm_oneview
        WHERE kpi = 'Target_NTS'
          AND additional_information <> 'Stretch'
          AND upper(cluster) NOT IN ('PACIFIC', 'INDIA')
          AND fisc_year > (date_part(year, current_timestamp()) - 3)
        GROUP BY cluster,
          fisc_year,
          additional_information,
          prod_hier_l1,
          prod_hier_l2
        ) fact
      LEFT JOIN edw_okr_gfo_map map ON map.franchise = fact.prod_hier_l1
        AND map.need_state = fact.prod_hier_l2
        AND map.measure = 'eCommerce_NTS'
      GROUP BY fisc_year,
        additional_information,
        gfo,
        cluster
    
      UNION ALL
    
      SELECT 'Target' AS data_type, -- market , segment and year
        'eCommerce_NTS' AS kpi,
        NULL AS year_month,
        fisc_year,
        NULL AS qtr,
        NULL AS brand,
        gfo AS franchise,
        cluster,
        market,
        NULL AS actual_value,
        CASE 
          WHEN additional_information = 'FBP'
            THEN sum(target_value)
          ELSE NULL
          END AS bp,
        CASE 
          WHEN additional_information = 'JU'
            THEN sum(target_value)
          ELSE NULL
          END AS ju,
        CASE 
          WHEN additional_information = 'NU'
            THEN sum(target_value)
          ELSE NULL
          END AS nu
      FROM edw_rpt_ecomm_oneview fact
      LEFT JOIN edw_okr_gfo_map map ON map.franchise = fact.prod_hier_l1
        AND map.need_state = fact.prod_hier_l2
        AND map.measure = 'eCommerce_NTS'
      WHERE kpi = 'Target_NTS'
        AND additional_information <> 'Stretch'
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year,
        fisc_month,
        additional_information,
        gfo,
        cluster,
        market
    
      UNION ALL
    
      SELECT 'Target' AS data_type, -- APAC , segment and month
        'eCommerce_NTS' AS kpi,
        (
          fisc_year || CASE 
            WHEN cast(fisc_month AS INT) < 10
              THEN (0 || fisc_month)
            ELSE fisc_month
            END
          ) year_month,
        fisc_year,
        CASE 
          WHEN cast(fisc_month AS INTEGER) BETWEEN 1
              AND 3
            THEN 1
          WHEN cast(fisc_month AS INTEGER) BETWEEN 4
              AND 6
            THEN 2
          WHEN cast(fisc_month AS INTEGER) BETWEEN 7
              AND 9
            THEN 3
          ELSE 4
          END AS qtr,
        NULL AS brand,
        gfo AS franchise,
        'APAC' AS cluster,
        NULL AS market,
        NULL AS actual_value,
        CASE 
          WHEN additional_information = 'FBP'
            THEN sum(target_value)
          ELSE NULL
          END AS bp,
        CASE 
          WHEN additional_information = 'JU'
            THEN sum(target_value)
          ELSE NULL
          END AS ju,
        CASE 
          WHEN additional_information = 'NU'
            THEN sum(target_value)
          ELSE NULL
          END AS nu
      FROM edw_rpt_ecomm_oneview fact
      LEFT JOIN edw_okr_gfo_map map ON map.franchise = fact.prod_hier_l1
        AND map.need_state = fact.prod_hier_l2
        AND map.measure = 'eCommerce_NTS'
      WHERE kpi = 'Target_NTS'
        AND additional_information <> 'Stretch'
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year,
        fisc_month,
        additional_information,
        gfo
    
      UNION ALL
    
      SELECT 'Target' AS data_type, -- cluster , segment and month
        'eCommerce_NTS' AS kpi,
        (
          fisc_year || CASE 
            WHEN cast(fisc_month AS INT) < 10
              THEN (0 || fisc_month)
            ELSE fisc_month
            END
          ) year_month,
        fisc_year,
        CASE 
          WHEN cast(fisc_month AS INTEGER) BETWEEN 1
              AND 3
            THEN 1
          WHEN cast(fisc_month AS INTEGER) BETWEEN 4
              AND 6
            THEN 2
          WHEN cast(fisc_month AS INTEGER) BETWEEN 7
              AND 9
            THEN 3
          ELSE 4
          END AS qtr,
        NULL AS brand,
        gfo AS franchise,
        cluster,
        NULL AS market,
        NULL AS actual_value,
        CASE 
          WHEN additional_information = 'FBP'
            THEN sum(target_value)
          ELSE NULL
          END AS bp,
        CASE 
          WHEN additional_information = 'JU'
            THEN sum(target_value)
          ELSE NULL
          END AS ju,
        CASE 
          WHEN additional_information = 'NU'
            THEN sum(target_value)
          ELSE NULL
          END AS nu
      FROM edw_rpt_ecomm_oneview fact
      LEFT JOIN edw_okr_gfo_map map ON map.franchise = fact.prod_hier_l1
        AND map.need_state = fact.prod_hier_l2
        AND map.measure = 'eCommerce_NTS'
      WHERE kpi = 'Target_NTS'
        AND additional_information <> 'Stretch'
        AND upper(cluster) NOT IN ('PACIFIC', 'INDIA')
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year,
        fisc_month,
        additional_information,
        gfo,
        cluster
    
      UNION ALL
    
      SELECT 'Target' AS data_type, -- market , segment and month
        'eCommerce_NTS' AS kpi,
        (
          fisc_year || CASE 
            WHEN cast(fisc_month AS INT) < 10
              THEN (0 || fisc_month)
            ELSE fisc_month
            END
          ) year_month,
        fisc_year,
        CASE 
          WHEN cast(fisc_month AS INTEGER) BETWEEN 1
              AND 3
            THEN 1
          WHEN cast(fisc_month AS INTEGER) BETWEEN 4
              AND 6
            THEN 2
          WHEN cast(fisc_month AS INTEGER) BETWEEN 7
              AND 9
            THEN 3
          ELSE 4
          END AS qtr,
        NULL AS brand,
        gfo AS franchise,
        cluster,
        market,
        NULL AS actual_value,
        CASE 
          WHEN additional_information = 'FBP'
            THEN sum(target_value)
          ELSE NULL
          END AS bp,
        CASE 
          WHEN additional_information = 'JU'
            THEN sum(target_value)
          ELSE NULL
          END AS ju,
        CASE 
          WHEN additional_information = 'NU'
            THEN sum(target_value)
          ELSE NULL
          END AS nu
      FROM edw_rpt_ecomm_oneview fact
      LEFT JOIN edw_okr_gfo_map map ON map.franchise = fact.prod_hier_l1
        AND map.need_state = fact.prod_hier_l2
        AND map.measure = 'eCommerce_NTS'
      WHERE kpi = 'Target_NTS'
        AND additional_information <> 'Stretch'
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year,
        fisc_month,
        additional_information,
        gfo,
        cluster,
        market
      )
),
final as
(   
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