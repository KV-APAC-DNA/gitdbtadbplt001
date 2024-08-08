with edw_rpt_ecomm_oneview as
(
    select * from {{ ref('aspedw_integration__edw_rpt_ecomm_oneview') }}
),
edw_okr_gfo_map as
(
    select * from {{source('aspedw_integration', 'edw_okr_gfo_map')}}
),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
itg_query_parameters as
(
    select * from {{source('aspitg_integration', 'itg_query_parameters')}}
),
trans as
(
    SELECT data_type,
      kpi,
      year_month,
      fisc_year,
      qtr,
      CASE 
        WHEN brand = 'Johnson''s'
          AND franchise = 'Essential Health'
          THEN 'Johnson''s Baby'
        WHEN brand = 'Johnson''s'
          AND franchise = 'Skin Health'
          THEN 'Johnson''s Adult'
        WHEN brand = ''
          THEN 'NA'
        ELSE brand
        END AS brand,
      CASE 
        WHEN brand = 'Listerine'
          THEN 'Essential Health'
        WHEN brand IN ('OGX', 'Vogue', 'Aveeno')
          THEN 'Skin Health'
        WHEN brand = 'Aveeno Baby'
          THEN 'Essential Health'
        ELSE franchise
        END AS franchise,
      cluster,
      CASE 
        WHEN market IN ('China OTC', 'China Selfcare')
          THEN 'China Self'
        WHEN market IN ('China Skincare', 'China Personal Care')
          THEN 'China PC'
        WHEN market = 'Japan'
          THEN 'JJKK'
        ELSE market
        END AS market,
      NULL as NTS_GRWNG_SHARE_SIZE,
      sum(actual_value) AS actual_value,
      sum(bp) AS bp,
      sum(ju) AS ju,
      sum(nu) AS nu,
      NULL AS YTD_ACTUAL,
      NULL AS YTD_BP_TARGET,
      NULL AS YTD_JU_TARGET,
      NULL AS YTD_NU_TARGET
    FROM (
      SELECT 'Actual' AS data_type,
        'eCommerce_NTS' AS kpi, -- APAC and year level
        NULL AS year_month,
        fisc_year,
        NULL AS qtr,
        NULL AS brand,
        NULL AS franchise,
        'APAC' AS cluster,
        NULL AS market,
        sum(value_usd) AS actual_value,
        NULL AS bp,
        NULL AS ju,
        NULL AS nu
      FROM edw_rpt_ecomm_oneview
      WHERE kpi = 'NTS'
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year
    
      UNION ALL
    
      SELECT 'Actual' AS data_type,
        'eCommerce_NTS' AS kpi, -- cluster and year level
        NULL AS year_month,
        fisc_year,
        NULL AS qtr,
        NULL AS brand,
        NULL AS franchise,
        cluster,
        NULL AS market,
        value_usd AS actual_value,
        NULL AS bp,
        NULL AS ju,
        NULL AS nu
      FROM (
        SELECT cluster,
          fisc_year,
          sum(value_usd) value_usd
        FROM edw_rpt_ecomm_oneview
        WHERE kpi = 'NTS'
          AND fisc_year > (date_part(year, current_timestamp()) - 3)
          AND upper(Cluster) NOT IN ('PACIFIC', 'INDIA')
        GROUP BY cluster,
          fisc_year
        )
    
      UNION ALL
    
      SELECT 'Actual' AS data_type,
        'eCommerce_NTS' AS kpi, -- market and year level
        NULL AS year_month,
        fisc_year,
        NULL AS qtr,
        NULL AS brand,
        NULL AS franchise,
        cluster,
        market,
        sum(value_usd) AS actual_value,
        NULL AS bp,
        NULL AS ju,
        NULL AS nu
      FROM edw_rpt_ecomm_oneview
      WHERE kpi = 'NTS'
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year,
        cluster,
        market
    
      UNION ALL ---APAC and month level
    
      SELECT 'Actual' AS data_type,
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
        sum(value_usd) AS actual_value,
        NULL AS bp,
        NULL AS ju,
        NULL AS nu
      FROM edw_rpt_ecomm_oneview
      WHERE kpi = 'NTS'
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year,
        fisc_month
    
      UNION ALL ---cluster and month level
    
      SELECT 'Actual' AS data_type,
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
        value_usd AS actual_value,
        NULL AS bp,
        NULL AS ju,
        NULL AS nu
      FROM (
        SELECT cluster,
          fisc_month,
          fisc_year,
          sum(value_usd) AS value_usd
        FROM edw_rpt_ecomm_oneview
        WHERE kpi = 'NTS'
          AND fisc_year > (date_part(year, current_timestamp()) - 3)
          AND upper(Cluster) NOT IN ('PACIFIC', 'INDIA')
        GROUP BY cluster,
          fisc_month,
          fisc_year
        )
    
      UNION ALL ---market and month level
    
      SELECT 'Actual' AS data_type,
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
        sum(value_usd) AS actual_value,
        NULL AS bp,
        NULL AS ju,
        NULL AS nu
      FROM edw_rpt_ecomm_oneview
      WHERE kpi = 'NTS'
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year,
        fisc_month,
        cluster,
        market
    
      UNION ALL -- APAC , segment and year level
    
      SELECT 'Actual' AS data_type,
        'eCommerce_NTS' AS kpi,
        NULL AS year_month,
        fisc_year,
        NULL AS qtr,
        NULL AS brand,
        coalesce(coalesce(map.gfo, prod_hier_l1), 'Others') AS franchise,
        'APAC' AS cluster,
        NULL AS market,
        sum(value_usd) AS actual_value,
        NULL AS bp,
        NULL AS ju,
        NULL AS nu
      FROM edw_rpt_ecomm_oneview fact
      --left join rg_edw.edw_gch_producthierarchy gph ON ltrim(fact.material_number,0) = ltrim(gph.materialnumber,0) and fact.material_number is not null
      LEFT JOIN (
        SELECT DISTINCT franchise,
          gfo
        FROM edw_okr_gfo_map
        WHERE measure = 'eCommerce_NTS'
        ) map ON map.franchise = CASE 
          WHEN fact.prod_hier_l1 IS NULL
            THEN 'Others'
          WHEN fact.prod_hier_l1 = ''
            THEN 'Others'
          ELSE prod_hier_l1
          END
      WHERE kpi = 'NTS'
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year,
        prod_hier_l1,
        map.gfo
    
      UNION ALL
    
      SELECT 'Actual' AS data_type,
        'eCommerce_NTS' AS kpi, -- Cluster , segment and year level
        NULL AS year_month,
        fisc_year,
        NULL AS qtr,
        NULL AS brand,
        coalesce(coalesce(map.gfo, prod_hier_l1), 'Others') AS franchise,
        cluster,
        NULL AS market,
        sum(value_usd) AS actual_value,
        NULL AS bp,
        NULL AS ju,
        NULL AS nu
      FROM (
        SELECT cluster,
          fisc_year,
          prod_hier_l1,
          sum(value_usd) value_usd
        FROM edw_rpt_ecomm_oneview
        WHERE kpi = 'NTS'
          AND upper(Cluster) NOT IN ('PACIFIC', 'INDIA')
          AND fisc_year > (date_part(year, current_timestamp()) - 3)
        GROUP BY cluster,
          fisc_year,
          prod_hier_l1
        ) fact
      --left join rg_edw.edw_gch_producthierarchy gph ON ltrim(fact.material_number,0) = ltrim(gph.materialnumber,0) and fact.material_number is not null
      LEFT JOIN (
        SELECT DISTINCT franchise,
          gfo
        FROM edw_okr_gfo_map
        WHERE measure = 'eCommerce_NTS'
        ) map ON map.franchise = CASE 
          WHEN fact.prod_hier_l1 IS NULL
            THEN 'Others'
          WHEN fact.prod_hier_l1 = ''
            THEN 'Others'
          ELSE prod_hier_l1
          END
      GROUP BY fisc_year,
        prod_hier_l1,
        map.gfo,
        cluster
    
      UNION ALL
    
      SELECT 'Actual' AS data_type,
        'eCommerce_NTS' AS kpi, -- Market , segment and year level
        NULL AS year_month,
        fisc_year,
        NULL AS qtr,
        NULL AS brand,
        coalesce(coalesce(map.gfo, prod_hier_l1), 'Others') AS franchise,
        cluster,
        fact.market,
        sum(value_usd) AS actual_value,
        NULL AS bp,
        NULL AS ju,
        NULL AS nu
      FROM edw_rpt_ecomm_oneview fact
      --left join rg_edw.edw_gch_producthierarchy gph ON ltrim(fact.material_number,0) = ltrim(gph.materialnumber,0) and fact.material_number is not null
      LEFT JOIN (
        SELECT DISTINCT franchise,
          gfo
        FROM edw_okr_gfo_map
        WHERE measure = 'eCommerce_NTS'
        ) map ON map.franchise = CASE 
          WHEN fact.prod_hier_l1 IS NULL
            THEN 'Others'
          WHEN fact.prod_hier_l1 = ''
            THEN 'Others'
          ELSE prod_hier_l1
          END
      WHERE kpi = 'NTS'
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year,
        prod_hier_l1,
        map.gfo,
        cluster,
        fact.market
    
      UNION ALL
    
      SELECT 'Actual' AS data_type, ---APAC , segment and month LEVEL
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
        coalesce(coalesce(map.gfo, prod_hier_l1), 'Others') AS franchise,
        'APAC' AS cluster,
        NULL AS market,
        sum(value_usd) AS actual_value,
        NULL AS bp,
        NULL AS ju,
        NULL AS nu
      FROM edw_rpt_ecomm_oneview fact
      --left join rg_edw.edw_gch_producthierarchy gph ON ltrim(fact.material_number,0) = ltrim(gph.materialnumber,0) and fact.material_number is not null
      LEFT JOIN (
        SELECT DISTINCT franchise,
          gfo
        FROM edw_okr_gfo_map
        WHERE measure = 'eCommerce_NTS'
        ) map ON map.franchise = CASE 
          WHEN fact.prod_hier_l1 IS NULL
            THEN 'Others'
          WHEN fact.prod_hier_l1 = ''
            THEN 'Others'
          ELSE prod_hier_l1
          END
      WHERE kpi = 'NTS'
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year,
        prod_hier_l1,
        map.gfo,
        fisc_month
    
      UNION ALL ---Cluster , segment and month LEVEL
    
      SELECT 'Actual' AS data_type,
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
        coalesce(coalesce(map.gfo, prod_hier_l1), 'Others') AS franchise,
        cluster,
        NULL AS market,
        sum(value_usd) AS actual_value,
        NULL AS bp,
        NULL AS ju,
        NULL AS nu
      FROM (
        SELECT cluster,
          prod_hier_l1,
          fisc_month,
          fisc_year,
          sum(value_usd) AS value_usd
        FROM edw_rpt_ecomm_oneview
        WHERE kpi = 'NTS'
          AND upper(Cluster) NOT IN ('PACIFIC', 'INDIA')
          AND fisc_year > (date_part(year, current_timestamp()) - 3)
        GROUP BY cluster,
          prod_hier_l1,
          fisc_month,
          fisc_year
        ) fact
      --left join rg_edw.edw_gch_producthierarchy gph ON ltrim(fact.material_number,0) = ltrim(gph.materialnumber,0) and fact.material_number is not null
      LEFT JOIN (
        SELECT DISTINCT franchise,
          gfo
        FROM edw_okr_gfo_map
        WHERE measure = 'eCommerce_NTS'
        ) map ON map.franchise = CASE 
          WHEN fact.prod_hier_l1 IS NULL
            THEN 'Others'
          WHEN fact.prod_hier_l1 = ''
            THEN 'Others'
          ELSE prod_hier_l1
          END
      GROUP BY fisc_year,
        prod_hier_l1,
        map.gfo,
        fisc_month,
        cluster
    
      UNION ALL
    
      SELECT 'Actual' AS data_type, --market , segment and month
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
        coalesce(coalesce(map.gfo, prod_hier_l1), 'Others') AS franchise,
        cluster,
        fact.market AS market,
        sum(value_usd) AS actual_value,
        NULL AS bp,
        NULL AS ju,
        NULL AS nu
      FROM edw_rpt_ecomm_oneview fact
      --left join rg_edw.edw_gch_producthierarchy gph ON ltrim(fact.material_number,0) = ltrim(gph.materialnumber,0) and fact.material_number is not null
      LEFT JOIN (
        SELECT DISTINCT franchise,
          gfo
        FROM edw_okr_gfo_map
        WHERE measure = 'eCommerce_NTS'
        ) map ON map.franchise = CASE 
          WHEN fact.prod_hier_l1 IS NULL
            THEN 'Others'
          WHEN fact.prod_hier_l1 = ''
            THEN 'Others'
          ELSE prod_hier_l1
          END
      WHERE kpi = 'NTS'
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year,
        prod_hier_l1,
        map.gfo,
        fisc_month,
        cluster,
        fact.market
    
      UNION ALL
    
      SELECT 'Actual' AS data_type, --market , brand and month
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
        COALESCE(matl.pka_brand_desc, 'NA') AS brand,
        coalesce(coalesce(map.gfo, prod_hier_l1), 'Others') AS franchise,
        cluster,
        fact.market AS market,
        sum(value_usd) AS actual_value,
        NULL AS bp,
        NULL AS ju,
        NULL AS nu
      FROM edw_rpt_ecomm_oneview fact
      --left join rg_edw.edw_gch_producthierarchy gph ON ltrim(fact.material_number,0) = ltrim(gph.materialnumber,0) and fact.material_number is not null
      LEFT JOIN (
        SELECT DISTINCT franchise,
          gfo
        FROM edw_okr_gfo_map
        WHERE measure = 'eCommerce_NTS'
        ) map ON map.franchise = CASE 
          WHEN fact.prod_hier_l1 IS NULL
            THEN 'Others'
          WHEN fact.prod_hier_l1 = ''
            THEN 'Others'
          ELSE prod_hier_l1
          END
      LEFT JOIN edw_material_dim matl ON ltrim(matl.matl_num, 0) = ltrim(fact.material_number, 0)
        AND fact.material_number IS NOT NULL
      WHERE kpi = 'NTS'
        AND fisc_year > (date_part(year, current_timestamp()) - 3)
      GROUP BY fisc_year,
        prod_hier_l1,
        map.gfo,
        fisc_month,
        cluster,
        fact.market,
        matl.pka_brand_desc
      )
    WHERE (
        year_month <= CASE 
          WHEN extract(day FROM current_timestamp()) < (
              SELECT cast(parameter_value AS INTEGER)
              FROM itg_query_parameters
              WHERE country_code = 'ALL'
                AND parameter_name = 'okr_data_update_date'
              )
            THEN to_char(add_months(current_timestamp(), - 2), 'YYYYMM')
          ELSE to_char(add_months(current_timestamp(), - 1), 'YYYYMM')
          END
        )
      OR year_month IS NULL
    GROUP BY data_type,
      kpi,
      year_month,
      fisc_year,
      qtr,
      CASE 
        WHEN brand = 'Johnson''s'
          AND franchise = 'Essential Health'
          THEN 'Johnson''s Baby'
        WHEN brand = 'Johnson''s'
          AND franchise = 'Skin Health'
          THEN 'Johnson''s Adult'
        WHEN brand = ''
          THEN 'NA'
        ELSE brand
        END,
      CASE 
        WHEN brand = 'Listerine'
          THEN 'Essential Health'
        WHEN brand IN ('OGX', 'Vogue', 'Aveeno')
          THEN 'Skin Health'
        WHEN brand = 'Aveeno Baby'
          THEN 'Essential Health'
        ELSE franchise
        END,
      cluster,
      CASE 
        WHEN market IN ('China OTC', 'China Selfcare')
          THEN 'China Self'
        WHEN market IN ('China Skincare', 'China Personal Care')
          THEN 'China PC'
        WHEN market = 'Japan'
          THEN 'JJKK'
        ELSE market
        END
),
final as
(
    select
    data_type::varchar(20) as data_type,
	kpi::varchar(100) as kpi,
	year_month::varchar(10) as year_month,
	fisc_year::varchar(10) as fisc_year,
	qtr::number(38,0) as quarter,
	brand::varchar(200) as brand,
	franchise::varchar(200) as franchise,
	cluster::varchar(100) as cluster,
	market::varchar(100) as market,
	nts_grwng_share_size::number(38,0) as nts_grwng_share_size,
	actual_value::number(38,4) as actual_value,
	bp::number(38,4) as bp_target,
	ju::number(38,4) as ju_target,
	nu::number(38,4) as nu_target,
	ytd_actual::number(38,4) as ytd_actual,	
	ytd_bp_target::number(38,4) as ytd_bp_target,
	ytd_ju_target::number(38,4) as ytd_ju_target,
	ytd_nu_target::number(38,4) as ytd_nu_target
    from trans
)
select * from final