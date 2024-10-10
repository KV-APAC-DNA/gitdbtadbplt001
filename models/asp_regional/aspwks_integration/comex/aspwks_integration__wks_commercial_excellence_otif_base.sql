WITH edw_otif_consumer_attr AS
(
  SELECT *
  FROM {{ ref ('aspedw_integration__edw_otif_consumer_attr') }}
),
edw_company_dim AS
(
  SELECT *
  FROM {{ ref ('aspedw_integration__edw_company_dim') }}
),
wks_commercial_excellence_otif_base AS
(
  SELECT fiscal_yr_mo,
         region,
         country_name,
         "cluster",
         segment_information,
         numerator,
         denominator,
         otif
  FROM (SELECT fiscal_yr_mo,
               region,
               CASE
                 WHEN market = 'CHINA' THEN 'CHINA SELFCARE'
                 WHEN market = 'SOUTH KOREA' THEN 'KOREA'
                 ELSE market
               END AS country_name,
               segment_information,
               SUM(numerator) AS numerator,
               SUM(denominator) AS denominator,
               nvl(SUM(numerator) / NULLIF(SUM(denominator),0),0) AS otif
        FROM edw_otif_consumer_attr 
        GROUP BY 1,
           2,
           3,
           4) oti
    LEFT JOIN (SELECT DISTINCT ctry_group, "cluster" FROM edw_company_dim) cd ON UPPER (oti.country_name) = UPPER (cd.ctry_group)
),
final AS
(
  SELECT fiscal_yr_mo::VARCHAR(23) AS month_id,
         country_name::VARCHAR(40) AS market,
         "cluster"::VARCHAR(100) AS "cluster",
         'OTIF'::VARCHAR(100) AS kpi,
         segment_information,
         numerator::NUMERIC(38,5) AS otif_numerator,
         denominator::NUMERIC(38,5) AS otif_denominator,
         otif::NUMERIC(38,5) AS otif,
         otif_ytd::NUMERIC(38,5) AS otif_ytd
  FROM (SELECT REPLACE(base.fiscal_yr_mo, '_', '') AS fiscal_yr_mo,
               INITCAP(base.country_name) AS country_name,
               base."cluster",
               base.segment_information,
               base.numerator,
               base.denominator,
               base.otif,
               nvl(SUM(ytd.numerator) / NULLIF(SUM(ytd.denominator),0),0) AS otif_ytd
        FROM wks_commercial_excellence_otif_base base
          LEFT JOIN wks_commercial_excellence_otif_base ytd
                 ON ytd.country_name = base.country_name
                AND IFNULL (ytd."cluster",'NA') = IFNULL (base."cluster",'NA')
                AND IFNULL (ytd.segment_information,'NA') = IFNULL (base.segment_information, 'NA')
                AND ytd.region = base.region
                AND CAST (LEFT (base.fiscal_yr_mo,4) AS INT) = CAST (LEFT (ytd.fiscal_yr_mo,4) AS INT)
                AND CAST (RIGHT (ytd.fiscal_yr_mo,2) AS INT) <= CAST (RIGHT (base.fiscal_yr_mo,2) AS INT)
        GROUP BY 1,
                 2,
                 3,
                 4,
                 5,
                 6,
                 7)
)
SELECT *
FROM final
