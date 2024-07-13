with 
itg_udcdetails as 
(
    select * from {{ ref('inditg_integration__itg_udcdetails') }}
),
edw_retailer_calendar_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
final as 
(
    SELECT 
       derived_table1."year",
       derived_table1.quarter,
       derived_table1."month",
       derived_table1.distcode,
       derived_table1.retailer_code,
       MIN(derived_table1.columnname::TEXT) AS columnname,
       "max"(derived_table1.program_name::TEXT) AS program_name
FROM (SELECT t.cal_yr AS "year",
             t.qtr AS quarter,
             t.mth_yyyymm AS "month",
             u.columnname,
             u.columnvalue AS program_name,
             u.mastervaluecode AS retailer_code,
             u.distcode
      FROM itg_udcdetails u
        JOIN edw_retailer_calendar_dim t      
         ON "right" (u.columnname::TEXT,4) = t.cal_yr::CHARACTER VARYING::TEXT
         AND "left" (SPLIT_PART (u.columnname::TEXT,' Q'::CHARACTER VARYING::TEXT,2),1) = t.qtr::CHARACTER VARYING::TEXT
         AND (u.columnname::TEXT like '%SSS Program Q%'::CHARACTER VARYING::TEXT
          OR u.columnname::TEXT like '%Platinum Q%'::CHARACTER VARYING::TEXT)
      WHERE u.mastername::TEXT = 'Retailer Master'::CHARACTER VARYING::TEXT
      AND   u.columnvalue IS NOT NULL
      AND   u.columnvalue::TEXT <> 'No'::CHARACTER VARYING::TEXT
      GROUP BY t.cal_yr,
               t.qtr,
               t.mth_yyyymm,
               u.columnname,
               u.columnvalue,
               u.mastervaluecode,
               u.distcode) derived_table1
GROUP BY derived_table1."year",
         derived_table1.quarter,
         derived_table1."month",
         derived_table1.distcode,
         derived_table1.retailer_code
)
select * from final