with edw_sku_recom_spike_msl as
(
    select * from indedw_integration.edw_sku_recom_spike_msl
),
itg_udcdetails as
(
    select * from inditg_integration.itg_udcdetails
),
edw_calendar_dim as
(
    select * from snenav01_workspace.aspedw_integration__edw_calendar_dim
    --{{ ref('aspedw_integration__edw_calendar_dim') }}
),
final as 
(
    SELECT Coalesce (tb1.mth_mm,tb2.mth_mm) AS mth_mm,
       tb2.count_of_retailers,
       tb2.recos,
       tb2.hits,
       tb1.no_of_orange_stores,
       (tb1.no_of_orange_stores*1.0 / tb2.count_of_retailers)*100 AS orange_store_perc,
       'Program Stores at 35 percent' AS STORE_TAG
    FROM (SELECT mth_mm,
                SUM(recos) AS recos,
                SUM(hits) AS hits,
                COUNT(DISTINCT retailer_cd) AS no_of_orange_stores
        FROM (SELECT mth_mm,
                    retailer_cd,
                    salesman_code,
                    SUM(COALESCE(A3,0) *1.0) /NULLIF(SUM(COALESCE(A1,0)),0) AS DIVI,
                    SUM(COALESCE(A1,0)) AS recos,
                    SUM(COALESCE(A3,0)) AS hits
                FROM (SELECT cust_cd,
                            salesman_code,
                            retailer_cd,
                            mother_sku_cd,
                            mth_mm,
                            MAX(ms_flag)::NUMERIC AS A1,
                            MAX(hit_ms_flag)::NUMERIC AS A3
                    FROM edw_sku_recom_spike_msl a
                    INNER JOIN ( SELECT derived_table1."year", derived_table1.quarter, derived_table1."month", derived_table1.distcode, derived_table1.retailer_code, min(derived_table1.columnname::text) AS columnname, "max"(derived_table1.program_name::text) AS program_name
                            FROM ( SELECT t.cal_yr AS "year", t.cal_qtr_1 AS quarter, t.cal_mo_2 AS "month", u.columnname, u.columnvalue AS program_name, u.mastervaluecode AS retailer_code, u.distcode
                                    FROM itg_udcdetails u
                                JOIN edw_calendar_dim t ON "right"(u.columnname::text, 4) = t.cal_yr::character varying::text AND "left"(split_part(u.columnname::text, ' Q'::character varying::text, 2), 1) = t.cal_qtr_1::character varying::text AND (u.columnname::text like '%SSS Program Q%'::character varying::text OR u.columnname::text like '%Platinum Q%'::character varying::text)
                                WHERE u.mastername::text = 'Retailer Master'::character varying::text AND u.columnvalue IS NOT NULL AND u.columnvalue::text <> 'No'::character varying::text
                                GROUP BY t.cal_yr, t.cal_qtr_1, t.cal_mo_2, u.columnname, u.columnvalue, u.mastervaluecode, u.distcode) derived_table1
                            GROUP BY derived_table1."year", derived_table1.quarter, derived_table1."month", derived_table1.distcode, derived_table1.retailer_code) c
                        ON a.retailer_cd::text = c.retailer_code::text AND a.qtr = c.quarter AND ltrim("left"(a.mth_mm::character varying::text, 4), '0'::character varying::text) = c."year"::character varying::text AND ltrim("right"(a.mth_mm::character varying::text, 2), '0'::character varying::text) = c."month"::character varying::text AND c.distcode::text = a.cust_cd::text
                    WHERE LEFT(mth_mm,4) >= 2022
                    GROUP BY 1,2,3,4,5) base_tbl
                GROUP BY 1,2,3) iNN
        WHERE DIVI >= 0.35
        GROUP BY 1) tb1
    RIGHT JOIN (SELECT mth_mm,
                        SUM(ms_flag) AS recos,
                        SUM(hit_ms_flag) AS hits,
                        COUNT(DISTINCT retailer_cd) AS count_of_retailers
                FROM edw_sku_recom_spike_msl a 
                INNER JOIN ( SELECT derived_table1."year", derived_table1.quarter, derived_table1."month", derived_table1.distcode, derived_table1.retailer_code, min(derived_table1.columnname::text) AS columnname, "max"(derived_table1.program_name::text) AS program_name
                            FROM ( SELECT t.cal_yr AS "year", t.cal_qtr_1 AS quarter, t.cal_mo_2 AS "month", u.columnname, u.columnvalue AS program_name, u.mastervaluecode AS retailer_code, u.distcode
                                    FROM itg_udcdetails u
                                JOIN edw_calendar_dim t ON "right"(u.columnname::text, 4) = t.cal_yr::character varying::text AND "left"(split_part(u.columnname::text, ' Q'::character varying::text, 2), 1) = t.cal_qtr_1::character varying::text AND (u.columnname::text like '%SSS Program Q%'::character varying::text OR u.columnname::text like '%Platinum Q%'::character varying::text)
                                WHERE u.mastername::text = 'Retailer Master'::character varying::text AND u.columnvalue IS NOT NULL AND u.columnvalue::text <> 'No'::character varying::text
                                GROUP BY t.cal_yr, t.cal_qtr_1, t.cal_mo_2, u.columnname, u.columnvalue, u.mastervaluecode, u.distcode) derived_table1
                            GROUP BY derived_table1."year", derived_table1.quarter, derived_table1."month", derived_table1.distcode, derived_table1.retailer_code) c
                ON a.retailer_cd::text = c.retailer_code::text AND a.qtr = c.quarter AND ltrim("left"(a.mth_mm::character varying::text, 4), '0'::character varying::text) = c."year"::character varying::text AND ltrim("right"(a.mth_mm::character varying::text, 2), '0'::character varying::text) = c."month"::character varying::text AND c.distcode::text = a.cust_cd::text
                WHERE LEFT(mth_mm,4) >= 2022
                GROUP BY 1) tb2
            ON tb1.mth_mm = tb2.mth_mm

    UNION ALL

    SELECT Coalesce (tb1.mth_mm,tb2.mth_mm) AS mth_mm,
        tb2.count_of_retailers,
        tb2.recos,
        tb2.hits,
        tb1.no_of_orange_stores,
        (tb1.no_of_orange_stores*1.0 / tb2.count_of_retailers)*100 AS orange_store_perc,
        'All Stores at 50 percent' AS STORE_TAG
    FROM (SELECT mth_mm,
                SUM(recos) AS recos,
                SUM(hits) AS hits,
                COUNT(DISTINCT retailer_cd) AS no_of_orange_stores
        FROM (SELECT mth_mm,
                    retailer_cd,
                    salesman_code,
                    SUM(COALESCE(A3,0) *1.0) /NULLIF(SUM(COALESCE(A1,0)),0) AS DIVI,
                    SUM(COALESCE(A1,0)) AS recos,
                    SUM(COALESCE(A3,0)) AS hits
                FROM (SELECT cust_cd,
                            salesman_code,
                            retailer_cd,
                            mother_sku_cd,
                            mth_mm,
                            MAX(ms_flag)::NUMERIC AS A1,
                            MAX(hit_ms_flag)::NUMERIC AS A3
                    FROM edw_sku_recom_spike_msl a
                    WHERE LEFT(mth_mm,4) >= 2022
                    GROUP BY 1,2,3,4,5) base_tbl
                GROUP BY 1,2,3) iNN
        WHERE DIVI >= 0.50
        GROUP BY 1) tb1
    RIGHT JOIN (SELECT mth_mm,
                        COUNT(DISTINCT retailer_cd) AS count_of_retailers,
                        SUM(ms_flag) AS recos,
                        SUM(hit_ms_flag) AS hits
                FROM edw_sku_recom_spike_msl a 
                WHERE LEFT(mth_mm,4) >= 2022
                GROUP BY 1) tb2
            ON tb1.mth_mm = tb2.mth_mm
)
select * from final