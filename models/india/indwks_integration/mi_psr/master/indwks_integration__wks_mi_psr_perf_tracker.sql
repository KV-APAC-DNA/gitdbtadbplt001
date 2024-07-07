with wks_mi_psr_base as
(
    select * from {{ ref('indwks_integration__wks_mi_psr_base') }}
),
v_rpt_sales_details as
(
    select * from {{ ref('indedw_integration__v_rpt_sales_details') }}
),
itg_query_parameters as
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
v_rpt_rt_sales as
(
    select * from {{ ref('indedw_integration__v_rpt_rt_sales') }}
),
edw_rpt_mi_msl_dashboard as
(
    select * from {{ ref('indedw_integration__edw_rpt_mi_msl_dashboard') }}
),
wks_mi_psr_perf_tracker_l2y_trans_recs as
(
    select * from {{ ref('indwks_integration__wks_mi_psr_perf_tracker_l2y_trans_recs') }}
),
edw_retailer_calendar_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
final as
(
    SELECT base1.fisc_yr AS year,
       base1.month_nm_shrt AS month,
       base1.qtr AS qtr,
       base1.rtruniquecode AS rtruniquecode_rt_dim,
       base1.customer_code AS customer_code_rt_dim,
       base1.retailer_code AS retailer_code_rt_dim,
       base1.smcode AS smcode_rt_dim,
       base1.smname AS smname_rt_dim,
       base1.uniquesalescode AS uniquesalescode_rt_dim,
       base1.region_name AS region_name_rt_dim,
       base1.zone_name AS zone_name_rt_dim,
       base1.territory_Name AS territory_name_rt_dim,
       base1.Channel_Name,
       SD.achievement_amt_sales_cube,
       SD.rtruniquecode_sales_cube,
       SD.retailer_code_sales_cube,
       SD.latest_customer_code,
       SD.latest_customer_name,
       SD.latest_salesman_code,
       SD.latest_salesman_name,
       SD.latest_uniquesalescode,
       RT.rtruniquecode_RT_cube,
       RT.subd_retailer_code,
       RT.billed_subd_rt_cube,
       RT.Total_Number_of_bills,
       RT.No_of_Packs,
       RT.achievement_amt_RT_CUBE,
       RS.total_outlets AS total_outlets_subd_dim,
       RC.total_outlets_cust AS total_outlets_cust_subd_dim,
       COALESCE(sku."os_y/n_flag",'NO') AS os_flag
    FROM (SELECT rtruniquecode,
                customer_code,
                retailer_code,
                region_name,
                zone_name,
                territory_name,
                channel_name,
                smcode,
                smname,
                uniquesalescode,
                latest_customer_code,
                rt_dim_mth_mm,
                fisc_yr,
                qtr,
                month_nm_shrt
                FROM wks_MI_PSR_base) base1
    LEFT JOIN (SELECT fisc_yr AS YEAR,
                        MONTH,
                        qtr,
                        SUM(achievement_nr) AS achievement_amt_sales_cube,
                        retailer_code AS retailer_code_sales_cube,
                        rtruniquecode AS rtruniquecode_sales_cube,
                        latest_customer_code,
                        latest_customer_name,
                        latest_salesman_code,
                        latest_salesman_name,
                        latest_uniquesalescode,
                        mth_mm
                FROM v_rpt_sales_details
                WHERE fisc_yr > EXTRACT(YEAR FROM convert_timezone('UTC',current_timestamp())) -(SELECT CAST(PARAMETER_VALUE AS INTEGER) AS PARAMETER_VALUE
                                                            FROM itg_query_parameters
                                                            WHERE UPPER(COUNTRY_CODE) = 'IN'
                                                            AND   UPPER(PARAMETER_TYPE) = 'DATA_RETENTION_PERIOD'
                                                            AND   UPPER(PARAMETER_NAME) = 'IN_MI_PSR_DATA_RETENTION_PERIOD')
                AND   UPPER(Channel_Name) IN ('SUB-STOCKIEST','URBAN SUBD','RURAL WHOLESALE','PHARMA RESELLER SUBD')
                AND   status_desc = 'Active'
                GROUP BY fisc_yr,
                        MONTH,
                        qtr,
                        retailer_code,
                        rtruniquecode,
                        latest_customer_code,
                        latest_customer_name,
                        latest_salesman_code,
                        latest_salesman_name,
                        latest_uniquesalescode,
                        mth_mm) SD
            ON base1.rtruniquecode = SD.rtruniquecode_sales_cube
            AND base1.rt_dim_mth_mm = SD.mth_mm
    LEFT JOIN (SELECT S.year,
                        S.month,
                        cal.month_cd,
                        S.qtr,
                        S.region_name,
                        S.Zone_name,
                        S.Territory_Name,
                        S.SuperStockiest_Code,
                        S.SuperStockiest_Name,
                        S.PSR_code,
                        S.PSR_name,
                        S.rtruniquecode AS rtruniquecode_RT_cube,
                        S.rtr_code AS billed_subd_rt_cube,
                        S.retailer_code AS subd_retailer_code,
                        COUNT(DISTINCT (order_id)) AS Total_Number_of_bills,
                        (SUM(no_of_packs)) AS No_of_Packs,
                        SUM(S.achievement_amt) AS achievement_amt_RT_CUBE
                FROM v_rpt_rt_sales S
                LEFT JOIN (SELECT month_nm_shrt,
                                    RIGHT(mth_mm,2) AS month_cd
                            FROM edw_retailer_calendar_dim
                            GROUP BY month_nm_shrt,
                                    RIGHT(mth_mm,2)) cal 
                        ON S.month = cal.month_nm_shrt
                WHERE retailer_code IS NOT NULL
                AND   retailer_code <> ''
                AND   YEAR > EXTRACT(YEAR FROM convert_timezone('UTC',current_timestamp())) -(SELECT CAST(PARAMETER_VALUE AS INTEGER) AS PARAMETER_VALUE
                                                            FROM itg_query_parameters
                                                            WHERE UPPER(COUNTRY_CODE) = 'IN'
                                                            AND   UPPER(PARAMETER_TYPE) = 'DATA_RETENTION_PERIOD'
                                                            AND   UPPER(PARAMETER_NAME) = 'IN_MI_PSR_DATA_RETENTION_PERIOD')
                GROUP BY S.year,
                        S.month,
                        cal.month_cd,
                        S.qtr,
                        S.region_name,
                        S.Zone_name,
                        S.Territory_Name,
                        S.SuperStockiest_Code,
                        S.SuperStockiest_Name,
                        S.PSR_code,
                        S.PSR_name,
                        S.rtruniquecode,
                        S.rtr_code,
                        S.retailer_code) RT
            ON base1.customer_code = RT.SuperStockiest_Code
            AND base1.retailer_code = RT.billed_subd_rt_cube
            AND RIGHT (base1.rt_dim_mth_mm,2) = RT.month_cd
            AND LEFT (base1.rt_dim_mth_mm,4) = RT.year

    LEFT JOIN (SELECT rtruniquecode,
                        mth_mm,
                        'YES' AS "os_y/n_flag"
                        FROM (SELECT rtruniquecode,
                                    unique_sales_code,
                                    mth_mm,
                                    SUM(msl_sold_count)*1.0 /SUM(msl_count) AS DIVI,
                                    SUM(msl_count),
                                    SUM(msl_sold_count)
                                    FROM edw_rpt_mi_msl_dashboard
                            WHERE dataset = 'MI_MSL_AGG'
                            GROUP BY rtruniquecode,
                                    unique_sales_code,
                                    mth_mm) INN
                WHERE DIVI >= 0.50
                GROUP BY rtruniquecode,
                        mth_mm) sku
            ON SD.rtruniquecode_sales_cube = sku.rtruniquecode
            AND SD.mth_mm = sku.mth_mm

    LEFT JOIN (SELECT superstockiest_code, rtr_code, psr_code, COUNT(DISTINCT retailer_code) AS total_outlets
                FROM wks_mi_psr_perf_tracker_l2y_trans_recs
                GROUP BY 1,2,3) RS
            ON base1.latest_customer_code = RS.superstockiest_code
            AND base1.retailer_code = RS.rtr_code
            AND base1.smcode = SPLIT_PART(RS.psr_code,'-',1)

    LEFT JOIN (SELECT superstockiest_code, rtr_code, psr_code, COUNT(DISTINCT retailer_code) AS total_outlets_cust
                FROM wks_mi_psr_perf_tracker_l2y_trans_recs
                GROUP BY 1,2,3) RC
            ON base1.latest_customer_code = RC.superstockiest_code
            AND base1.retailer_code = RC.rtr_code
            AND base1.smcode = SPLIT_PART (RC.psr_code,'-',1)
)
select * from final