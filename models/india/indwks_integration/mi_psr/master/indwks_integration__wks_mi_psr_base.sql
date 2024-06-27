with wks_mi_psr_ret_dim_lat_cust as
(
    select * from {{ ref('indwks_integration__wks_mi_psr_ret_dim_lat_cust') }}
),
wks_mi_psr_salesman_master as
(
    select * from {{ ref('indwks_integration__wks_mi_psr_salesman_master') }}
),
edw_retailer_calendar_dim as
(
    select * from snapindedw_integration.edw_retailer_calendar_dim
    --{{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
itg_query_parameters as
(
    select * from snapinditg_integration.itg_query_parameters
),
final as
(
    select ret.rtruniquecode,ret.customer_code,ret.retailer_code,ret.region_name,ret.zone_name,ret.territory_name, ret.Channel_Name, sm.smcode,sm.smname,sm.uniquesalescode, cal.mth_mm AS rt_dim_mth_mm, cal.qtr, cal.fisc_yr, cal.month_nm_shrt
        ,ret.latest_customer_code
    from wks_MI_PSR_ret_dim_lat_cust ret
    left join (select sm.*, 
                    row_number() OVER (PARTITION BY sm.distcode,sm.rtrcode ORDER BY sm.smcode DESC) AS rn
            from  wks_MI_PSR_salesman_master sm) sm
    on ret.latest_customer_code = sm.distcode
    and ret.retailer_code = sm.rtrcode
    AND sm.rn =1
    CROSS JOIN (SELECT mth_mm,
                    qtr,
                    fisc_yr,
                    month_nm_shrt
                FROM edw_retailer_calendar_dim
                WHERE fisc_yr > EXTRACT(YEAR FROM convert_timezone('UTC',current_timestamp())) -(SELECT CAST(PARAMETER_VALUE AS INTEGER) AS PARAMETER_VALUE
                                                            FROM itg_query_parameters
                                                            WHERE UPPER(COUNTRY_CODE) = 'IN'
                                                            AND   UPPER(PARAMETER_TYPE) = 'DATA_RETENTION_PERIOD'
                                                            AND   UPPER(PARAMETER_NAME) = 'IN_MI_PSR_DATA_RETENTION_PERIOD')
                AND   day<= TO_CHAR(convert_timezone('UTC',current_timestamp())::DATE,'YYYYMMDD')
                GROUP BY mth_mm,
                        qtr,
                        fisc_yr,
                        month_nm_shrt) cal
)
select * from final