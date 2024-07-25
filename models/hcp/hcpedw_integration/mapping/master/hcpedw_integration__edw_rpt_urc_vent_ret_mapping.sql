with wks_rx_to_cx_urc_vrtl_udc_mapp as 
(
    select * from {{ ref('hcpwks_integration__wks_rx_to_cx_urc_vrtl_udc_mapp') }}
),
final as 
(
    SELECT 
        urc::varchar(50) as urc,
        v_custid_rtl::varchar(50) as v_custid_rtl,
        cust_name::varchar(500) as cust_name,
        cust_endtered_date::date as cust_endtered_date,
        period::varchar(10) as period,
        flex1::varchar(200) as flex1,
        flex2::varchar(200) as flex2,
        udc_orslcac2021_flag::varchar(200) as udc_orslcac2021_flag,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz AS CRT_DTTM,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM
    FROM wks_rx_to_cx_urc_vrtl_udc_mapp
)
select * from final 
