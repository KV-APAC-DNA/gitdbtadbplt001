with wks_rx_to_cx_urc_vrtl_udc_mapp as 
(
    select * from 
),
final as 
(
    SELECT 
        urc,
        v_custid_rtl,
        cust_name,
        cust_endtered_date,
        period,
        flex1,
        flex2,
        udc_orslcac2021_flag,
        SYSDATE,
        SYSDATE
    FROM wks_rx_to_cx_urc_vrtl_udc_mapp
)
select * from final
