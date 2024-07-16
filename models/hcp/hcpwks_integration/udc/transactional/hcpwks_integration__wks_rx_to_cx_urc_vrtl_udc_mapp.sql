with wks_rx_to_cx_urc_vrtl_mapp as
(
    select * from 
),
wks_rx_to_cx_urc_udc_macpp
(
    select * from
),
transformed as
(
SELECT mapp.urc,
       mapp.v_custid_rtl,
       mapp.cust_name,
       mapp.cust_endtered_date,
       mapp.period,
       mapp.flex1,
       mapp.flex2,
       NVL(udcf.udc_orslcac2021_flag,'NO') AS udc_orslcac2021_flag
FROM wks_rx_to_cx_urc_vrtl_mapp mapp
LEFT JOIN wks_rx_to_cx_urc_udc_macpp udcf
       ON  mapp.urc = udcf.urc
),
final as 
(   
    select * from transformed 
)
select * from final
