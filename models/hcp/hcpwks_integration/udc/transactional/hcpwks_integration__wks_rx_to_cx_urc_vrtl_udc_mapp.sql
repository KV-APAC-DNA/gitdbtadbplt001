with wks_rx_to_cx_urc_vrtl_mapp as
(
    select * from DEV_DNA_CORE.VSHRIR01_WORKSPACE.HCPWKS_INTEGRATION__WKS_RX_TO_CX_URC_VRTL_MAPP
),
wks_rx_to_cx_urc_udc_macpp as
(
    select * from DEV_DNA_CORE.VSHRIR01_WORKSPACE.HCPWKS_INTEGRATION__WKS_RX_TO_CX_URC_UDC_MACPP
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
ON mapp.urc::text = udcf.urc
),
final as 
(   
    select * from transformed
)
select * from final 
