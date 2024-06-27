with itg_rx_cx_target_data as
(
    select * from inditg_integration.itg_rx_cx_target_data
),
itg_rx_cx_pre_target_data as
(
    select * from inditg_integration.itg_rx_cx_pre_target_data
),
sales_actual_achnr_qty as
(
    select * from {{ ref('hcpwks_integration__sales_actual_achnr_qty') }}
),
