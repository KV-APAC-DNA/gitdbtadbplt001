with sdl_my_afgr as (
    select * from {{ source('myssdl_raw','sdl_my_afgr') }} ),
edw_vw_my_orders_fact as (
    select * from {{ ref('mysedw_integration__edw_vw_my_orders_fact') }}
),
transformed as 
(
select
source.cust_id, 
source.cust_nm, 
source.afgr_num, 
source.cust_dn_num, 
source.dn_amt_exc_gst_val, 
source.afgr_amt, 
source.dt_to_sc,
source.sc_validation, 
evmof.sls_doc_num as rtn_ord_num, 
evmof.doc_dt as rtn_ord_dt, 
evmof.ord_grs_trd_sls as rtn_ord_amt, 
source.cn_exp_issue_dt, 
evmof.bill_num as bill_num, 
evmof.bill_dt as bill_dt, 
evmof.bill_net_val as cn_amt, 
cdl_dttm
from sdl_my_afgr as source,edw_vw_my_orders_fact as evmof
where afgr_num = evmof.po_num
),
final as 
(
    select * from transformed

)  
select * from final