with WKS_HK_base as (
  select 
    * 
  from 
    DEV_DNA_CORE.SNAPNTAWKS_INTEGRATION.WKS_HK_BASE
), 
edw_vw_os_time_dim as (
  select 
    * 
  from 
    DEV_DNA_CORE.SNENAV01_WORKSPACE.EDW_VW_OS_TIME_DIM
), 
WKS_HK_base as (
  select 
    * 
  from 
    DEV_DNA_CORE.SNAPNTAWKS_INTEGRATION.WKS_HK_BASE
), 
transformed as (
  select 
    all_months.dstr_cd as sap_parent_customer_key, 
    all_months.matl_num, 
    all_months.mnth_id as month, 
    sum(b.so_sls_qty) as so_qty, 
    sum(b.so_trd_sls) as so_value, 
    sum(b.inventory_quantity) as inv_qty, 
    sum(b.inventory_val) as inv_value, 
    sum(b.si_sls_qty) as sell_in_qty, 
    sum(b.si_gts_val) as sell_in_value 
  from 
    (
      select 
        distinct dstr_cd, 
        matl_num, 
        mnth_id 
      from 
        (
          select 
            distinct dstr_cd, 
            matl_num 
          from 
            WKS_HK_base
        ) a, 
        (
          select 
            distinct "year", 
            mnth_id 
          from 
            edw_vw_os_time_dim -- limit 100
          where 
            "year" >= (
              DATE_PART(YEAR, convert_timezone('UTC',current_timestamp())::timestamp_ntz(9))-6
            )
        ) b
    ) all_months, 
    WKS_HK_base b 
  where 
    all_months.dstr_cd = b.dstr_cd (+) 
    and all_months.matl_num = b.matl_num (+) 
    and mnth_id = month(+) 
  group by 
    all_months.dstr_cd, 
    all_months.matl_num, 
    all_months.mnth_id
),
final as (
select
sap_parent_customer_key::varchar(50) as sap_parent_customer_key,
matl_num::varchar(255) as matl_num,
month::varchar(23) as month,
so_qty::number(38,0) as so_qty,
so_value::number(38,4) as so_value,
inv_qty::number(38,5) as inv_qty,
inv_value::number(38,9) as inv_value,
sell_in_qty::number(38,4) as sell_in_qty,
sell_in_value::number(38,4) as sell_in_value
from transformed
)
select * from final 