{{   config
   (
       materialized="incremental",
       incremental_strategy="append",
       pre_hook="{% if is_incremental() %}
       delete from {{this}} where
                                   (upper(trim(jj_sap_dstrbtr_id)),trim(bill_doc),trim(jj_sap_prod_id),order_dt) in
                                           (
                                               select
                                                   distinct trim(sdio.distributor_code),
                                                   trim(sdio.order_id),
                                                   trim(sdio.product_code),
                                                   replace(sdio.order_date,'T',' ')::timestamp_ntz(9)
                                               from {{ source('idnsdl_raw', 'sdl_distributor_ivy_order') }} sdio
                                           )
                                   and
                                       (
                                           (nvl(upper(trim(dstrbtr_grp_cd)),'NA')) in
                                           ('DNR','CSA','PON','SAS','RFS','SNC','DSD','AWS','GMP','SPS','PMJ')
                                       );
{% endif %}"
   )
}}
with source as (
   select * from {{ source('idnsdl_raw', 'sdl_distributor_ivy_order') }}
),
edw_distributor_dim as (
   select * from {{ ref('idnedw_integration__edw_distributor_dim') }}
),
edw_product_dim as (
   select * from {{ ref('idnedw_integration__edw_product_dim') }}
),
edw_distributor_ivy_outlet_master as (
   select * from {{ ref('idnedw_integration__edw_distributor_ivy_outlet_master') }}
),
edw_time_dim as (
   select * from {{ source('idnedw_integration', 'edw_time_dim') }}
),
transformed as (
   select
       trim(sdio.distributor_code || sdio.retailer_code || sdio.product_code || sdio.order_id || (to_date(sdio.order_date))) as trans_key,
       trim(sdio.order_id) as bill_doc,
       replace(sdio.order_date,'T',' ')::timestamp_ntz(9) as order_dt,
       trim(sdio.jj_mnth_id) as jj_mnth_id,
       trim(sdio.jj_wk) as jj_wk,
       trim(edd.dstrbtr_grp_cd) as dstrbtr_grp_cd,
       trim(edd.dstrbtr_id) as dstrbtr_id,
       trim(sdio.distributor_code) as jj_sap_dstrbtr_id,
       trim(sdio.retailer_code) as dstrbtr_cust_id,
       '0' as dstrbtr_prod_id,
       trim(sdio.product_code) as jj_sap_prod_id,
       trim(ediom.outlet_type) as dstrbtn_chnl,
       null as grp_outlet,
       trim(sdio.user_code) as dstrbtr_slsmn_id,
case
           when upper(trim(sdio.uom))='CASE'
               then (sdio.qty*sdio.uom_count)::numeric(18,4)
 when trim(substring(sdio.product_code,9,10)) like '99%'
               then (sdio.qty / decode(epd.uom,null,1,0,1,epd.uom))::numeric(18,4)
 else (sdio.qty)::numeric(18,4)
 end as sls_qty,
      (sdio.line_value) as grs_val,
      (sdio.line_value*0.93*0.99) as jj_net_val,
      0 as trd_dscnt,
      0 as dstrbtr_net_val,
      case
        when sdio.qty < 0
           then sdio.qty*-1
        else 0
      end as rtrn_qty,
      case
        when sdio.qty < 0
           then sdio.line_value*-1
        else 0
      end as rtrn_val,
      current_timestamp()::timestamp_ntz(9) crtd_dttm,
      null updt_dttm,
 sdio.run_id
from (
   select
       t1.*,
       etd.jj_mnth_id,
       etd.jj_wk
   from
   source t1 ,
   edw_time_dim etd
   where date(etd.cal_date) = date(trim(t1.order_date)) ) sdio,
   edw_distributor_dim edd,
   edw_product_dim epd,
   edw_distributor_ivy_outlet_master ediom
where
  upper(trim(edd.jj_sap_dstrbtr_id)) = upper(trim(sdio.distributor_code))
and  sdio.jj_mnth_id between edd.effective_from(+) and edd.effective_to(+)
and  trim(ediom.jj_sap_dstrbtr_id(+)) = trim(sdio.distributor_code)
and sdio.product_code=epd.jj_sap_prod_id
and  sdio.jj_mnth_id between epd.effective_from(+) and epd.effective_to(+)
and  trim(ediom.cust_id(+)) = trim(sdio.retailer_code)
and
 (
 (nvl(upper(trim(edd.dstrbtr_grp_cd)),'na')) in
 ('DNR','CSA','PON','SAS','RFS','SNC','DSD','AWS','GMP','SPS','PMJ')
 )
),
final as (
   select
       trans_key::varchar(100) as trans_key,
       bill_doc::varchar(100) as bill_doc,
       order_dt::timestamp_ntz(9) as order_dt,
       jj_mnth_id::varchar(10) as jj_mnth_id,
       jj_wk::varchar(4) as jj_wk,
       dstrbtr_grp_cd::varchar(20) as dstrbtr_grp_cd,
       dstrbtr_id::varchar(50) as dstrbtr_id,
       jj_sap_dstrbtr_id::varchar(50) as jj_sap_dstrbtr_id,
       dstrbtr_cust_id::varchar(100) as dstrbtr_cust_id,
       dstrbtr_prod_id::varchar(100) as dstrbtr_prod_id,
       jj_sap_prod_id::varchar(50) as jj_sap_prod_id,
       dstrbtn_chnl::varchar(100) as dstrbtn_chnl,
       grp_outlet::varchar(5) as grp_outlet,
       dstrbtr_slsmn_id::varchar(100) as dstrbtr_slsmn_id,
       sls_qty::number(18,4) as sls_qty,
       grs_val::number(18,4) as grs_val,
       jj_net_val::number(18,4) as jj_net_val,
       trd_dscnt::number(18,4) as trd_dscnt,
       dstrbtr_net_val::number(18,4) as dstrbtr_net_val,
       rtrn_qty::number(18,4) as rtrn_qty,
       rtrn_val::number(18,4) as rtrn_val,
       crtd_dttm::timestamp_ntz(9) as crtd_dttm,
       updt_dttm::timestamp_ntz(9) as updt_dttm,
       run_id::number(14,0) as run_id
   from transformed
)
select * from transformed