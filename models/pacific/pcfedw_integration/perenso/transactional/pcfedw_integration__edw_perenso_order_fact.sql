with itg_perenso_order_header as (
     select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_ORDER_HEADER
),
itg_perenso_order_type as (
   select * from  DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_ORDER_TYPE
 ),
itg_perenso_order_detail as (
   select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_ORDER_DETAIL
),
itg_perenso_order_batch as (
   select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_ORDER_BATCH
),
itg_perenso_deal_discount as (
   select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_DEAL_DISCOUNT
),
itg_perenso_constants as (
   select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_CONSTANTS
),
itg_perenso_constants as (
   select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_CONSTANTS
), 
itg_perenso_account as (
select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_ACCOUNT
),
edw_time_dim as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_TIME_DIM
),
ipa as  (select distinct acct_key,

             acct_country

      from itg_perenso_account) ,
etd as (select jj_mnth_id,

        max(jj_wk) as jj_wk,

        max(cal_date) as cal_date

        from edw_time_dim

        group by jj_mnth_id),
final as 
(select ipoh.order_key,

       ipoh.order_type_key,

       ipot.order_type_desc,

       ipoh.acct_key,

       ipoh.order_date,

       ipoh.status as order_header_status_key,

       ipoh.charge,

       ipoh.confirmation,

       ipoh.diary_item_key,

       ipoh.work_item_key,

       ipoh.account_order_no,

       ipoh.delvry_instns,

       ipod.batch_key,

       ipod.line_key,

       ipod.prod_key,

       ipod.unit_qty,

       ipod.entered_qty,

       ipod.entered_unit_key,

       ipod.list_price,

       ipod.nis,

       ipod.rrp,

       ipod.credit_line_key,

       ipod.credited,

       ipod.disc_key,

       ipob.branch_key,

       ipob.dist_acct,

       case 

            when  upper(ipot.order_type_desc) = 'SHIPPED ORDERS'

            then  etd.cal_date::date 
            --dateadd(day,14,ipob.delvry_dt)

            else  ipob.delvry_dt::date

       end  as  delvry_dt,

       ipob.status as order_batch_status_key,

       ipob.suffix,

       ipob.sent_dt,

       ipdd.deal_key,

       ipdd.deal_desc,

       ipdd.start_date,

       ipdd.end_date,

       ipdd.short_desc,

       ipdd.discount_desc,

       ipch.const_desc as order_header_status,

       ipcb.const_desc as order_batch_status,

       case

         when upper(ipa.acct_country) = 'AUSTRALIA' then 'AUD'

         when upper(ipa.acct_country) = 'NEW ZEALAND' then 'NZD'

         else 'NOT ASSIGNED'

       end order_currency_cd

from itg_perenso_order_header ipoh,

     itg_perenso_order_type ipot,

     itg_perenso_order_detail ipod,

     itg_perenso_order_batch ipob,

     itg_perenso_deal_discount ipdd,

     itg_perenso_constants ipch,

     itg_perenso_constants ipcb,
     
     ipa,

     etd

where ipoh.order_type_key = ipot.order_type_key

and   ipoh.order_key = ipod.order_key

and   ipod.disc_key = ipdd.disc_key(+)

and   ipoh.order_key = ipob.ord_key

and   ipod.batch_key = ipob.batch_key

and   ipoh.status = ipch.const_key

and   ipob.status = ipcb.const_key

and   ipoh.acct_key = ipa.acct_key(+)

and   (extract(year from delvry_dt)||lpad(extract(month from delvry_dt),2,0))::number = etd.jj_mnth_id(+)

)

select * from final