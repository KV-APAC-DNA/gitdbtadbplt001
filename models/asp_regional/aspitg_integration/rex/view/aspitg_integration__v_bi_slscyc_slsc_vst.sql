with v_slscyc_salescycle as (
select * from DEV_DNA_LOAD.SNAPASPSDL_RAW.V_SLSCYC_SALESCYCLE
),
v_vst_visit as (
select * from DEV_DNA_LOAD.SNAPASPSDL_RAW.V_VST_VISIT
),
v_cust_customer as (
select * from DEV_DNA_LOAD.SNAPASPSDL_RAW.V_CUST_CUSTOMER
),
final as (
SELECT 
  v.customerid AS itemid, 
  c.customername, 
  c.remotekey, 
  c.country, 
  c.storetype, 
  c.channel, 
  c.salesgroup, 
  v.visitid, 
  v.scheduleddate, 
  v.scheduledtime, 
  v.status, 
  v.salespersonid 
FROM 
  (
    SELECT 
      DISTINCT v_slscyc_salescycle.salescycleid, 
      v_slscyc_salescycle.salescyclename, 
      v_slscyc_salescycle.startdate, 
      v_slscyc_salescycle.enddate 
    FROM 
      v_slscyc_salescycle 
    WHERE 
      v_slscyc_salescycle.rank = 1
  ) a 
  LEFT JOIN (
    SELECT 
      DISTINCT v_vst_visit.visitid, 
      v_vst_visit.customerid, 
      v_vst_visit.targetgroupid, 
      v_vst_visit.scheduleddate, 
      v_vst_visit.scheduledtime, 
      v_vst_visit.status, 
      v_vst_visit.salescycleid, 
      v_vst_visit.salespersonid 
    FROM 
      v_vst_visit 
    WHERE 
      v_vst_visit.rank = 1
  ) v ON a.salescycleid :: text = v.salescycleid :: text 
  LEFT JOIN (
    SELECT 
      DISTINCT v_cust_customer.customerid, 
      v_cust_customer.customername, 
      v_cust_customer.remotekey, 
      v_cust_customer.country, 
      v_cust_customer.storetype, 
      v_cust_customer.channel, 
      v_cust_customer.salesgroup 
    FROM 
      v_cust_customer 
    WHERE 
      v_cust_customer.rank = 1
  ) c ON v.customerid :: text = c.customerid :: text 
WHERE 
  c.country IS NOT NULL
  )
  select * from final 