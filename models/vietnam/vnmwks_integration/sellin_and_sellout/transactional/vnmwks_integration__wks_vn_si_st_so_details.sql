with edw_vw_os_time_dim as
(select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}),
itg_vn_dms_kpi as (
select * from {{ ref('vnmitg_integration__itg_vn_dms_kpi') }}
),
itg_vn_weekly_sellout_target as (
select * from {{ ref('vnmitg_integration__itg_vn_weekly_sellout_target') }}
),
edw_vw_vn_sellthrgh_sales_fact as (
select * from {{ ref('vnmedw_integration__edw_vw_vn_sellthrgh_sales_fact') }}
),
itg_vn_dms_distributor_dim_rnk as (
select *,Row_number() over (partition by dstrbtr_id order by crtd_dttm asc) as rnk 
 from {{ ref('vnmitg_integration__itg_vn_dms_distributor_dim') }}
),
itg_vn_dms_distributor_dim as (
select * from itg_vn_dms_distributor_dim_rnk
),
itg_vn_weekly_sellthrough_target as (
select * from {{ ref('vnmitg_integration__itg_vn_weekly_sellthrough_target') }}
),
edw_vw_vn_billing_fact as (
select * from {{ ref('vnmedw_integration__edw_vw_vn_billing_fact') }}
),
itg_vn_weekly_sellin_target as (
select * from {{ ref('vnmitg_integration__itg_vn_weekly_sellin_target') }}
),
itg_vn_dms_product_dim as (
select * from {{ ref('vnmitg_integration__itg_vn_dms_product_dim') }}
),
itg_vn_dms_call_details as (
select * from {{ ref('vnmitg_integration__itg_vn_dms_call_details') }}
),
itg_vn_dms_sales_org_dim as (
select * from {{ ref('vnmitg_integration__itg_vn_dms_sales_org_dim') }}
),
itg_vn_distributor_sap_sold_to_mapping as (
select * from {{ source('vnmitg_integration','itg_vn_distributor_sap_sold_to_mapping') }}
),
edw_vw_vn_sellout_sales_fact as (
select * from {{ ref('vnmedw_integration__edw_vw_vn_sellout_sales_fact') }}
),
itg_vn_productivity_call_target as (
select * from {{ ref('vnmitg_integration__itg_vn_productivity_call_target') }}
),
itg_vn_visit_call_target as (
select * from {{ ref('vnmitg_integration__itg_vn_visit_call_target') }}
),
wrk_vn_mnth_week as (
select * from {{ ref('vnmwks_integration__wrk_vn_mnth_week') }}
),
itg_vn_dms_customer_dim as (
select * from {{ ref('vnmitg_integration__itg_vn_dms_customer_dim') }}
),
itg_vn_gt_topdoor_storetype_hierarchy as (
select * from {{ ref('vnmitg_integration__itg_vn_gt_topdoor_storetype_hierarchy') }}
),
itg_vn_gt_topdoor_targets as (
select * from {{ ref('vnmitg_integration__itg_vn_gt_topdoor_targets') }}
),
itg_vn_dms_msl as (
select * from {{ ref('vnmitg_integration__itg_vn_dms_msl') }}
),
itg_mds_vn_gt_msl_shoptype_mapping as (
select * from {{ ref('vnmitg_integration__itg_mds_vn_gt_msl_shoptype_mapping') }}
),
 timedim as ((SELECT DISTINCT edw_vw_os_time_dim."year",
                    edw_vw_os_time_dim.qrtr,
                    edw_vw_os_time_dim.mnth_id,
                    edw_vw_os_time_dim.mnth_no,
                    edw_vw_os_time_dim.wk,
                    edw_vw_os_time_dim.mnth_wk_no,
					edw_vw_os_time_dim.cal_date,
                    edw_vw_os_time_dim.cal_date_id
             FROM edw_vw_os_time_dim )
),
veodcd as (
(SELECT distributor_id,
                    sap_sold_to_code,
                    mapp.region,
                    sap_ship_to_code,
                    territory_dist
             FROM itg_vn_distributor_sap_sold_to_mapping mapp,
                 itg_vn_dms_distributor_dim
             WHERE dstrbtr_id = distributor_id)
),
sls_fact as (
(select * from edw_vw_vn_sellout_sales_fact)
),
cstr as (
 (SELECT cst.outlet_id,
                          cst.outlet_name, cst.shop_type,cst.status,cstr_hier.Group_hierarchy, cstr_hier.Top_Door_Group
                     FROM itg_vn_dms_customer_dim cst
                     LEFT JOIN itg_vn_gt_topdoor_storetype_hierarchy cstr_hier
                       ON cst.shop_type = cstr_hier.Storetype )
),
dstrb as (
       (SELECT territory_dist,
                 mapped_spk as dstr_mapped_spk ,
                 dstrbtr_id as dstr_dstrbtr_id ,
                 dstrbtr_type,
                 dstrbtr_name,
                 asmid,
                 region,
                 sls.asm_name
    	  FROM 		 
	 (SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asmid,
             region
             --sls.asm_name
      FROM       
      (
           SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asm_id as asmid,
             region,
             Row_number() over (partition by dstrbtr_id order by crtd_dttm desc) as rn 
      FROM itg_vn_dms_distributor_dim) where rn = 1) dstr
        LEFT JOIN 
		(select asm_id, asm_name from
(select asm_id, asm_name,row_number() over (partition by asm_id order by crtd_dttm desc) as rnk from itg_vn_dms_sales_org_dim)
where rnk=1) sls ON UPPER (sls.asm_id) = UPPER (dstr.asmid))
),
so_fact as (
(  select * from sls_fact
			 left join 
           dstrb            
on lower(sls_fact.dstrbtr_grp_cd) = lower(dstrb.dstr_dstrbtr_id) --and lower(sls_fact.cust_cd) = lower(dstrb.outlet_id)
               LEFT JOIN 
             cstr
				on lower(sls_fact.cust_cd) = lower(cstr.outlet_id)	   
			)
        
),
so_det as (
(SELECT timedim."year" AS jj_year,
              timedim.qrtr AS jj_qrtr,
              timedim.mnth_id AS jj_mnth_id,
              timedim.mnth_no AS jj_mnth_no,
              timedim.mnth_wk_no AS jj_mnth_wk_no,
			  timedim.wk,
              veodcd.territory_dist,
              so_fact.dstrbtr_grp_cd,
              --dstrb.dstrbtr_type,
			        so_fact.dstrbtr_type,
              so_fact.sap_matl_num,
              so_fact.dstrbtr_matl_num,
              veodcd.sap_sold_to_code,
              /*nvl(dstrb.mapped_spk,dstrb.dstrbtr_id) distributor_id_report,*/
			        nvl(so_fact.dstr_mapped_spk,so_fact.dstr_dstrbtr_id) distributor_id_report,
              so_fact.bill_doc,
              so_fact.bill_date,
              so_fact.cust_cd,
			  so_fact.status,			 
              so_fact.slsmn_cd,
              so_fact.slsmn_nm,
              org.active AS slsmn_status,
              --dstrb.region dstrb_region,
			  so_fact.region dstrb_region,
              so_fact.sls_qty AS sls_qty,
              so_fact.ret_qty AS ret_qty,
              so_fact.grs_trd_sls AS grs_trd_sls,
              /*so_fact.jj_net_sls AS net_trd_sls,*/
			  /*Map jj_net_trd_sls i.e. total sellout afvat afdisc to so_net_trd_sls*/
			  so_fact.jj_net_trd_sls as net_trd_sls,
              NULL AS so_avg_wk_tgt,
              NULL AS so_mnth_tgt,
              supervisor_code,
              supervisor_name,
              /*dstrb.asm_id,
              dstrb.asm_name,*/
              so_fact.asmid,
              so_fact.asm_name,
              so_fact.outlet_name as outlet_name,
              so_fact.shop_type as shop_type,
              so_fact.Group_Hierarchy as Group_hierarchy, 
              so_fact.Top_Door_Group as Top_Door_Group			  
       FROM  timedim,
            /*edw_vw_vn_sellout_sales_fact so_fact,*/
             veodcd,
             so_fact,
           itg_vn_dms_sales_org_dim org
       WHERE (so_fact.sls_qty <> 0 OR so_fact.ret_qty <> 0 OR so_fact.grs_trd_sls <> 0 OR so_fact.ret_val <> 0 OR so_fact.trd_discnt <> 0)
       AND   so_fact.cntry_cd = 'VN'
       AND   timedim.cal_date::date = so_fact.bill_date::date
       --AND   dstrb.dstrbtr_id = so_fact.dstrbtr_grp_cd
       AND   veodcd.distributor_id = nvl(so_fact.dstr_mapped_spk,so_fact.dstr_dstrbtr_id)
       AND   dstrbtr_grp_cd = org.dstrbtr_id 
       AND   so_fact.slsmn_cd = org.salesrep_id
	   )
),
tgt as (
(select from_cycle,to_cycle, distributor_code,customer_code,target_value
               from itg_vn_gt_topdoor_targets)
),
union_1 as ( select so_det.jj_year, 
              so_det.jj_qrtr,
			  so_det.jj_mnth_id,
			  so_det.jj_mnth_no,
			  so_det.jj_mnth_wk_no,
			  so_det.wk,
			  so_det.territory_dist,
              so_det.dstrbtr_grp_cd,
              --dstrb.dstrbtr_type,
              so_det.dstrbtr_type,
              so_det.sap_matl_num,
              so_det.dstrbtr_matl_num,
              so_det.sap_sold_to_code,
              --nvl(dstrb.mapped_spk,dstrb.dstrbtr_id) distributor_id_report,
              so_det.distributor_id_report,
              so_det.bill_doc,
              so_det.bill_date,
              so_det.cust_cd,
			  so_det.status,				
              so_det.slsmn_cd,
              so_det.slsmn_nm,
              so_det.slsmn_status,
              --so_det.region dstrb_region,
              so_det.dstrb_region,
              so_det.sls_qty,
              so_det.ret_qty,
              so_det.grs_trd_sls,
              so_det.net_trd_sls,
              NULL AS so_avg_wk_tgt,
              NULL AS so_mnth_tgt,
              so_det.supervisor_code,
              so_det.supervisor_name,
              --dstrb.asm_id,
              --dstrb.asm_name,
              so_det.asmid as asm_id,
              so_det.asm_name,
			  so_det.outlet_name,
              so_det.shop_type,
              so_det.Group_hierarchy, 
              so_det.Top_Door_Group,
			  --tgt.target_value as top_door_target,
			  0 as top_door_target,
			  case when --tgt.distributor_code is not null and 
			  tgt.customer_code is not null then 'Y'
                         else 'N' end as top_door_flag
              from 
       so_det
	   left join 
	    tgt 
              on (cast(so_det.jj_mnth_id as numeric) >= cast(tgt.from_cycle as numeric) and 
                  cast(so_det.jj_mnth_id as numeric) <= cast(tgt.to_cycle as numeric)) and 
                  --lower(so_det.dstrbtr_grp_cd) = lower(tgt.distributor_code) and 
                  so_det.cust_cd = tgt.customer_code 	   
	   ),
union_2 as (
(SELECT timedim."year" AS jj_year,
              timedim.qrtr AS jj_qrtr,
              timedim.mnth_id AS jj_mnth_id,
              timedim.mnth_no AS jj_mnth_no,
              timedim.mnth_wk_no AS jj_mnth_wk_no,
			  timedim.wk,
              distsoldtomap.territory_dist,
              t.dstrbtr_id AS dstrbtr_grp_cd,
              dstrb.dstrbtr_type,
              NULL AS sap_matl_num,
              NULL AS dstrbtr_matl_num,
              distsoldtomap.sap_sold_to_code,
              --nvl(dstrb.dstr_mapped_spk,dstrb.dstr_dstrbtr_id) distributor_id_report,
              nvl(dstrb.mapped_spk,dstrb.dstrbtr_id) distributor_id_report,
              NULL AS bill_doc,
              NULL AS bill_date,
              NULL AS cust_cd,
			  NULL AS status,			 
              t.saleman_code AS slsmn_cd,
              org.salesrep_name AS slsmn_nm,
              org.active AS slsmn_status,
              dstrb.region dstrb_region,
              NULL AS sls_qty,
              NULL AS ret_qty,
              NULL AS grs_trd_sls,
              NULL AS net_trd_sls,
              (tgt_by_week*1000) / 1.1 so_avg_wk_tgt,
              (tgt_by_month*1000) / 1.1 so_mnth_tgt,
              supervisor_code,
              supervisor_name,
              --dstrb.asm_id,
              dstrb.asmid,              
              dstrb.asm_name,
	          NULL AS  outlet_name,
	          NULL AS  shop_type,
              NULL AS  Group_hierarchy, 
              NULL AS  Top_Door_Group,
              NULL AS  Top_Door_Target,  
	         'N' AS Top_Door_Flag	
			 FROM (SELECT edw_vw_os_time_dim."year",
                    edw_vw_os_time_dim.qrtr,
                    edw_vw_os_time_dim.mnth_id,
                    edw_vw_os_time_dim.mnth_no,
                    edw_vw_os_time_dim.wk,
                    edw_vw_os_time_dim.mnth_wk_no,
					MIN(edw_vw_os_time_dim.cal_date) AS cal_date
             FROM edw_vw_os_time_dim
             GROUP BY edw_vw_os_time_dim."year",
                      edw_vw_os_time_dim.qrtr,
                      edw_vw_os_time_dim.mnth_id,
                      edw_vw_os_time_dim.mnth_no,
                      edw_vw_os_time_dim.wk,
                      edw_vw_os_time_dim.mnth_wk_no) timedim,
            (SELECT distributor_id,
                    sap_sold_to_code,
                    mapp.region,
                    sap_ship_to_code,
                    territory_dist
             FROM itg_vn_distributor_sap_sold_to_mapping mapp,
                  itg_vn_dms_distributor_dim
             WHERE dstrbtr_id = distributor_id) distsoldtomap,
        (SELECT territory_dist,
               mapped_spk ,
               dstrbtr_id,
               dstrbtr_type,
               dstrbtr_name,
               asmid,
               region,
               sls.asm_name
  	     FROM 					 
			(SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asmid,
             region
             --sls.asm_name
			FROM       
			(
				 SELECT territory_dist,
						mapped_spk,
						dstrbtr_id,
						dstrbtr_type,
						dstrbtr_name,
						asm_id as asmid,
						region,
						Row_number() over (partition by dstrbtr_id order by crtd_dttm desc) as rn 
				FROM itg_vn_dms_distributor_dim) where rn = 1) dstr
               LEFT JOIN (select asm_id, asm_name from
(select asm_id, asm_name,row_number() over (partition by asm_id order by crtd_dttm desc) as rnk from itg_vn_dms_sales_org_dim)
where rnk=1) sls ON UPPER (sls.asm_id) = UPPER (dstr.asmid)) dstrb,
            itg_vn_weekly_sellout_target t,
            itg_vn_dms_sales_org_dim org
       WHERE t.dstrbtr_id = dstrb.dstrbtr_id
             --t.dstrbtr_id = dstrb.dstr_dstrbtr_id 
       AND   t.target_cyc = CAST(jj_mnth_id AS NUMERIC)
       AND   target_wk = jj_mnth_wk_no
       AND   t.saleman_code = org.salesrep_id (+)
       AND   t.dstrbtr_id = org.dstrbtr_id (+)
       AND   t.target_cyc > '201904'
       --AND   distsoldtomap.distributor_id = nvl(dstrb.mapped_spk,dstrb.dstrbtr_id))
       AND   distsoldtomap.distributor_id = nvl(dstrb.mapped_spk,dstrb.dstrbtr_id))
),
union_3 as (
  (SELECT timedim."year" AS jj_year,
              timedim.qrtr AS jj_qrtr,
              timedim.mnth_id AS jj_mnth_id,
              timedim.mnth_no AS jj_mnth_no,
			  NULL AS jj_mnth_wk_no,
			  timedim.wk,
              distsoldtomap.territory_dist,
              t.Distributor_code AS dstrbtr_grp_cd,
              dstrb.dstrbtr_type,
              NULL AS sap_matl_num,
              NULL AS dstrbtr_matl_num,
              distsoldtomap.sap_sold_to_code,
              --nvl(dstrb.dstr_mapped_spk,dstrb.dstr_dstrbtr_id) distributor_id_report,
              nvl(dstrb.mapped_spk,dstrb.dstrbtr_id) distributor_id_report,              
              NULL AS bill_doc,
              NULL AS bill_date,
              t.customer_code AS cust_cd,
				NULL as status,			 
              NULL AS slsmn_cd,
              NULL AS  slsmn_nm,
              NULL AS  slsmn_status,
              dstrb.region dstrb_region,
              NULL AS sls_qty,
              NULL AS ret_qty,
              NULL AS grs_trd_sls,
              NULL AS net_trd_sls,
              NULL AS so_avg_wk_tgt,
              NULL AS  so_mnth_tgt,
              NULL AS  supervisor_code,
              NULL AS  supervisor_name,
              --dstrb.asm_id,
              dstrb.asmid,
              dstrb.asm_name,
			  dstrb.outlet_name,
			  dstrb.shop_type,
			  dstrb.Group_hierarchy, 
			  dstrb.Top_Door_Group,
			  t.Target_value AS  Top_Door_Target,
			  'Y' AS Top_Door_Flag
       FROM (SELECT DISTINCT edw_vw_os_time_dim."year",
                    edw_vw_os_time_dim.qrtr,
                    edw_vw_os_time_dim.mnth_id,
                    edw_vw_os_time_dim.mnth_no,
					min(edw_vw_os_time_dim.wk) as wk
             FROM edw_vw_os_time_dim
			       GROUP BY edw_vw_os_time_dim."year",
                    edw_vw_os_time_dim.qrtr,
                    edw_vw_os_time_dim.mnth_id,
                    edw_vw_os_time_dim.mnth_no) timedim,
            (SELECT distributor_id,
                    sap_sold_to_code,
                    mapp.region,
                    sap_ship_to_code,
                    territory_dist
             FROM itg_vn_distributor_sap_sold_to_mapping mapp,
                  itg_vn_dms_distributor_dim
             WHERE dstrbtr_id = distributor_id) distsoldtomap,
            (SELECT dstr.territory_dist,
                    dstr.mapped_spk,
                    dstr.dstrbtr_id,
                    dstrbtr_type,
                    dstrbtr_name,
                    dstr.region,
                    dstr.asmid,
                    sls.asm_name,
                    cstr.outlet_id,
                    cstr.outlet_name,
                    cstr.shop_type,
                    cstr.Group_hierarchy, 
                    cstr.Top_Door_Group
             FROM (
			        SELECT territory_dist,
						   mapped_spk,
						   dstrbtr_id,
						   dstrbtr_type,
						   dstrbtr_name,
						   region,
						   asmid					       
					FROM 
					( 
					   SELECT territory_dist,
						   mapped_spk,
						   dstrbtr_id,
						   dstrbtr_type,
						   dstrbtr_name,
						   region,
						   asm_id as asmid,
					       row_number() over (partition by dstrbtr_id order by crtd_dttm desc) as rn
						FROM itg_vn_dms_distributor_dim) where rn = 1) dstr
               LEFT JOIN   
                 ( SELECT cst.dstrbtr_id, cst.outlet_id,
                          cst.outlet_name, cst.shop_type, cstr_hier.Group_hierarchy, cstr_hier.Top_Door_Group
                     FROM itg_vn_dms_customer_dim cst
                     LEFT JOIN itg_vn_gt_topdoor_storetype_hierarchy cstr_hier
                       ON cst.shop_type = cstr_hier.Storetype ) cstr
                  ON  lower(dstr.dstrbtr_id) = lower(cstr.dstrbtr_id) 
               LEFT JOIN (select asm_id, asm_name from
(select asm_id, asm_name,row_number() over (partition by asm_id order by crtd_dttm desc) as rnk from itg_vn_dms_sales_org_dim)
where rnk=1) sls ON UPPER (sls.asm_id) = UPPER (dstr.asmid)) dstrb,
            itg_vn_gt_topdoor_targets t 
       WHERE lower(t.Distributor_code) = lower(dstrb.dstrbtr_id)
       --lower(t.Distributor_code) = lower(dstrb.dstr_dstrbtr_id)
       AND  cast(t.from_cycle as numeric) >=  cast(timedim.mnth_id as numeric)
	   AND  cast(t.to_cycle as numeric)<= cast(timedim.mnth_id as numeric)
       AND   t.Customer_Code = dstrb.outlet_id 
       --AND   distsoldtomap.distributor_id = nvl(dstrb.mapped_spk,dstrb.dstrbtr_id))  
       AND   distsoldtomap.distributor_id = nvl(dstrb.mapped_spk,dstrb.dstrbtr_id))  
),
union_4 as (
(SELECT timedim."year" AS jj_year,
              timedim.qrtr AS jj_qrtr,
              timedim.mnth_id AS jj_mnth_id,
              timedim.mnth_no AS jj_mnth_no,
              timedim.mnth_wk_no AS jj_mnth_wk_no,
			  timedim.wk,
              distsoldtomap.territory_dist,
              t.dstrbtr_id AS dstrbtr_grp_cd,
              dstrb.dstrbtr_type,
              NULL AS sap_matl_num,
              NULL AS dstrbtr_matl_num,
              distsoldtomap.sap_sold_to_code,
              --nvl(dstrb.mapped_spk,dstrb.dstrbtr_id) distributor_id_report,
              nvl(dstrb.mapped_spk,dstrb.dstrbtr_id) distributor_id_report,
              NULL AS bill_doc,
              NULL AS bill_date,
              NULL AS cust_cd,
			  NULL AS status,			 
              t.saleman_code AS slsmn_cd,
              org.salesrep_name AS slsmn_nm,
              org.active AS slsmn_status,
              dstrb.region dstrb_region,
			  NULL AS sls_qty,
              NULL AS ret_qty,
              NULL AS grs_trd_sls,
              NULL AS net_trd_sls,
              CASE
                WHEN (jj_mnth_id = 201901 OR jj_mnth_id = 201902) THEN (tgt_by_month) / 1.1
                ELSE (tgt_by_month*1000) / 1.1
              END so_avg_wk_tgt,
              CASE
                WHEN (jj_mnth_id = 201901 OR jj_mnth_id = 201902) THEN (tgt_by_month) / 1.1
                ELSE (tgt_by_month*1000) / 1.1
              END so_mnth_tgt,
              supervisor_code,
              supervisor_name,
              --dstrb.asm_id,
              dstrb.asmid,
              dstrb.asm_name,
			  NULL AS  outlet_name,
	          NULL AS  shop_type,
              NULL AS  Group_hierarchy, 
              NULL AS  Top_Door_Group,
              NULL AS  Top_Door_Target,  
	          'N' AS Top_Door_Flag
       FROM (SELECT edw_vw_os_time_dim."year",
                    edw_vw_os_time_dim.qrtr,
                    edw_vw_os_time_dim.mnth_id,
                    edw_vw_os_time_dim.mnth_no,
                    edw_vw_os_time_dim.wk,
                    edw_vw_os_time_dim.mnth_wk_no,
					MIN(edw_vw_os_time_dim.cal_date) AS cal_date
             FROM edw_vw_os_time_dim
             GROUP BY edw_vw_os_time_dim."year",
                      edw_vw_os_time_dim.qrtr,
                      edw_vw_os_time_dim.mnth_id,
                      edw_vw_os_time_dim.mnth_no,
                      edw_vw_os_time_dim.wk,
                      edw_vw_os_time_dim.mnth_wk_no) timedim,
            (SELECT distributor_id,
                    sap_sold_to_code,
                    mapp.region,
                    sap_ship_to_code,
                    territory_dist
             FROM itg_vn_distributor_sap_sold_to_mapping mapp,
                  itg_vn_dms_distributor_dim
             WHERE dstrbtr_id = distributor_id) distsoldtomap,
          (SELECT territory_dist,
                 mapped_spk ,
                 dstrbtr_id ,
                 dstrbtr_type,
                 dstrbtr_name,
                 asmid,
                 region,
                 sls.asm_name
    	  FROM 		 
	 (SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asmid,
             region
             --sls.asm_name
      FROM       
      (
           SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asm_id as asmid,
             region,
             Row_number() over (partition by dstrbtr_id order by crtd_dttm desc) as rn 
      FROM itg_vn_dms_distributor_dim) where rn = 1) dstr
        LEFT JOIN 
		(select asm_id, asm_name from
(select asm_id, asm_name,row_number() over (partition by asm_id order by crtd_dttm desc) as rnk from itg_vn_dms_sales_org_dim)
where rnk=1) sls ON UPPER (sls.asm_id) = UPPER (dstr.asmid)) dstrb,
            (SELECT dstrbtr_id,
                    saleman_code,
                    CYCLE,
                    wk,
                    mnth_wk_no target_wk,
                    CASE
                      WHEN mnth_wk_no = 1 THEN target_value
                      ELSE 0
                    END tgt_by_month,
                    CASE
                      WHEN mnth_wk_no = 1 THEN actual_value
                      ELSE 0
                    END actual_value
             FROM itg_vn_dms_kpi,
                  (SELECT edw_vw_os_time_dim."year",
                          edw_vw_os_time_dim.qrtr,
                          edw_vw_os_time_dim.mnth_id,
                          edw_vw_os_time_dim.mnth_no,
                          edw_vw_os_time_dim.wk,
                          edw_vw_os_time_dim.mnth_wk_no,
						  MIN(edw_vw_os_time_dim.cal_date) AS cal_date
                   FROM edw_vw_os_time_dim
                   GROUP BY edw_vw_os_time_dim."year",
                            edw_vw_os_time_dim.qrtr,
                            edw_vw_os_time_dim.mnth_id,
                            edw_vw_os_time_dim.mnth_no,
                            edw_vw_os_time_dim.wk,
                            edw_vw_os_time_dim.mnth_wk_no) timedim
             WHERE kpi_type = 'Sellout'
             AND   CYCLE = CAST(mnth_id AS NUMERIC)
             AND   CYCLE NOT IN (SELECT target_cyc
                                 FROM itg_vn_weekly_sellout_target
                                 WHERE target_cyc > 201904)) t,
            itg_vn_dms_sales_org_dim org
       WHERE t.dstrbtr_id = dstrb.dstrbtr_id
       --t.dstrbtr_id = dstrb.dstr_dstrbtr_id
       AND   t.cycle = CAST(jj_mnth_id AS NUMERIC)
       AND   target_wk = jj_mnth_wk_no
       AND   t.saleman_code = org.salesrep_id (+)
       AND   t.dstrbtr_id = org.dstrbtr_id (+)
       --AND   distsoldtomap.distributor_id = nvl(dstrb.mapped_spk,dstrb.dstrbtr_id)
       AND   distsoldtomap.distributor_id = nvl(dstrb.mapped_spk,dstrb.dstrbtr_id)
       ORDER BY jj_mnth_id,
                jj_mnth_no,
                jj_mnth_wk_no)
				
),
union_5 as (
SELECT sell_thrgh.jj_year,
       sell_thrgh.jj_qrtr AS jj_qrtr,
       sell_thrgh.jj_mnth_id AS jj_mnth_id,
       sell_thrgh.jj_mnth_no,
       sell_thrgh.jj_mnth_wk_no,
	   NULL AS wks_in_qrtr,
       territory_dist,
       distributor_id_report,
       sell_thrgh.dstrbtr_grp_cd,
       sell_thrgh.dstrbtr_type,
       sell_thrgh.sap_sold_to_code,
       sell_thrgh.sap_matl_num,
       prod_dim.productnamesap AS sap_matl_name,
       sell_thrgh.dstrbtr_matl_num,
       prod_dim.product_name AS dstrbtr_matl_name,
       'GENERAL TRADE' AS trade_type,
       sell_thrgh.bill_doc AS st_bill_doc,
       sell_thrgh.bill_date AS st_bill_date,
       NULL AS cust_cd,
       NULL AS slsmn_cd,
       NULL AS slsmn_nm,
       NULL AS slsmn_status,
       0 AS so_sls_qty,
       0 AS so_ret_qty,
       0 AS so_grs_trd_sls,
       0 AS so_net_trd_sls,
       0 AS so_avg_wk_tgt,
       0 AS so_mnth_tgt,
       supervisor_code,
       supervisor_name,
       --asm_id,
       asmid,
       asm_name,
       region,
       NULL AS  outlet_name,
	   NULL AS  shop_type,
       NULL AS  Group_hierarchy, 
       NULL AS  Top_Door_Group,
       NULL AS  Top_Door_Target,  
	   'N' AS Top_Door_Flag,   
       0 AS si_sls_qty,
       0 AS si_ret_qty,
       0 AS si_gts_val,
       0 AS si_nts_val,
       0 AS si_avg_wk_tgt,
       0 AS si_mnth_tgt,
       sell_thrgh.sls_qty AS st_sls_qty,
       sell_thrgh.ret_qty AS st_ret_qty,
       sell_thrgh.grs_trd_sls AS st_grs_trd_sls,
       sell_thrgh.net_trd_sls AS st_net_trd_sls,
       sell_thrgh.st_avg_wk_tgt AS st_avg_wk_tgt,
       sell_thrgh.st_mnth_tgt AS st_mnth_tgt,
       NULL AS visit_call_date,
       NULL AS product_visit_call_date,
       NULL AS PC_Target,
       NULL AS PC_Target_by_week,
       NULL AS CE_Target,
       NULL AS CE_Target_by_week,
       NULL AS ub_weekly_target,
       prod_dim.franchise,
       prod_dim.brand,
       prod_dim.variant,
       prod_dim.product_group,
       prod_dim.group_jb,
	   NULL AS groupmsl
FROM (SELECT timedim."year" AS jj_year,
             timedim.qrtr AS jj_qrtr,
             timedim.mnth_id AS jj_mnth_id,
             timedim.mnth_no AS jj_mnth_no,
             timedim.mnth_wk_no AS jj_mnth_wk_no,
             dist_dim.territory_dist,
             --nvl(dstrb.dstr_mapped_spk,dstrb.dstr_dstrbtr_id) distributor_id_report,
             nvl(dstrb.mapped_spk,dstrb.dstrbtr_id) distributor_id_report,
             a.dstrbtr_id AS dstrbtr_grp_cd,
             dstrb.dstrbtr_type,
             a.sap_matl_num AS sap_matl_num,
             a.matl_num AS dstrbtr_matl_num,
             dist_dim.sap_sold_to_code,
             a.bill_doc,
             a.bill_date,
             NULL AS supervisor_code,
             NULL AS supervisor_name,
             --dstrb.asm_id,
             dstrb.asmid,             
             dstrb.asm_name,
             dstrb.region,
             a.sls_qty AS sls_qty,
             a.ret_qty AS ret_qty,
             a.grs_trd_sls AS grs_trd_sls,
             a.ret_val AS ret_val,
             a.trd_discnt_item_lvl AS trd_discnt,
             a.jj_net_trd_sls AS trd_sls,
             a.jj_net_trd_sls AS net_trd_sls,
             NULL AS st_avg_wk_tgt,
             NULL AS st_mnth_tgt
      -- 		 SUM(a.sls_qty) AS sls_qty, SUM(a.ret_qty) AS ret_qty, 
             -- 		 SUM(a.grs_trd_sls) AS grs_trd_sls, SUM(a.ret_val) AS ret_val, SUM(a.trd_discnt_item_lvl) AS trd_discnt, SUM(a.net_trd_sls) AS trd_sls, 
             -- 		 SUM(a.net_trd_sls) AS net_trd_sls  
             FROM (SELECT DISTINCT edw_vw_os_time_dim."year",
                          edw_vw_os_time_dim.qrtr,
                          edw_vw_os_time_dim.mnth_id,
                          edw_vw_os_time_dim.mnth_no,
                          edw_vw_os_time_dim.wk,
                          edw_vw_os_time_dim.mnth_wk_no,
                          edw_vw_os_time_dim.cal_date,
                          edw_vw_os_time_dim.cal_date_id
                   FROM edw_vw_os_time_dim) timedim,
           (SELECT *
            FROM edw_vw_vn_sellthrgh_sales_fact
            WHERE dstrbtr_id NOT IN (SELECT DISTINCT dstrbtr_id
                                     FROM itg_vn_dms_distributor_dim
                                     WHERE dstrbtr_type = 'SPK')) a,
           (SELECT distributor_id,
                   sap_sold_to_code,
                   mapp.region,
                   sap_ship_to_code,
                   territory_dist
            FROM itg_vn_distributor_sap_sold_to_mapping mapp,
                 itg_vn_dms_distributor_dim
            WHERE dstrbtr_id = distributor_id) dist_dim,
          (SELECT territory_dist,
                 mapped_spk ,
                 dstrbtr_id ,
                 dstrbtr_type,
                 dstrbtr_name,
                 asmid,
                 region,
                 sls.asm_name
    	  FROM 		 
	 (SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asmid,
             region
             --sls.asm_name
      FROM       
      (
           SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asm_id as asmid,
             region,
             Row_number() over (partition by dstrbtr_id order by crtd_dttm desc) as rn 
      FROM itg_vn_dms_distributor_dim) where rn = 1) dstr
        LEFT JOIN 
		(select asm_id, asm_name from
(select asm_id, asm_name,row_number() over (partition by asm_id order by crtd_dttm desc) as rnk from itg_vn_dms_sales_org_dim)
where rnk=1) sls ON UPPER (sls.asm_id) = UPPER (dstr.asmid)) dstrb

      WHERE (a.sls_qty <> 0 OR a.ret_qty <> 0 OR a.grs_trd_sls <> 0 OR a.ret_val <> 0)
      AND   a.cntry_cd = 'VN'
      AND   timedim.cal_date::date = a.bill_date::date
      AND   dstrb.dstrbtr_id = a.dstrbtr_id
      --AND   dstrb.dstr_dstrbtr_id = a.dstrbtr_id
      AND   dist_dim.distributor_id = nvl(dstrb.mapped_spk,dstrb.dstrbtr_id)
      --AND   dist_dim.distributor_id = nvl(dstrb.mapped_spk,dstrb.dstr_dstrbtr_id)
      --AND   --dstrb.mapped_spk = a.mapped_spk
      AND   dstrb.mapped_spk = a.mapped_spk
      UNION ALL
      SELECT timedim."year" AS jj_year,
             timedim.qrtr AS jj_qrtr,
             timedim.mnth_id AS jj_mnth_id,
             timedim.mnth_no AS jj_mnth_no,
             timedim.mnth_wk_no AS jj_mnth_wk_no,
             distsoldtomap.territory_dist,
             --nvl(dstrb.mapped_spk,dstrb.dstrbtr_id) distributor_id_report,
             nvl(dstrb.mapped_spk,dstrb.dstrbtr_id) distributor_id_report,             
             sell_th_tgt.dstrbtr_id AS dstrbtr_grp_cd,
             dstrb.dstrbtr_type,
             NULL AS sap_matl_num,
             NULL AS dstrbtr_matl_num,
             distsoldtomap.sap_sold_to_code,
             NULL AS bill_doc,
             NULL AS bill_date,
             NULL AS supervisor_code,
             NULL AS supervisor_name,
            -- dstrb.asm_id,
             dstrb.asmid,            
             dstrb.asm_name,
             dstrb.region,
             NULL AS sls_qty,
             NULL AS ret_qty,
             NULL AS grs_trd_sls,
             NULL AS ret_val,
             NULL AS trd_discnt,
             NULL AS trd_sls,
             NULL AS net_trd_sls,
             (sell_th_tgt.tgt_by_week*1000) AS st_avg_wk_tgt,
             (sell_th_tgt.tgt_by_month*1000) AS st_mnth_tgt
      FROM (SELECT edw_vw_os_time_dim."year",
                   edw_vw_os_time_dim.qrtr,
                   edw_vw_os_time_dim.mnth_id,
                   edw_vw_os_time_dim.mnth_no,
                   edw_vw_os_time_dim.wk,
                   edw_vw_os_time_dim.mnth_wk_no,
                   MIN(edw_vw_os_time_dim.cal_date) AS cal_date
            FROM edw_vw_os_time_dim
            GROUP BY edw_vw_os_time_dim."year",
                     edw_vw_os_time_dim.qrtr,
                     edw_vw_os_time_dim.mnth_id,
                     edw_vw_os_time_dim.mnth_no,
                     edw_vw_os_time_dim.wk,
                     edw_vw_os_time_dim.mnth_wk_no) timedim,
           (SELECT distributor_id,
                   sap_sold_to_code,
                   mapp.region,
                   sap_ship_to_code,
                   territory_dist
            FROM itg_vn_distributor_sap_sold_to_mapping mapp,
                 itg_vn_dms_distributor_dim
            WHERE dstrbtr_id = distributor_id) distsoldtomap,
          (SELECT territory_dist,
                 mapped_spk,
                 dstrbtr_id,
                 dstrbtr_type,
                 dstrbtr_name,
                 asmid,
                 region,
                 sls.asm_name
    	  FROM 		 
	 (SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asmid,
             region
             --sls.asm_name
      FROM       
      (
           SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asm_id as asmid,
             region,
             Row_number() over (partition by dstrbtr_id order by crtd_dttm desc) as rn 
      FROM itg_vn_dms_distributor_dim) where rn = 1) dstr
        LEFT JOIN 
		(select asm_id, asm_name from
(select asm_id, asm_name,row_number() over (partition by asm_id order by crtd_dttm desc) as rnk from itg_vn_dms_sales_org_dim)
where rnk=1) sls ON UPPER (sls.asm_id) = UPPER (dstr.asmid)) dstrb,
           (SELECT dstrbtr_id,
                   mapped_spk,
                   target_cyc,
                   tgt_by_month,
                   target_wk,
                   tgt_by_week
            FROM itg_vn_weekly_sellthrough_target) sell_th_tgt
      WHERE sell_th_tgt.target_cyc = CAST(timedim.mnth_id AS NUMERIC)
      AND   sell_th_tgt.target_wk = timedim.mnth_wk_no
      AND   dstrb.dstrbtr_id = sell_th_tgt.dstrbtr_id
      --AND   dstrb.dstr_dstrbtr_id = sell_th_tgt.dstrbtr_id   
      --AND   distsoldtomap.distributor_id = nvl(dstrb.mapped_spk,dstrb.dstrbtr_id)) sell_thrgh         
      AND   distsoldtomap.distributor_id = nvl(dstrb.mapped_spk,dstrb.dstrbtr_id)) sell_thrgh
  LEFT JOIN itg_vn_dms_product_dim prod_dim ON prod_dim.product_code = sell_thrgh.dstrbtr_matl_num
),
union_6 as (
SELECT sell_in.jj_year,
       sell_in.jj_qtr AS jj_qrtr,
       sell_in.jj_mnth_id AS jj_mnth_id,
       sell_in.jj_mnth_no,
       sell_in.jj_mnth_wk_no,
	   NULL AS wks_in_qrtr,
       territory_dist,
       distributor_id_report,
       sell_in.distributor_id,
       NULL AS dstrbtr_type,
       sell_in.cust_id,
       sell_in.item_cd,
       prod_dim.productnamesap AS sap_matl_name,
       NULL AS dstrbtr_matl_num,
       NULL AS dstrbtr_matl_name,
       'GENERAL TRADE' AS trade_type,
       NULL AS so_bill_doc,
       bill_dt,
       NULL AS cust_cd,
       NULL AS slsmn_cd,
       NULL AS slsmn_nm,
       NULL AS slsmn_status,
       0 AS so_sls_qty,
       0 AS so_ret_qty,
       0 AS so_grs_trd_sls,
       0 AS so_net_trd_sls,
       0 AS so_avg_wk_tgt,
       0 AS so_mnth_tgt,
       NULL supervisor_code,
       NULL superisor_name,
       NULL asm_id,
       NULL asm_name,
       NULL dstrb_region,
	   NULL AS  outlet_name,
	   NULL AS  shop_type,
       NULL AS  Group_hierarchy, 
       NULL AS  Top_Door_Group,
       NULL AS  Top_Door_Target,  
	   'N' AS Top_Door_Flag,
       sell_in.sls_qty AS si_sls_qty,
       sell_in.ret_qty AS si_ret_qty,
       sell_in.gts_val/100 AS si_gts_val,
       sell_in.nts_val AS si_nts_val,
       sell_in.si_avg_wk_tgt AS si_avg_wk_tgt,
       sell_in.si_mnth_tgt AS si_mnth_tgt,
       0 AS st_sls_qty,
       0 AS st_ret_qty,
       0 AS st_grs_trd_sls,
       0 AS st_net_trd_sls,
       0 AS st_avg_wk_tgt,
       0 AS st_mnth_tgt,
       NULL AS visit_call_date,
       NULL AS product_visit_call_date,
       NULL AS PC_Target,
       NULL AS PC_Target_by_week,
       NULL AS CE_Target,
       NULL AS CE_Target_by_week,
       NULL AS ub_weekly_target,
       prod_dim.franchise,
       prod_dim.brand,
       prod_dim.variant,
       NULL AS product_group,
       prod_dim.group_jb,
	   NULL AS groupmsl
FROM (SELECT timedim.mnth_id AS jj_mnth_id,
             timedim.mnth_no AS jj_mnth_no,
             timedim."year" AS jj_year,
             timedim.qrtr AS jj_qtr,
             timedim.mnth_wk_no AS jj_mnth_wk_no,
             bill_ft.bill_dt,
             dist_dim.territory_dist,
             dist_dim.distributor_id distributor_id_report,
             dist_dim.distributor_id,
             bill_ft.sold_to AS cust_id,
             bill_ft.matl_num item_cd,
             SUM(CASE WHEN (bill_ft.bill_qty_pc > 0) THEN bill_qty_pc ELSE 0 END) AS sls_qty,
             SUM(CASE WHEN (bill_ft.bill_qty_pc > 0) THEN bill_qty_pc ELSE 0 END) AS ret_qty,
             SUM(bill_ft.grs_trd_sls) AS gts_val,
             SUM(bill_ft.net_amt) AS nts_val,
             NULL AS si_avg_wk_tgt,
             NULL AS si_mnth_tgt
      FROM edw_vw_vn_billing_fact bill_ft,
           edw_vw_os_time_dim timedim,
           (SELECT distributor_id,
                   sap_sold_to_code,
                   mapp.region,
                   sap_ship_to_code,
                   territory_dist
            FROM itg_vn_distributor_sap_sold_to_mapping mapp,
                 itg_vn_dms_distributor_dim
            WHERE dstrbtr_id = distributor_id) dist_dim
      WHERE (bill_ft.bill_qty_pc <> 0)
      AND   bill_ft.cntry_key = 'VN'
      AND   bill_ft.bill_dt = timedim.cal_date
      AND   bill_ft.ship_to = dist_dim.sap_ship_to_code
      AND   bill_ft.bill_type IN ('ZF2V','ZSML')
      GROUP BY timedim.mnth_id,
               timedim.mnth_no,
               timedim."year",
               timedim.qrtr,
               timedim.mnth_wk_no,
               bill_ft.bill_dt,
               dist_dim.territory_dist,
               dist_dim.distributor_id,
               bill_ft.sold_to,
               dist_dim.region,
               bill_ft.matl_num
      UNION ALL
      SELECT timedim.mnth_id AS jj_mnth_id,
             timedim.mnth_no AS jj_mnth_no,
             timedim."year" AS jj_year,
             timedim.qrtr AS jj_qtr,
             mnth_wk_no AS jj_mnth_wk_no,
             NULL AS bill_dt,
             distsoldtomap.territory_dist,
             NULL AS distributor_id_report,
             NULL AS distributor_id,
             NULL AS cust_id,
             NULL AS item_cd,
             NULL AS sls_qty,
             NULL AS ret_qty,
             NULL AS gts_val,
             NULL AS nts_val,
             (sell_in_tgt.tgt_by_week*1000) AS si_avg_wk_tgt,
             (sell_in_tgt.tgt_by_month*1000) AS si_mnth_tgt
      FROM (SELECT edw_vw_os_time_dim."year",
                   edw_vw_os_time_dim.qrtr,
                   edw_vw_os_time_dim.mnth_id,
                   edw_vw_os_time_dim.mnth_no,
                   edw_vw_os_time_dim.wk,
                   edw_vw_os_time_dim.mnth_wk_no,
                   MIN(edw_vw_os_time_dim.cal_date) AS cal_date
            FROM edw_vw_os_time_dim
            GROUP BY edw_vw_os_time_dim."year",
                     edw_vw_os_time_dim.qrtr,
                     edw_vw_os_time_dim.mnth_id,
                     edw_vw_os_time_dim.mnth_no,
                     edw_vw_os_time_dim.wk,
                     edw_vw_os_time_dim.mnth_wk_no) timedim,
           (SELECT DISTINCT territory_dist
            FROM itg_vn_distributor_sap_sold_to_mapping mapp,
                 itg_vn_dms_distributor_dim
            WHERE dstrbtr_id = distributor_id) distsoldtomap,
           (SELECT territory_dist,
                   target_cyc,
                   target_wk,
                   tgt_by_month,
                   tgt_by_week
            FROM itg_vn_weekly_sellin_target) sell_in_tgt
      WHERE sell_in_tgt.territory_dist = distsoldtomap.territory_dist (+)
      AND   sell_in_tgt.target_cyc = CAST(timedim.mnth_id AS NUMERIC)
      AND   sell_in_tgt.target_wk = timedim.mnth_wk_no) sell_in
  LEFT JOIN (SELECT * FROM (
			SELECT productcodesap,
                    productnamesap,
                    group_jb,
                    franchise,
                    brand,
                    variant, row_number() over (partition by productcodesap order by productnamesap) rn
             FROM itg_vn_dms_product_dim) where rn=1) prod_dim ON prod_dim.productcodesap = sell_in.item_cd
),
union_7 as (
SELECT "year" jj_year,
       qrtr AS jj_qrtr,
       mnth_id AS jj_mnth_id,
       mnth_no AS jj_mnth_no,
       mnth_wk_no AS jj_mnth_wk_no,
	   NULL AS wks_in_qrtr,
       distrb.territory_dist,
       nvl(distrb.mapped_spk,distrb.dstrbtr_id) distributor_id_report,
       org.dstrbtr_id AS dstrbtr_grp_cd,
       distrb.dstrbtr_type,
       sap_sold_to_map.sap_sold_to_code AS sap_sold_to_code,
       NULL AS sap_matl_num,
       NULL AS sap_matl_name,
       NULL AS dstrbtr_matl_num,
       NULL AS dstrbtr_matl_name,
       'GENERAL TRADE' AS trade_type,
       NULL AS bill_doc,
       call_details.visit_date AS bill_date,
       NULL AS cust_cd,
       call_details.salesrep_id AS slsmn_cd,
       org.salesrep_name AS slsmn_nm,
       org.active AS slsmn_status,
       0 AS so_sls_qty,
       0 AS so_ret_qty,
       0 AS so_grs_trd_sls,
       0 AS so_net_trd_sls,
       0 AS so_avg_wk_tgt,
       0 AS so_mnth_tgt,
       supervisor_code,
       supervisor_name,
       --distrb.asm_id,
       distrb.asmid,
       distrb.asm_name,
       distrb.region dstrb_region,
	   NULL AS  outlet_name,
	   NULL AS  shop_type,
       NULL AS  Group_hierarchy, 
       NULL AS  Top_Door_Group,
	   NULL AS  Top_Door_Target,	
       'N' AS  Top_Door_Flag,	   
       0 AS si_sls_qty,
       0 AS si_ret_qty,
       0 AS si_gts_val,
       0 AS si_nts_val,
       0 AS si_avg_wk_tgt,
       0 AS si_mnth_tgt,
       0 AS st_sls_qty,
       0 AS st_ret_qty,
       0 AS st_grs_trd_sls,
       0 AS st_net_trd_sls,
       0 AS st_avg_wk_tgt,
       0 AS st_mnth_tgt,
       call_details.visit_date visit_call_date,
       NULL AS product_visit_call_date,
       NULL AS PC_Target,
       NULL AS PC_Target_by_week,
       NULL AS CE_Target,
       NULL AS CE_Target_by_week,
       NULL AS ub_weekly_target,
       NULL AS franchise,
       NULL AS brand,
       NULL AS variant,
       NULL AS product_group,
       NULL AS group_jb,
	   NULL AS groupmsl
FROM (SELECT dstrbtr_id,
             salesrep_id,
             outlet_id,
             visit_date,
             distance,
             ROW_NUMBER() OVER (PARTITION BY dstrbtr_id,salesrep_id,outlet_id,visit_date ORDER BY checkin_time) rn
      FROM itg_vn_dms_call_details
      WHERE distance <= 300
      AND   visit_date < '2019-05-27'
      UNION ALL
      SELECT dstrbtr_id,
             salesrep_id,
             outlet_id,
             visit_date,
             distance,
             ROW_NUMBER() OVER (PARTITION BY dstrbtr_id,salesrep_id,outlet_id,visit_date ORDER BY checkin_time) rn
      FROM itg_vn_dms_call_details
      WHERE ((distance BETWEEN 1 AND 300) OR (distance = 0 AND reason IS NULL)) AND visit_date >= '2019-05-27') call_details,
     edw_vw_os_time_dim time_dim,
          (SELECT territory_dist,
                 mapped_spk ,
                 dstrbtr_id ,
                 dstrbtr_type,
                 dstrbtr_name,
                 asmid,
                 region,
                 sls.asm_name
    	  FROM 		 
	 (SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asmid,
             region
             --sls.asm_name
      FROM       
      (
           SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asm_id as asmid,
             region,
             Row_number() over (partition by dstrbtr_id order by crtd_dttm desc) as rn 
      FROM itg_vn_dms_distributor_dim) where rn = 1) dstr
        LEFT JOIN 
		(select asm_id, asm_name from
(select asm_id, asm_name,row_number() over (partition by asm_id order by crtd_dttm desc) as rnk from itg_vn_dms_sales_org_dim)
where rnk=1) sls ON UPPER (sls.asm_id) = UPPER (dstr.asmid)) distrb,
     itg_vn_dms_sales_org_dim org,
     (SELECT distributor_id,
             sap_sold_to_code,
             mapp.region,
             sap_ship_to_code,
             territory_dist
      FROM itg_vn_distributor_sap_sold_to_mapping mapp,
           itg_vn_dms_distributor_dim
      WHERE dstrbtr_id = distributor_id) sap_sold_to_map
WHERE call_details.visit_date = cal_date
AND   call_details.rn = 1
--AND   call_details.distance <= 300
AND   call_details.dstrbtr_id = distrb.dstrbtr_id
AND   sap_sold_to_map.distributor_id = nvl(distrb.mapped_spk,distrb.dstrbtr_id)
AND   call_details.salesrep_id = org.salesrep_id (+)
AND   call_details.dstrbtr_id = org.dstrbtr_id (+)
),
union_8 as (
SELECT "year" jj_year,
       qrtr AS jj_qrtr,
       mnth_id AS jj_mnth_id,
       mnth_no AS jj_mnth_no,
       mnth_wk_no AS jj_mnth_wk_no,
	   NULL AS wks_in_qrtr,
       distrb.territory_dist,
       nvl(distrb.mapped_spk,distrb.dstrbtr_id) distributor_id_report,
       org.dstrbtr_id AS dstrbtr_grp_cd,
       distrb.dstrbtr_type,
       sap_sold_to_map.sap_sold_to_code AS sap_sold_to_code,
       NULL AS sap_matl_num,
       NULL AS sap_matl_name,
       NULL AS dstrbtr_matl_num,
       NULL AS dstrbtr_matl_name,
       'GENERAL TRADE' AS trade_type,
       NULL AS bill_doc,
       so.bill_date AS bill_date,
       NULL AS cust_cd,
       so.slsmn_cd AS slsmn_cd,
       org.salesrep_name AS slsmn_nm,
       org.active AS slsmn_status,
       0 AS so_sls_qty,
       0 AS so_ret_qty,
       0 AS so_grs_trd_sls,
       0 AS so_net_trd_sls,
       0 AS so_avg_wk_tgt,
       0 AS so_mnth_tgt,
       supervisor_code,
       supervisor_name,
       --distrb.asm_id,
       distrb.asmid,
       distrb.asm_name,
       distrb.region dstrb_region,
	   NULL AS  outlet_name,
	   NULL AS  shop_type,
       NULL AS  Group_hierarchy, 
       NULL AS  Top_Door_Group,
	   NULL AS  Top_Door_Target,
	   'N' AS  Top_Door_Flag,
       0 AS si_sls_qty,
       0 AS si_ret_qty,
       0 AS si_gts_val,
       0 AS si_nts_val,
       0 AS si_avg_wk_tgt,
       0 AS si_mnth_tgt,
       0 AS st_sls_qty,
       0 AS st_ret_qty,
       0 AS st_grs_trd_sls,
       0 AS st_net_trd_sls,
       0 AS st_avg_wk_tgt,
       0 AS st_mnth_tgt,
       NULL AS visit_call_date,
       so.bill_date product_visit_call_date,
       NULL AS PC_Target,
       NULL AS PC_Target_by_week,
       NULL AS CE_Target,
       NULL AS CE_Target_by_week,
       NULL AS ub_weekly_target,
       NULL AS franchise,
       NULL AS brand,
       NULL AS variant,
       NULL AS product_group,
       NULL AS group_jb,
	   NULL AS groupmsl
FROM (SELECT *
      FROM (SELECT dstrbtr_grp_cd,
                   slsmn_cd,
                   cust_cd,
                   bill_date,
                   sls_qty,
                   ROW_NUMBER() OVER (PARTITION BY dstrbtr_grp_cd,slsmn_cd,cust_cd,bill_date ORDER BY bill_date) rn
            FROM edw_vw_vn_sellout_sales_fact
            WHERE sls_qty > 0)
      WHERE rn = 1) so,
     edw_vw_os_time_dim time_dim,
          (SELECT territory_dist,
                 mapped_spk  ,
                 dstrbtr_id ,
                 dstrbtr_type,
                 dstrbtr_name,
                 asmid,
                 region,
                 sls.asm_name
    	  FROM 		 
	 (SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asmid,
             region
             --sls.asm_name
      FROM       
      (
           SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asm_id as asmid,
             region,
             Row_number() over (partition by dstrbtr_id order by crtd_dttm desc) as rn 
      FROM itg_vn_dms_distributor_dim) where rn = 1) dstr
        LEFT JOIN 
		(select asm_id, asm_name from
(select asm_id, asm_name,row_number() over (partition by asm_id order by crtd_dttm desc) as rnk from itg_vn_dms_sales_org_dim)
where rnk=1) sls ON UPPER (sls.asm_id) = UPPER (dstr.asmid)) distrb,
     itg_vn_dms_sales_org_dim org,
     (SELECT distributor_id,
             sap_sold_to_code,
             mapp.region,
             sap_ship_to_code,
             territory_dist
      FROM itg_vn_distributor_sap_sold_to_mapping mapp,
           itg_vn_dms_distributor_dim
      WHERE dstrbtr_id = distributor_id) sap_sold_to_map
WHERE so.bill_date = cal_date
AND   so.dstrbtr_grp_cd = distrb.dstrbtr_id
AND   sap_sold_to_map.distributor_id = nvl(distrb.mapped_spk,distrb.dstrbtr_id)
AND   so.slsmn_cd = org.salesrep_id (+)
AND   so.dstrbtr_grp_cd = org.dstrbtr_id (+)
),
union_9 as (
SELECT jj_year,
       jj_qrtr,
       jj_mnth_id,
       jj_mnth_no,
       jj_mnth_wk_no,
	   NULL AS wks_in_qrtr,
       distrb.territory_dist,
       nvl(distrb.mapped_spk,distrb.dstrbtr_id) distributor_id_report,
       org.dstrbtr_id AS dstrbtr_grp_cd,
       distrb.dstrbtr_type,
       sap_sold_to_map.sap_sold_to_code AS sap_sold_to_code,
       NULL AS sap_matl_num,
       NULL AS sap_matl_name,
       NULL AS dstrbtr_matl_num,
       NULL AS dstrbtr_matl_name,
       'GENERAL TRADE' AS trade_type,
       NULL AS bill_doc,
       NULL AS bill_date,
       NULL AS cust_cd,
       PC.salesrep_id AS slsmn_cd,
       org.salesrep_name AS slsmn_nm,
       org.active AS slsmn_status,
       NULL AS so_sls_qty,
       NULL AS so_ret_qty,
       NULL AS so_grs_trd_sls,
       NULL AS so_net_trd_sls,
       NULL AS so_avg_wk_tgt,
       NULL AS so_mnth_tgt,
       supervisor_code,
       supervisor_name,
       --distrb.asm_id,
       distrb.asmid,
       distrb.asm_name,
       distrb.region AS dstrb_region,
	   NULL AS  outlet_name,
	   NULL AS  shop_type,
        NULL AS  Group_hierarchy, 
        NULL AS  Top_Door_Group,
	    NULL AS  Top_Door_Target,
		'N' AS  Top_Door_Flag,
       0 AS si_sls_qty,
       0 AS si_ret_qty,
       0 AS si_gts_val,
       0 AS si_nts_val,
       0 AS si_avg_wk_tgt,
       0 AS si_mnth_tgt,
       0 AS st_sls_qty,
       0 AS st_ret_qty,
       0 AS st_grs_trd_sls,
       0 AS st_net_trd_sls,
       0 AS st_avg_wk_tgt,
       0 AS st_mnth_tgt,
       NULL AS visit_call_date,
       NULL AS product_visit_call_date,
       PC.target_by_month AS PC_Target,
       PC.target_by_week AS PC_Target_by_week,
       NULL AS CE_Target,
       NULL AS CE_Target_by_week,
       NULL AS ub_weekly_target,
       NULL AS franchise,
       NULL AS brand,
       NULL AS variant,
       NULL AS product_group,
       NULL AS group_jb,
	   NULL AS groupmsl
FROM           (SELECT territory_dist,
                 mapped_spk ,
                 dstrbtr_id ,
                 dstrbtr_type,
                 dstrbtr_name,
                 asmid,
                 region,
                 sls.asm_name
    	  FROM 		 
	 (SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asmid,
             region
             --sls.asm_name
      FROM       
      (
           SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asm_id as asmid,
             region,
             Row_number() over (partition by dstrbtr_id order by crtd_dttm desc) as rn 
      FROM itg_vn_dms_distributor_dim) where rn = 1) dstr
        LEFT JOIN 
		(select asm_id, asm_name from
(select asm_id, asm_name,row_number() over (partition by asm_id order by crtd_dttm desc) as rnk from itg_vn_dms_sales_org_dim)
where rnk=1) sls ON UPPER (sls.asm_id) = UPPER (dstr.asmid)) distrb,
     itg_vn_dms_sales_org_dim org,
     (SELECT jj_year,
             jj_qrtr,
             jj_mnth_id,
             jj_mnth_no,
             jj_mnth_wk_no,
             dstrbtr_id,
             salesrep_id,
             MIN(visit_date) AS visit_date,
             MIN(target_by_week) AS target_by_week,
             MIN(target_by_month) AS target_by_month
      FROM itg_vn_productivity_call_target
      GROUP BY jj_year,
               jj_qrtr,
               jj_mnth_id,
               jj_mnth_no,
               jj_mnth_wk_no,
               dstrbtr_id,
               salesrep_id) PC,
     (SELECT distributor_id,
             sap_sold_to_code,
             mapp.region,
             sap_ship_to_code,
             territory_dist
      FROM itg_vn_distributor_sap_sold_to_mapping mapp,
           itg_vn_dms_distributor_dim
      WHERE dstrbtr_id = distributor_id) sap_sold_to_map
WHERE PC.dstrbtr_id = distrb.dstrbtr_id
AND   sap_sold_to_map.distributor_id = nvl(distrb.mapped_spk,distrb.dstrbtr_id)
AND   sap_sold_to_map.territory_dist = distrb.territory_dist
AND   PC.dstrbtr_id = org.dstrbtr_id (+)
AND   PC.salesrep_id = org.salesrep_id (+)
),
union_10 as (
SELECT jj_year,
       jj_qrtr,
       jj_mnth_id,
       jj_mnth_no,
       jj_mnth_wk_no,
	   NULL AS wks_in_qrtr,
       distrb.territory_dist,
       nvl(distrb.mapped_spk,distrb.dstrbtr_id) distributor_id_report,
       org.dstrbtr_id AS dstrbtr_grp_cd,
       distrb.dstrbtr_type,
       sap_sold_to_map.sap_sold_to_code AS sap_sold_to_code,
       NULL AS sap_matl_num,
       NULL AS sap_matl_name,
       NULL AS dstrbtr_matl_num,
       NULL AS dstrbtr_matl_name,
       'GENERAL TRADE' AS trade_type,
       NULL AS bill_doc,
       NULL AS bill_date,
       NULL AS cust_cd,
       CE.salesrep_id AS slsmn_cd,
       org.salesrep_name AS slsmn_nm,
       org.active AS slsmn_name,
       NULL AS so_sls_qty,
       NULL AS so_ret_qty,
       NULL AS so_grs_trd_sls,
       NULL AS so_net_trd_sls,
       NULL AS so_avg_wk_tgt,
       NULL AS so_mnth_tgt,
       supervisor_code,
       supervisor_name,
       --distrb.asm_id,
       distrb.asmid,
       distrb.asm_name,
       distrb.region AS dstrb_region,
	   NULL AS  outlet_name,
	   NULL AS  shop_type,
       NULL AS  Group_hierarchy, 
       NULL AS  Top_Door_Group,
	   NULL AS  Top_Door_Target,
	   'N' AS  Top_Door_Flag,
       0 AS si_sls_qty,
       0 AS si_ret_qty,
       0 AS si_gts_val,
       0 AS si_nts_val,
       0 AS si_avg_wk_tgt,
       0 AS si_mnth_tgt,
       0 AS st_sls_qty,
       0 AS st_ret_qty,
       0 AS st_grs_trd_sls,
       0 AS st_net_trd_sls,
       0 AS st_avg_wk_tgt,
       0 AS st_mnth_tgt,
       NULL AS visit_call_date,
       NULL AS product_visit_call_date,
       NULL AS PC_Target,
       NULL AS PC_Target_by_week,
       CE.target_by_month AS CE_Target,
       CE.target_by_week AS CE_Target_by_week,
       NULL AS ub_weekly_target,
       NULL AS franchise,
       NULL AS brand,
       NULL AS variant,
       NULL AS product_group,
       NULL AS group_jb,
	   NULL AS groupmsl
FROM           (SELECT territory_dist,
                 mapped_spk ,
                 dstrbtr_id,
                 dstrbtr_type,
                 dstrbtr_name,
                 asmid,
                 region,
                 sls.asm_name
    	  FROM 		 
	 (SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asmid,
             region
             --sls.asm_name
      FROM       
      (
           SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asm_id as asmid,
             region,
             Row_number() over (partition by dstrbtr_id order by crtd_dttm desc) as rn 
      FROM itg_vn_dms_distributor_dim) where rn = 1) dstr
        LEFT JOIN 
		(select asm_id, asm_name from
(select asm_id, asm_name,row_number() over (partition by asm_id order by crtd_dttm desc) as rnk from itg_vn_dms_sales_org_dim)
where rnk=1) sls ON UPPER (sls.asm_id) = UPPER (dstr.asmid)) distrb,
     itg_vn_dms_sales_org_dim org,
     (SELECT jj_year,
             jj_qrtr,
             jj_mnth_id,
             jj_mnth_no,
             jj_mnth_wk_no,
             dstrbtr_id,
             salesrep_id,
             MIN(visit_date) AS visit_date,
             MIN(target_by_week) AS target_by_week,
             MIN(target_by_month) AS target_by_month
      FROM itg_vn_visit_call_target
      GROUP BY jj_year,
               jj_qrtr,
               jj_mnth_id,
               jj_mnth_no,
               jj_mnth_wk_no,
               dstrbtr_id,
               salesrep_id) CE,
     (SELECT distributor_id,
             sap_sold_to_code,
             mapp.region,
             sap_ship_to_code,
             territory_dist
      FROM itg_vn_distributor_sap_sold_to_mapping mapp,
           itg_vn_dms_distributor_dim
      WHERE dstrbtr_id = distributor_id) sap_sold_to_map
WHERE CE.dstrbtr_id = distrb.dstrbtr_id
AND   sap_sold_to_map.distributor_id = nvl(distrb.mapped_spk,distrb.dstrbtr_id)
AND   sap_sold_to_map.territory_dist = distrb.territory_dist
AND   CE.dstrbtr_id = org.dstrbtr_id (+)
AND   CE.salesrep_id = org.salesrep_id (+)
),
union_11 as (
SELECT timedim."year" AS jj_year,
       timedim.qrtr AS jj_qrtr,
       timedim.mnth_id AS jj_mnth_id,
       timedim.mnth_no AS jj_mnth_no,
       timedim.mnth_wk_no AS jj_mnth_wk_no,
	   NULL AS wks_in_qrtr,
       distsoldtomap.territory_dist,
       --nvl(dstrb.dstr_mapped_spk,dstrb.dstr_dstrbtr_id) distributor_id_report,
       nvl(dstrb.mapped_spk,dstrb.dstrbtr_id) distributor_id_report,       
       t.dstrbtr_id AS dstrbtr_grp_cd,
       dstrb.dstrbtr_type,
       distsoldtomap.sap_sold_to_code,
       NULL AS sap_matl_num,
       NULL AS sap_matl_name,
       NULL AS dstrbtr_matl_num,
       NULL AS dstrbtr_matl_name,
       'GENERAL TRADE' AS trade_type,
       NULL AS bill_doc,
       NULL AS bill_date,
       NULL AS cust_cd,
       t.saleman_code AS slsmn_cd,
       org.salesrep_name AS slsmn_nm,
       org.active AS slsmn_status,
       NULL AS sls_qty,
       NULL AS ret_qty,
       NULL AS grs_trd_sls,
       NULL AS net_trd_sls,
       NULL AS so_avg_wk_tgt,
       NULL AS so_mnth_tgt,
       supervisor_code,
       supervisor_name,
       --dstrb.asm_id
       dstrb.asmid,
       dstrb.asm_name,
       dstrb.region dstrb_region,
	   NULL AS  outlet_name,
	   NULL AS  shop_type,
       NULL AS  Group_hierarchy, 
       NULL AS  Top_Door_Group,
	   NULL AS  Top_Door_Target,
	   'N' AS  Top_Door_Flag,
       0 AS si_sls_qty,
       0 AS si_ret_qty,
       0 AS si_gts_val,
       0 AS si_nts_val,
       0 AS si_avg_wk_tgt,
       0 AS si_mnth_tgt,
       0 AS st_sls_qty,
       0 AS st_ret_qty,
       0 AS st_grs_trd_sls,
       0 AS st_net_trd_sls,
       0 AS st_avg_wk_tgt,
       0 AS st_mnth_tgt,
       NULL AS visit_call_date,
       NULL AS product_visit_call_date,
       NULL AS PC_Target,
       NULL AS PC_Target_by_week,
       NULL AS CE_Target,
       NULL AS CE_Target_by_week,
       ub_weekly_target,
       NULL AS franchise,
       NULL AS brand,
       NULL AS variant,
       NULL AS product_group,
       NULL AS group_jb,
	   NULL AS groupmsl
FROM (SELECT edw_vw_os_time_dim."year",
             edw_vw_os_time_dim.qrtr,
             edw_vw_os_time_dim.mnth_id,
             edw_vw_os_time_dim.mnth_no,
             edw_vw_os_time_dim.wk,
             edw_vw_os_time_dim.mnth_wk_no,
             MIN(edw_vw_os_time_dim.cal_date) AS cal_date
      FROM edw_vw_os_time_dim
      GROUP BY edw_vw_os_time_dim."year",
               edw_vw_os_time_dim.qrtr,
               edw_vw_os_time_dim.mnth_id,
               edw_vw_os_time_dim.mnth_no,
               edw_vw_os_time_dim.wk,
               edw_vw_os_time_dim.mnth_wk_no) timedim,
     (SELECT distributor_id,
             sap_sold_to_code,
             mapp.region,
             sap_ship_to_code,
             territory_dist
      FROM itg_vn_distributor_sap_sold_to_mapping mapp,
           itg_vn_dms_distributor_dim
      WHERE dstrbtr_id = distributor_id) distsoldtomap,
          (SELECT territory_dist,
                 mapped_spk ,
                 dstrbtr_id ,
                 dstrbtr_type,
                 dstrbtr_name,
                 asmid,
                 region,
                 sls.asm_name
    	  FROM 		 
	 (SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asmid,
             region
             --sls.asm_name
      FROM       
      (
           SELECT territory_dist,
             mapped_spk,
             dstrbtr_id,
             dstrbtr_type,
             dstrbtr_name,
             asm_id as asmid,
             region,
             Row_number() over (partition by dstrbtr_id order by crtd_dttm desc) as rn 
      FROM itg_vn_dms_distributor_dim) where rn = 1) dstr
        LEFT JOIN 
		(select asm_id, asm_name from
(select asm_id, asm_name,row_number() over (partition by asm_id order by crtd_dttm desc) as rnk from itg_vn_dms_sales_org_dim)
where rnk=1) sls ON UPPER (sls.asm_id) = UPPER (dstr.asmid)) dstrb,
     (SELECT a.*,
             target_value /MAX(mnth_wk_no) OVER (PARTITION BY mnth_id,saleman_code) AS ub_weekly_target,
             b.*
      FROM (SELECT * FROM itg_vn_dms_kpi WHERE kpi_type = 'UB') a
        LEFT JOIN (SELECT * FROM wrk_vn_mnth_week) b ON a.cycle = b.mnth_id) t,
     itg_vn_dms_sales_org_dim org
WHERE t.dstrbtr_id = dstrb.dstrbtr_id
--t.dstrbtr_id = dstrb.dstr_dstrbtr_id
AND   t.cycle = CAST(jj_mnth_id AS NUMERIC)
AND   t.mnth_wk_no = jj_mnth_wk_no
AND   t.saleman_code = org.salesrep_id (+)
AND   t.dstrbtr_id = org.dstrbtr_id (+)
AND   distsoldtomap.territory_dist = dstrb.territory_dist
AND   t.cycle > '201904'
--AND   distsoldtomap.distributor_id = nvl(dstrb.mapped_spk,dstrb.dstrbtr_id)
AND   distsoldtomap.distributor_id = nvl(dstrb.mapped_spk,dstrb.dstrbtr_id)
),
transformed as (
SELECT sell_out.jj_year,
       sell_out.jj_qrtr AS jj_qrtr,
       sell_out.jj_mnth_id AS jj_mnth_id,
       sell_out.jj_mnth_no,
       sell_out.jj_mnth_wk_no,
	   qrtr_wk.wks_in_qrtr,
       territory_dist,
       distributor_id_report,
       sell_out.dstrbtr_grp_cd,
       sell_out.dstrbtr_type,
       sell_out.sap_sold_to_code,
       sell_out.sap_matl_num,
       prod_dim.productnamesap AS sap_matl_name,
       sell_out.dstrbtr_matl_num,
       prod_dim.product_name AS dstrbtr_matl_name,
       'GENERAL TRADE' AS trade_type,
       sell_out.bill_doc AS bill_doc,
       sell_out.bill_date AS bill_date,
       sell_out.cust_cd,
       sell_out.slsmn_cd,
       sell_out.slsmn_nm,
       sell_out.slsmn_status,
       sell_out.sls_qty AS so_sls_qty,
       sell_out.ret_qty AS so_ret_qty,
       sell_out.grs_trd_sls AS so_grs_trd_sls,
       sell_out.net_trd_sls AS so_net_trd_sls,
       so_avg_wk_tgt,
       so_mnth_tgt,
       supervisor_code,
       supervisor_name,
       asm_id,
       asm_name,
       dstrb_region,
	   outlet_name,
       shop_type,
	   Group_hierarchy, 
	   Top_Door_Group,
       Top_Door_Target,
	   Top_Door_Flag,
       0 AS si_sls_qty,
       0 AS si_ret_qty,
       0 AS si_gts_val,
       0 AS si_nts_val,
       0 AS si_avg_wk_tgt,
       0 AS si_mnth_tgt,
       0 AS st_sls_qty,
       0 AS st_ret_qty,
       0 AS st_grs_trd_sls,
       0 AS st_net_trd_sls,
       0 AS st_avg_wk_tgt,
       0 AS st_mnth_tgt,
       NULL AS visit_call_date,
       NULL AS product_visit_call_date,
       NULL AS PC_Target,
       NULL AS PC_Target_by_week,
       NULL AS CE_Target,
       NULL AS CE_Target_by_week,
       NULL AS ub_weekly_target,
       prod_dim.franchise,
       prod_dim.brand,
       prod_dim.variant,
       prod_dim.product_group,
       prod_dim.group_jb,
	--case when status='A' then
	 case when sum(sell_out.net_trd_sls) OVER (partition by sell_out.jj_qrtr,sell_out.cust_cd,sell_out.dstrbtr_matl_num) >0 
	 THEN msl_base.groupmsl else Null  END  
	 /*end*/ AS groupmsl 
FROM ( select * from union_1 
       UNION ALL
       select * from union_2
       UNION ALL
	   	  
    select * from union_3
       UNION ALL
    select * from union_4
 ) sell_out
  LEFT JOIN itg_vn_dms_product_dim prod_dim ON prod_dim.product_code = sell_out.dstrbtr_matl_num
  LEFT JOIN (select *, 
			 row_number() over ( partition by qrtr order by wk) wks_in_qrtr
			 from
			 (select distinct qrtr, wk from edw_vw_os_time_dim)) qrtr_wk ON sell_out.jj_qrtr = qrtr_wk.qrtr AND sell_out.wk = qrtr_wk.wk
  LEFT JOIN (select distinct from_cycle
					,to_cycle
					,product_id
					,sub_channel.shoptype_name
					,groupmsl
					from itg_vn_dms_msl msl
					LEFT JOIN itg_mds_vn_gt_msl_shoptype_mapping sub_channel ON sub_channel.msl_subchannel = msl.sub_channel
					WHERE msl.active='True') msl_base
	ON (sell_out.jj_mnth_id>=from_cycle AND sell_out.jj_mnth_id<=to_cycle)
	AND coalesce(nullif(sell_out.shop_type,''),'NA') = coalesce(nullif(shoptype_name,''),'NA')
	AND sell_out.dstrbtr_matl_num = product_id
UNION ALL
select * from union_5
UNION ALL
select * from union_6
UNION ALL
select * from union_7
UNION ALL
select * from union_8
UNION ALL
select * from union_9
UNION ALL
select * from union_10
union all
select * from union_11

),
final as (
select
jj_year::number(18,0) as jj_year,
jj_qrtr::varchar(14) as jj_qrtr,
jj_mnth_id::varchar(23) as jj_mnth_id,
jj_mnth_no::number(18,0) as jj_mnth_no,
jj_mnth_wk_no::number(38,0) as jj_mnth_wk_no,
wks_in_qrtr::number(38,0) as wks_in_qrtr,
territory_dist::varchar(100) as territory_dist,
distributor_id_report::varchar(30) as distributor_id_report,
dstrbtr_grp_cd::varchar(100) as dstrbtr_grp_cd,
dstrbtr_type::varchar(100) as dstrbtr_type,
sap_sold_to_code::varchar(50) as sap_sold_to_code,
sap_matl_num::varchar(50) as sap_matl_num,
sap_matl_name::varchar(100) as sap_matl_name,
dstrbtr_matl_num::varchar(50) as dstrbtr_matl_num,
dstrbtr_matl_name::varchar(100) as dstrbtr_matl_name,
trade_type::varchar(13) as trade_type,
bill_doc::varchar(30) as bill_doc,
bill_date::date as bill_date,
cust_cd::varchar(100) as cust_cd,
slsmn_cd::varchar(50) as slsmn_cd,
slsmn_nm::varchar(100) as slsmn_nm,
slsmn_status::varchar(1) as slsmn_status,
so_sls_qty::number(15,4) as so_sls_qty,
so_ret_qty::number(15,4) as so_ret_qty,
so_grs_trd_sls::number(15,4) as so_grs_trd_sls,
so_net_trd_sls::number(15,4) as so_net_trd_sls,
so_avg_wk_tgt::number(28,6) as so_avg_wk_tgt,
so_mnth_tgt::number(23,6) as so_mnth_tgt,
supervisor_code::varchar(50) as supervisor_code,
supervisor_name::varchar(100) as supervisor_name,
asm_id::varchar(50) as asm_id,
asm_name::varchar(100) as asm_name,
dstrb_region::varchar(20) as dstrb_region,
outlet_name::varchar(500) as outlet_name,
shop_type::varchar(100) as shop_type,
group_hierarchy::varchar(100) as group_hierarchy,
top_door_group::varchar(100) as top_door_group,
top_door_target::number(18,2) as top_door_target,
top_door_flag::varchar(1) as top_door_flag,
si_sls_qty::number(38,4) as si_sls_qty,
si_ret_qty::number(38,4) as si_ret_qty,
si_gts_val::number(38,4) as si_gts_val,
si_nts_val::number(38,4) as si_nts_val,
si_avg_wk_tgt::number(25,4) as si_avg_wk_tgt,
si_mnth_tgt::number(20,4) as si_mnth_tgt,
st_sls_qty::number(14,2) as st_sls_qty,
st_ret_qty::number(14,2) as st_ret_qty,
st_grs_trd_sls::number(15,4) as st_grs_trd_sls,
st_net_trd_sls::number(17,4) as st_net_trd_sls,
st_avg_wk_tgt::number(25,4) as st_avg_wk_tgt,
st_mnth_tgt::number(20,4) as st_mnth_tgt,
visit_call_date::date as visit_call_date,
product_visit_call_date::date as product_visit_call_date,
pc_target::number(20,4) as pc_target,
pc_target_by_week::number(20,4) as pc_target_by_week,
ce_target::number(20,4) as ce_target,
ce_target_by_week::number(20,4) as ce_target_by_week,
ub_weekly_target::number(35,24) as ub_weekly_target,
franchise::varchar(50) as franchise,
brand::varchar(100) as brand,
variant::varchar(100) as variant,
product_group::varchar(200) as product_group,
group_jb::varchar(20) as group_jb,
groupmsl::varchar(100) as groupmsl
from transformed
)
select * from final