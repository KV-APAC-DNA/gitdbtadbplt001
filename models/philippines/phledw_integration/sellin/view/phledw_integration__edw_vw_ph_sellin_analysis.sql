with 
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_mds_ph_pos_pricelist as
(
    select * from {{ ref('phlitg_integration__itg_mds_ph_pos_pricelist') }}
),
edw_vw_ph_sellin_sales_fact as
(
    select * from {{ ref('phledw_integration__edw_vw_ph_sellin_sales_fact') }}
),
edw_vw_ph_material_dim as
(
    select * from {{ ref('phledw_integration__edw_vw_ph_material_dim') }}
),
edw_product_key_attributes as
(
    select * from {{ ref('aspedw_integration__edw_product_key_attributes') }}
),
itg_mds_ph_lav_product as
(
    select * from {{ ref('phlitg_integration__itg_mds_ph_lav_product') }}
),
edw_vw_ph_customer_dim as
(
    select * from {{ ref('phledw_integration__edw_vw_ph_customer_dim') }}
),
edw_mv_ph_customer_dim as
(
    select * from {{ ref('phledw_integration__edw_mv_ph_customer_dim') }}
) ,
itg_mds_ph_pos_pricelist as
(
    select * from {{ ref('phlitg_integration__itg_mds_ph_pos_pricelist') }}
),
itg_ph_iop_trgt as
(
    select * from {{ ref('phlitg_integration__itg_ph_iop_trgt') }}
),
itg_ph_bp_trgt as
(
    select * from {{ ref('phlitg_integration__itg_ph_bp_trgt') }}
),
itg_ph_le_trgt as
(
    select * from {{ source('phlitg_integration','itg_ph_le_trgt') }}
),
final as
(
    select veossf.jj_year,
       veossf.jj_qtr,
       veossf.jj_mnth_id,
       veossf.jj_mnth_no,
       null as jj_wk,
       veossf.jj_mnth_wk_no as jj_mnth_wk_no,
       veossf.bill_typ as sellin_bill_type,
       null as sls_grp_cd,
       epcad.rpt_grp_2_desc as sls_grp_desc,
       veocd.sap_cntry_nm as cntry_nm,
       epcad.region as region,
       veocd.sap_state_cd as sap_state_cd,
       ltrim(veocd.sap_cust_id,'0') as sold_to,
       veocd.sap_cust_nm as sold_to_nm,
       veocd.sap_sls_org as sap_sls_org,
       veocd.sap_cmp_id,
       veocd.sap_cntry_cd,
       veocd.sap_cntry_nm,
       veocd.sap_addr,
       veocd.sap_region,
       veocd.sap_city,
       veocd.sap_post_cd,
       veocd.sap_chnl_cd,
       veocd.sap_chnl_desc,
       veocd.sap_sls_office_cd,
       veocd.sap_sls_office_desc,
       veocd.sap_sls_grp_cd,
       veocd.sap_sls_grp_desc,
       veocd.sap_curr_cd,
       veocd.gch_region,
       veocd.gch_cluster,
       veocd.gch_subcluster,
       veocd.gch_market,
       veocd.gch_retail_banner,
       epcad.channel_cd as chnl_cd,
       epcad.channel_desc as chnl_desc,
       epcad.sub_chnl_cd as sub_chnl_cd,
       epcad.sub_chnl_desc as sub_chnl_desc,
       ltrim(veomd.sap_matl_num,'0') as sku,
       veomd.sap_mat_desc as sku_desc,
       veomd.gph_region as global_mat_region,
       veomd.gph_prod_frnchse as global_prod_franchise,
       veomd.gph_prod_brnd as global_prod_brand,
       veomd.gph_prod_vrnt as global_prod_variant,
       veomd.gph_prod_put_up_cd as global_prod_put_up_cd,
       veomd.gph_prod_put_up_desc as global_put_up_desc,
       veomd.sap_prod_mnr_cd,
       veomd.sap_prod_mnr_desc,
       veomd.sap_prod_hier_cd,
       veomd.sap_prod_hier_desc,
       veomd.gph_prod_sub_brnd as global_prod_sub_brand,
       veomd.gph_prod_needstate as global_prod_need_state,
       veomd.gph_prod_ctgry as global_prod_category,
       veomd.gph_prod_subctgry as global_prod_subcategory,
       upper(veomd.gph_prod_sgmnt) as global_prod_segment,
       veomd.gph_prod_subsgmnt as global_prod_subsegment,
       veomd.gph_prod_size as global_prod_size,
       veomd.gph_prod_size_uom as global_prod_size_uom,
       epcad.parent_cust_cd as parent_customer_cd,
       epcad.parent_cust_nm as parent_customer,
       epcad.rpt_grp_6_desc as account_grp,
       epcad.rpt_grp_1_desc as trade_type,
       case
         when upper(epmad.promo_reg_ind) = 'REG' THEN 'Y'
         ELSE 'N'
       end as is_reg,
       case
         when upper(epmad.promo_reg_ind) = 'PROMO' THEN 'Y'
         ELSE 'N'
       end as is_promo,
       epmad.promo_strt_period as local_mat_promo_strt_period,
       case
         when epp2.status = '**' and (veotd2.mnth_id between epp2.launch_period and epp2.end_period) then 'Y'
         ELSE 'N'
       end as is_npi,
       case
         when upper(epmad.hero_sku_ind) = 'Y' THEN 'HERO'
         ELSE 'NA'
       end as is_hero,
       null as is_mcl,
       epp2.launch_period as local_mat_npi_strt_period,
       epp.lst_price_unit as local_mat_listpriceunit,
       veossf.sls_qty,
       veossf.ret_qty,
       veossf.sls_less_rtn_qty,
       veossf.gts_val,
       veossf.ret_val,
       veossf.gts_less_rtn_val,
       veossf.nts_qty,
       veossf.nts_val,
       veossf.tp_val,
       null as target_type,
       0 as iop_trgt_val,
       0 as bp_trgt_val,
       0 as le_trgt_val,
       cast(null as timestamp) as last_updt_dt ,
       cast(null as numeric(15,4)) as iop_gts_trgt_val ,
	   veomd.pka_productkey 
from (select mnth_id
      from edw_vw_os_time_dim
      where cal_date = current_timestamp::date) veotd2,
     (select status,
             item_cd,
             min(jj_mnth_id) as launch_period,
             MIN(left(replace(add_months(to_date(JJ_MNTH_ID||'01','yyyymmdd'),11),'-',''),6)) AS END_PERIOD 
      from itg_mds_ph_pos_pricelist
      where status = '**'
      and   active = 'Y'
      group by status,
               item_cd) epp2,
     (select bill_typ,
             veotd.mnth_id as jj_mnth_id,
             veotd.mnth_no as jj_mnth_no,
             veotd."year" as jj_year,
             veotd.qrtr as jj_qtr,
             veotd.mnth_wk_no as jj_mnth_wk_no,
             cust_id,
             item_cd,
             cntry_nm,
             sum(sls_qty) as sls_qty,
             sum(ret_qty) as ret_qty,
             sum(sls_less_rtn_qty) as sls_less_rtn_qty,
             sum(gts_val) as gts_val,
             sum(ret_val) as ret_val,
             sum(gts_less_rtn_val) as gts_less_rtn_val,
             sum(nts_qty) as nts_qty,
             sum(nts_val) as nts_val,
             sum(tp_val) as tp_val
      from edw_vw_ph_sellin_sales_fact veossf1,
           (select a."year",
                   a.qrtr_no,
                   a.qrtr,
                   a.mnth_id,
                   a.mnth_desc,
                   a.mnth_no,
                   a.mnth_shrt,
                   a.mnth_long,
                   a.mnth_wk_no,
                   case
                     when a.mnth_wk_no = 1 then '19000101'
                     else (min(a.cal_date_id))
                   end as first_day,
                   case
                     when a.mnth_wk_no = b.mnth_wk_no then '20991231'
                     else (max(a.cal_date_id))
                   end as last_day
            from edw_vw_os_time_dim a
            join
             (select mnth_id,max(mnth_wk_no) as mnth_wk_no from edw_vw_os_time_dim group by mnth_id) b
             on a.mnth_id = b.mnth_id
            group by a."year",
                     a.qrtr_no,
                     a.qrtr,
                     a.mnth_id,
                     a.mnth_desc,
                     a.mnth_no,
                     a.mnth_shrt,
                     a.mnth_long,
                     a.mnth_wk_no,
                     b.mnth_wk_no) veotd
      where (sls_qty <> 0 or ret_qty <> 0 or gts_val <> 0 or ret_val <> 0 or nts_val <> 0 or tp_val <> 0)
      and   cntry_nm = 'PH'
      and   trim(veossf1.jj_mnth_id) = trim(cast((veotd.mnth_id) as varchar))
      and   veossf1.pstng_dt between veotd.first_day and veotd.last_day
      group by bill_typ,
               veotd.mnth_id,
               veotd.mnth_no,
               veotd."year",
               veotd.qrtr,
               veotd.mnth_wk_no,
               cust_id,
               item_cd,
               cntry_nm) veossf,
     (select mat.*, prod.pka_productkey
      from edw_vw_ph_material_dim mat 
	  left join edw_product_key_attributes prod
      on ltrim(mat.sap_matl_num,'0') =  ltrim(prod.matl_num,'0')
      where cntry_key = 'PH' and upper(prod.ctry_nm) = 'PHILIPPINES') veomd,
     (select item_cd,
             promo_reg_ind,
             hero_sku_ind,
             promo_strt_period,
             npi_strt_period
      from itg_mds_ph_lav_product
      where active = 'Y') epmad,
     (select *
      from edw_vw_ph_customer_dim
      where sap_cntry_cd = 'PH') veocd,
     edw_mv_ph_customer_dim epcad,
     (select *
      from itg_mds_ph_pos_pricelist
      where active = 'Y') epp
     
where upper(ltrim(veocd.sap_cust_id(+),0)) = upper(ltrim(veossf.cust_id,0))
and   upper(ltrim(veomd.sap_matl_num(+),0)) = upper(ltrim(veossf.item_cd,0))
and   upper(trim(epmad.item_cd(+))) = upper(ltrim(veossf.item_cd,0))
and   upper(trim(epcad.cust_id(+))) = upper(ltrim(veossf.cust_id,0))
and   upper(trim(epp.item_cd(+))) = upper(ltrim(veossf.item_cd,0))
and   trim(epp.jj_mnth_id(+)) = veossf.jj_mnth_id
and   upper(trim(epp2.item_cd(+))) = upper(ltrim(veossf.item_cd,0))
 

 
union all
 
select  
       veotd."year" as jj_year,
       veotd.qrtr as jj_qtr,
       veotd.mnth_id as jj_mnth_id,
       veotd.mnth_no as jj_mnth_no,
       null as jj_wk,
       null as jj_mnth_wk_no,
       null as sellin_bill_type,
       null as sls_grp_cd,
       epcad.rpt_grp_2_desc as sls_grp_desc,
       'PH' as cntry_nm,
       epcad.region as region,
       veocd.sap_state_cd as sap_state_cd,
       veocd.sap_cust_id as sold_to,
       veocd.sap_cust_nm as sold_to_nm,
       veocd.sap_sls_org as sap_sls_org,
       veocd.sap_cmp_id,
       veocd.sap_cntry_cd,
       veocd.sap_cntry_nm,
       veocd.sap_addr,
       veocd.sap_region,
       veocd.sap_city,
       veocd.sap_post_cd,
       veocd.sap_chnl_cd,
       veocd.sap_chnl_desc,
       veocd.sap_sls_office_cd,
       veocd.sap_sls_office_desc,
       veocd.sap_sls_grp_cd,
       veocd.sap_sls_grp_desc,
       veocd.sap_curr_cd,
       veocd.gch_region,
       veocd.gch_cluster,
       veocd.gch_subcluster,
       veocd.gch_market,
       veocd.gch_retail_banner,
       epcad.channel_cd as chnl_cd,
       epcad.channel_desc as chnl_desc,
       epcad.sub_chnl_cd as sub_chnl_cd,
       epcad.sub_chnl_desc as sub_chnl_desc,
       null as sku,
       null as sku_desc,
       null as global_mat_region,
       veomd.gph_prod_frnchse as global_prod_franchise,
       veomd.gph_prod_brnd as global_prod_brand,
       null as global_prod_variant,
       null as global_prod_put_up_cd,
       null as global_put_up_desc,
       null as sap_prod_mnr_cd,
       null as sap_prod_mnr_desc,
       null as sap_prod_hier_cd,
       null as sap_prod_hier_desc,
       null as global_prod_sub_brand,
       null as global_prod_need_state,
       null as global_prod_category,
       null as global_prod_subcategory,
       upper(epit.segment) as global_prod_segment,
       null as global_prod_subsegment,
       null as global_prod_size,
       null as global_prod_size_uom,
       epcad.parent_cust_cd as parent_customer_cd,
       epcad.parent_cust_nm as parent_customer,
       epcad.rpt_grp_6_desc as account_grp,
       epcad.rpt_grp_1_desc as trade_type,
       null as is_reg,
       null as is_promo,
       null as local_mat_promo_strt_period,
       null as is_npi,
       null as is_hero,
       null as is_mcl,
       null as local_mat_npi_strt_period,
       null as local_mat_listpriceunit,
       0 as sls_qty,
       0 as ret_qty,
       0 as sls_less_rtn_qty,
       0 as gts_val,
       0 as ret_val,
       0 as gts_less_rtn_val,
       0 as nts_qty,
       0 as nts_val,
       0 as tp_val,
       'IOP' as target_type,
       epit.tp_trgt_amt as iop_trgt_val,
       0 as bp_trgt_val ,
       0 as le_trgt_val,
       epit.crtd_dttm as last_updt_dt,
       epit.gts_trgt_amt as iop_gts_trgt_val ,
      null as pka_productkey
from itg_ph_iop_trgt epit,
     (select distinct "year",
             qrtr_no,
             qrtr,
             mnth_id,
             mnth_desc,
             mnth_no,
             mnth_shrt,
             mnth_long
      from edw_vw_os_time_dim) veotd,
     edw_mv_ph_customer_dim epcad,
     edw_vw_ph_customer_dim veocd,
     (select distinct gph_prod_brnd,
             gph_prod_frnchse
             from edw_vw_ph_material_dim
      where gph_prod_brnd is not null
      and   cntry_key = 'PH') veomd
where veotd.mnth_id = epit.jj_mnth_id
and   ltrim(veocd.sap_cust_id(+),0) = ltrim(epit.cust_id,0)
and   trim(epcad.cust_id(+)) = trim(epit.cust_id)
and   upper(veomd.gph_prod_brnd(+)) = upper(epit.brnd_cd) 

union all
select 
       veotd."year" as jj_year,
       veotd.qrtr as jj_qtr,
       veotd.mnth_id as jj_mnth_id,
       veotd.mnth_no as jj_mnth_no,
       null as jj_wk,
       null as jj_mnth_wk_no,
       null as sellin_bill_type,
       null as sls_grp_cd,
       epcad.rpt_grp_2_desc as sls_grp_desc,
       'PH' as cntry_nm,
       epcad.region as region,
       veocd.sap_state_cd as sap_state_cd,
       veocd.sap_cust_id as sold_to,
       veocd.sap_cust_nm as sold_to_nm,
       veocd.sap_sls_org as sap_sls_org,
       veocd.sap_cmp_id,
       veocd.sap_cntry_cd,
       veocd.sap_cntry_nm,
       veocd.sap_addr,
       veocd.sap_region,
       veocd.sap_city,
       veocd.sap_post_cd,
       veocd.sap_chnl_cd,
       veocd.sap_chnl_desc,
       veocd.sap_sls_office_cd,
       veocd.sap_sls_office_desc,
       veocd.sap_sls_grp_cd,
       veocd.sap_sls_grp_desc,
       veocd.sap_curr_cd,
       veocd.gch_region,
       veocd.gch_cluster,
       veocd.gch_subcluster,
       veocd.gch_market,
       veocd.gch_retail_banner,
       epcad.channel_cd as chnl_cd,
       epcad.channel_desc as chnl_desc,
       epcad.sub_chnl_cd as sub_chnl_cd,
       epcad.sub_chnl_desc as sub_chnl_desc,
       null as sku,
       null as sku_desc,
       null as global_mat_region,
       veomd.gph_prod_frnchse as global_prod_franchise,
       veomd.gph_prod_brnd as global_prod_brand,
       null as global_prod_variant,
       null as global_prod_put_up_cd,
       null as global_put_up_desc,
       null as sap_prod_mnr_cd,
       null as sap_prod_mnr_desc,
       null as sap_prod_hier_cd,
       null as sap_prod_hier_desc,
       null as global_prod_sub_brand,
       null as global_prod_need_state,
       null as global_prod_category,
       null as global_prod_subcategory,
       null as global_prod_segment,
       null as global_prod_subsegment,
       null as global_prod_size,
       null as global_prod_size_uom,
       epcad.parent_cust_cd as parent_customer_cd,
       epcad.parent_cust_nm as parent_customer,
       epcad.rpt_grp_6_desc as account_grp,
       epcad.rpt_grp_1_desc as trade_type,
       null as is_reg,
       null as is_promo,
       null as local_mat_promo_strt_period,
       null as is_npi,
       null as is_hero,
       null as is_mcl,
       null as local_mat_npi_strt_period,
       null as local_mat_listpriceunit,
       0 as sls_qty,
       0 as ret_qty,
       0 as sls_less_rtn_qty,
       0 as gts_val,
       0 as ret_val,
       0 as gts_less_rtn_val,
       0 as nts_qty,
       0 as nts_val,
       0 as tp_val,
       'BP' as target_type,
       0 as iop_trgt_val,
       epit.tp_trgt_amt as bp_trgt_val,
       0 as le_trgt_val,
       epit.crtd_dttm as last_updt_dt ,
      cast ( null as numeric(15,4) ) as iop_gts_trgt_val ,
       null as pka_productkey
from itg_ph_bp_trgt epit,
     (select distinct "year",
             qrtr_no,
             qrtr,
             mnth_id,
             mnth_desc,
             mnth_no,
             mnth_shrt,
             mnth_long
      from edw_vw_os_time_dim) veotd,
     edw_mv_ph_customer_dim epcad,
     edw_vw_ph_customer_dim veocd,
     (select distinct gph_prod_brnd,
             gph_prod_frnchse
      from edw_vw_ph_material_dim) veomd
where veotd.mnth_id = epit.jj_mnth_id
and   ltrim(veocd.sap_cust_id(+),0) = ltrim(epit.cust_id,0)
and   trim(epcad.cust_id(+)) = trim(epit.cust_id)
and   upper(veomd.gph_prod_brnd(+)) = upper(epit.brnd_cd)

union all
select  
        veotd."year" as jj_year,
       veotd.qrtr as jj_qtr,
       veotd.mnth_id as jj_mnth_id,
       veotd.mnth_no as jj_mnth_no,
       null as jj_wk,
       null as jj_mnth_wk_no,
       null as sellin_bill_type,
       null as sls_grp_cd,
       epcad.rpt_grp_2_desc as sls_grp_desc,
       'PH' as cntry_nm,
       epcad.region as region,
       veocd.sap_state_cd as sap_state_cd,
       veocd.sap_cust_id as sold_to,
       veocd.sap_cust_nm as sold_to_nm,
       veocd.sap_sls_org as sap_sls_org,
       veocd.sap_cmp_id,
       veocd.sap_cntry_cd,
       veocd.sap_cntry_nm,
       veocd.sap_addr,
       veocd.sap_region,
       veocd.sap_city,
       veocd.sap_post_cd,
       veocd.sap_chnl_cd,
       veocd.sap_chnl_desc,
       veocd.sap_sls_office_cd,
       veocd.sap_sls_office_desc,
       veocd.sap_sls_grp_cd,
       veocd.sap_sls_grp_desc,
       veocd.sap_curr_cd,
       veocd.gch_region,
       veocd.gch_cluster,
       veocd.gch_subcluster,
       veocd.gch_market,
       veocd.gch_retail_banner,
       epcad.channel_cd as chnl_cd,
       epcad.channel_desc as chnl_desc,
       epcad.sub_chnl_cd as sub_chnl_cd,
       epcad.sub_chnl_desc as sub_chnl_desc,
       null as sku,
       null as sku_desc,
       null as global_mat_region,
       veomd.gph_prod_frnchse as global_prod_franchise,
       veomd.gph_prod_brnd as global_prod_brand,
       null as global_prod_variant,
       null as global_prod_put_up_cd,
       null as global_put_up_desc,
       null as sap_prod_mnr_cd,
       null as sap_prod_mnr_desc,
       null as sap_prod_hier_cd,
       null as sap_prod_hier_desc,
       null as global_prod_sub_brand,
       null as global_prod_need_state,
       null as global_prod_category,
       null as global_prod_subcategory,
       null as global_prod_segment,
       null as global_prod_subsegment,
       null as global_prod_size,
       null as global_prod_size_uom,
       epcad.parent_cust_cd as parent_customer_cd,
       epcad.parent_cust_nm as parent_customer,
       epcad.rpt_grp_6_desc as account_grp,
       epcad.rpt_grp_1_desc as trade_type,
       null as is_reg,
       null as is_promo,
       null as local_mat_promo_strt_period,
       null as is_npi,
       null as is_hero,
       null as is_mcl,
       null as local_mat_npi_strt_period,
       null as local_mat_listpriceunit,
       0 as sls_qty,
       0 as ret_qty,
       0 as sls_less_rtn_qty,
       0 as gts_val,
       0 as ret_val,
       0 as gts_less_rtn_val,
       0 as nts_qty,
       0 as nts_val,
       0 as tp_val,
       'LE' as target_type,
       0 as iop_trgt_val,
       0 as bp_trgt_val,
       epit.tp_trgt_amt as le_trgt_val,
       epit.crtd_dttm as last_updt_dt ,
      cast(null as numeric(15,4) ) as iop_gts_trgt_val ,
	  null as pka_productkey      
from itg_ph_le_trgt epit,
     (select distinct "year",
             qrtr_no,
             qrtr,
             mnth_id,
             mnth_desc,
             mnth_no,
             mnth_shrt,
             mnth_long
      from edw_vw_os_time_dim) veotd,
     edw_mv_ph_customer_dim epcad,
     edw_vw_ph_customer_dim veocd,
     (select distinct gph_prod_brnd,
             gph_prod_frnchse
      from edw_vw_ph_material_dim) veomd
where veotd.mnth_id = epit.jj_mnth_id
and   ltrim(veocd.sap_cust_id(+),0) = ltrim(epit.cust_id,0)
and   trim(epcad.cust_id(+)) = trim(epit.cust_id)
and   upper(veomd.gph_prod_brnd(+)) = upper(epit.brnd_cd)
)
select * from final