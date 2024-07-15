with edw_vw_ph_pos_sales_fact as (
select * from {{ ref('phledw_integration__edw_vw_ph_pos_sales_fact') }}
),
edw_vw_os_time_dim as (
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_mds_ph_ref_pos_primary_sold_to as (
select * from {{ ref('phlitg_integration__itg_mds_ph_ref_pos_primary_sold_to') }}
) ,
edw_vw_ph_pos_customer_dim as (
select * from {{ ref('phledw_integration__edw_vw_ph_pos_customer_dim') }}
),
edw_vw_ph_pos_material_dim as (
select * from {{ ref('phledw_integration__edw_vw_ph_pos_material_dim') }}
) ,
edw_vw_ph_material_dim as (
select * from {{ ref('phledw_integration__edw_vw_ph_material_dim') }}
), 
edw_vw_ph_customer_dim as (
select * from {{ ref('phledw_integration__edw_vw_ph_customer_dim') }}
), 
edw_mv_ph_customer_dim as (
select * from {{ ref('phledw_integration__edw_mv_ph_customer_dim') }}
),
edw_vw_ph_sellin_sales_fact as (
select * from {{ ref('phledw_integration__edw_vw_ph_sellin_sales_fact') }}
) ,
itg_mds_ph_pos_pricelist as (
select * from {{ ref('phlitg_integration__itg_mds_ph_pos_pricelist') }}
) ,
final as (
select veposf."year" as jj_year,
       veposf.qrtr as jj_qtr,
       veposf.jj_mnth_id,
       veposf.mnth_no as jj_mnth_no,
       veposf.cntry_nm as cntry_nm,
       veposf.cust_cd,
       veposf.cust_brnch_cd,
       veposf.brnch_nm as mt_cust_brnch_nm,
       veposf.item_cd,
       veposf.item_nm as mt_item_nm,
       veposf.sold_to,
       veocd.sap_cust_nm as sold_to_nm,
       eocd.region,
       eocd.channel_cd as chnl_cd,
       eocd.channel_desc as chnl_desc,
       eocd.sub_chnl_cd as sub_chnl_cd,
       eocd.sub_chnl_desc as sub_chnl_desc,
       eocd.parent_cust_cd as parent_customer_cd,
       eocd.parent_cust_nm as parent_customer,
       eocd.rpt_grp_6_desc as account_grp,
       'MODERN TRADE' as trade_type,
       eocd.rpt_grp_2_desc as sls_grp_desc,
       veocd.sap_state_cd,
       veocd.sap_sls_org,
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
       ltrim(veomd.sap_matl_num,'0') as sku,
       veomd.sap_mat_desc as sku_desc,
       veomd.sap_mat_type_cd,
       veomd.sap_mat_type_desc,
       veomd.sap_base_uom_cd,
       veomd.sap_prchse_uom_cd,
       veomd.sap_prod_sgmt_cd,
       veomd.sap_prod_sgmt_desc,
       veomd.sap_base_prod_cd,
       veomd.sap_base_prod_desc,
       veomd.sap_mega_brnd_cd,
       veomd.sap_mega_brnd_desc,
       veomd.sap_brnd_cd,
       veomd.sap_brnd_desc,
       veomd.sap_vrnt_cd,
       veomd.sap_vrnt_desc,
       veomd.sap_put_up_cd,
       veomd.sap_put_up_desc,
       veomd.sap_grp_frnchse_cd,
       veomd.sap_grp_frnchse_desc,
       veomd.sap_frnchse_cd,
       veomd.sap_frnchse_desc,
       veomd.sap_prod_frnchse_cd,
       veomd.sap_prod_frnchse_desc,
       veomd.sap_prod_mjr_cd,
       veomd.sap_prod_mjr_desc,
       veomd.sap_prod_mnr_cd,
       veomd.sap_prod_mnr_desc,
       veomd.sap_prod_hier_cd,
       veomd.sap_prod_hier_desc,
       veomd.gph_region as global_mat_region,
       veomd.gph_prod_frnchse as global_prod_franchise,
       veomd.gph_prod_brnd as global_prod_brand,
       veomd.gph_prod_vrnt as global_prod_variant,
       veomd.gph_prod_put_up_cd as global_prod_put_up_cd,
       veomd.gph_prod_put_up_desc as global_put_up_desc,
       veomd.gph_prod_sub_brnd as global_prod_sub_brand,
       veomd.gph_prod_needstate as global_prod_need_state,
       veomd.gph_prod_ctgry as global_prod_category,
       veomd.gph_prod_subctgry as global_prod_subcategory,
       veomd.gph_prod_sgmnt as global_prod_segment,
       veomd.gph_prod_subsgmnt as global_prod_subsegment,
       veomd.gph_prod_size as global_prod_size,
       veomd.gph_prod_size_uom as global_prod_size_uom,
       pos_qty,
       pos_gts,
       pos_item_prc,
       pos_tax,
       pos_nts,
       conv_factor,
       jj_qty_pc,
       jj_item_prc_per_pc,
       jj_gts,
       jj_vat_amt,
       jj_nts,
       0 as local_mat_listpriceunit,
       0 as si_sls_qty,
       0 as si_ret_qty,
       0 as si_sls_less_rtn_qty,
       0 as si_gts_val,
       0 as si_ret_val,
       0 as si_gts_less_rtn_val,
       0 as si_nts_qty,
       0 as si_nts_val,
       0 as si_tp_val
from (select b."year",
             b.qrtr,
             b.qrtr_no,
             b.mnth_no,
             a.cntry_cd,
             c.cntry_nm,
             a.jj_mnth_id,
             a.cust_cd,
             cust_brnch_cd,
             e.primary_soldto as sold_to,
             c.brnch_nm,
             a.item_cd,
             d.item_nm,
             d.sap_item_cd,
             pos_qty,
             pos_gts,
             pos_item_prc,
             pos_tax,
             pos_nts,
             conv_factor,
             jj_qty_pc,
             jj_item_prc_per_pc,
             jj_gts,
             jj_vat_amt,
             jj_nts
    from (select *
            from edw_vw_ph_pos_sales_fact
            where cntry_cd = 'PH') as a,
           (select distinct "year",
                   qrtr_no,
                   qrtr,
                   mnth_id,
                   mnth_desc,
                   mnth_no
            from edw_vw_os_time_dim) b,
           (select *
            from itg_mds_ph_ref_pos_primary_sold_to
            where active = 'Y') e,
           (select *
            from EDW_VW_ph_POS_CUSTOMER_DIM
            where cntry_cd = 'PH') c,
           (select *
            from EDW_VW_ph_POS_material_DIM
            where cntry_cd = 'PH') d
      where b.mnth_id = a.jj_mnth_id
      and   c.brnch_cd(+) = a.cust_brnch_cd
      and   c.cust_cd(+) = a.cust_cd
      and   e.cust_cd(+) = a.cust_cd
      and   ltrim(d.item_cd(+),'0') = ltrim(a.item_cd,'0')
      and   d.jj_mnth_id(+) = a.jj_mnth_id
      and   d.cust_cd(+) = a.cust_cd) veposf,
     (select *
      from edw_vw_ph_material_dim
      where cntry_key = 'PH') veomd,
     (select *
      from edw_vw_ph_customer_dim
      where sap_cntry_cd = 'PH') veocd,
     edw_mv_ph_customer_dim eocd
where upper(ltrim(veomd.sap_matl_num(+),0)) = trim(veposf.sap_item_cd)
and   upper(trim(eocd.cust_id(+))) = upper(trim(veposf.sold_to))
and   upper(ltrim(veocd.sap_cust_id(+),'0')) = upper(trim(veposf.sold_to))
union all
select veossf.jj_year as jj_year,
       veossf.jj_qtr as jj_qtr,
       veossf.jj_mnth_id as jj_mnth_id,
       veossf.jj_mnth_no as jj_mnth_no,
       veocd.sap_cntry_nm as cntry_nm,
       null as mt_cust_cd,
       null as mt_cust_brnch_cd,
       null as mt_cust_brnch_nm,
       null as mt_item_cd,
       null as mt_item_nm,
       veossf.cust_id as sold_to,
       veocd.sap_cust_nm as sold_to_nm,
       eocd.region,
       eocd.channel_cd as chnl_cd,
       eocd.channel_desc as chnl_desc,
       eocd.sub_chnl_cd as sub_chnl_cd,
       eocd.sub_chnl_desc as sub_chnl_desc,
       eocd.parent_cust_cd as parent_customer_cd,
       eocd.parent_cust_nm as parent_customer,
       eocd.rpt_grp_6_desc as account_grp,
       'GENERAL TRADE' as trade_type,
       eocd.rpt_grp_2_desc as sls_grp_desc,
       veocd.sap_state_cd,
       veocd.sap_sls_org,
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
       ltrim(veomd.sap_matl_num,'0') as sku,
       veomd.sap_mat_desc as sku_desc,
       veomd.sap_mat_type_cd,
       veomd.sap_mat_type_desc,
       veomd.sap_base_uom_cd,
       veomd.sap_prchse_uom_cd,
       veomd.sap_prod_sgmt_cd,
       veomd.sap_prod_sgmt_desc,
       veomd.sap_base_prod_cd,
       veomd.sap_base_prod_desc,
       veomd.sap_mega_brnd_cd,
       veomd.sap_mega_brnd_desc,
       veomd.sap_brnd_cd,
       veomd.sap_brnd_desc,
       veomd.sap_vrnt_cd,
       veomd.sap_vrnt_desc,
       veomd.sap_put_up_cd,
       veomd.sap_put_up_desc,
       veomd.sap_grp_frnchse_cd,
       veomd.sap_grp_frnchse_desc,
       veomd.sap_frnchse_cd,
       veomd.sap_frnchse_desc,
       veomd.sap_prod_frnchse_cd,
       veomd.sap_prod_frnchse_desc,
       veomd.sap_prod_mjr_cd,
       veomd.sap_prod_mjr_desc,
       veomd.sap_prod_mnr_cd,
       veomd.sap_prod_mnr_desc,
       veomd.sap_prod_hier_cd,
       veomd.sap_prod_hier_desc,
       veomd.gph_region as global_mat_region,
       veomd.gph_prod_frnchse as global_prod_franchise,
       veomd.gph_prod_brnd as global_prod_brand,
       veomd.gph_prod_vrnt as global_prod_variant,
       veomd.gph_prod_put_up_cd as global_prod_put_up_cd,
       veomd.gph_prod_put_up_desc as global_put_up_desc,
       veomd.gph_prod_sub_brnd as global_prod_sub_brand,
       veomd.gph_prod_needstate as global_prod_need_state,
       veomd.gph_prod_ctgry as global_prod_category,
       veomd.gph_prod_subctgry as global_prod_subcategory,
       veomd.gph_prod_sgmnt as global_prod_segment,
       veomd.gph_prod_subsgmnt as global_prod_subsegment,
       veomd.gph_prod_size as global_prod_size,
       veomd.gph_prod_size_uom as global_prod_size_uom,
       0 as pos_qty,
       0 as pos_gts,
       0 as pos_item_prc,
       0 as pos_tax,
       0 as pos_nts,
       0 as conv_factor,
       0 as jj_qty_pc,
       0 as jj_item_prc_per_pc,
       0 as jj_gts,
       0 as jj_vat_amt,
       0 as jj_nts,
       epp.lst_price_unit as local_mat_listpriceunit,
       veossf.sls_qty as si_sls_qty,
       veossf.ret_qty as si_ret_qty,
       veossf.sls_less_rtn_qty as si_sls_less_rtn_qty,
       veossf.gts_val as si_gts_val,
       veossf.ret_val as si_ret_val,
       veossf.gts_less_rtn_val as si_gts_less_rtn_val,
       nts_qty as si_nts_qty,
       veossf.nts_val as si_nts_val,
       veossf.tp_val as si_tp_val
from (select veotd.mnth_id as jj_mnth_id,
             veotd.mnth_no as jj_mnth_no,
             veotd."year" as jj_year,
             veotd.qrtr as jj_qtr,
             -- veotd.mnth_wk_no as jj_mnth_wk_no,
             eocd1.dstrbtr_grp_cd,
             ltrim(veossf1.cust_id,'0') as cust_id,
             coalesce(item_cd,'') as item_cd,
             sum(sls_qty) as sls_qty,
             sum(ret_qty) as ret_qty,
             sum(sls_less_rtn_qty) as sls_less_rtn_qty,
             sum(gts_val) as gts_val,
             sum(ret_val) as ret_val,
             sum(gts_less_rtn_val) as gts_less_rtn_val,
             sum(nts_qty) as nts_qty,
             sum(nts_val) as nts_val,
             sum(tp_val) as tp_val
      from edw_vw_ph_sellin_sales_fact  veossf1,
           (select distinct cust_id,
                   dstrbtr_grp_cd
            from edw_mv_ph_customer_dim
            where cust_id in (select distinct cust_id
                              from edw_mv_ph_customer_dim
                              where parent_cust_cd in (select distinct parent_cust_cd
                                                       from edw_mv_ph_customer_dim
                                                       where cust_id in (select primary_soldto from itg_mds_ph_ref_pos_primary_sold_to
                                                                          where active='Y')))) eocd1,
           (select "year",
                   qrtr_no,
                   qrtr,
                   mnth_id,
                   mnth_desc,
                   mnth_no,
                   mnth_shrt,
                   mnth_long /*      wk,
                   mnth_wk_no,
                   case
                     when mnth_wk_no = 1 then '19000101'
                     else frst_day
                   end as first_day,
                   case
                     when mnth_wk_no = lst_wk then '20991231'
                     else lst_day
                   end as last_day*/
            from (select distinct "year",
                         qrtr_no,
                         qrtr,
                         mnth_id,
                         mnth_desc,
                         mnth_no,
                         mnth_shrt,
                         mnth_long /*   wk,
                   mnth_wk_no,
                   min(cal_date_id) as frst_day,
                   max(cal_date_id) as lst_day,
                   max(mnth_wk_no) over (partition by mnth_id) as lst_wk*/
                  from edw_vw_os_time_dim t /* group by
                    "year",
                   qrtr_no,
                   qrtr,
                   mnth_id,
                   mnth_desc,
                   mnth_no,
                   mnth_shrt,
                   mnth_long,
                   wk,
                   mnth_wk_no*/) a) veotd
      where (sls_qty <> 0 or ret_qty <> 0 or gts_val <> 0 or ret_val <> 0 or nts_val <> 0 or tp_val <> 0)
      and   cntry_nm = 'PH'
      and   trim(veossf1.jj_mnth_id) = cast(trunc(veotd.mnth_id) as varchar)
      --  and   veossf1.pstng_dt between veotd.first_day and veotd.last_day
      and   upper(trim(eocd1.cust_id(+))) = upper(ltrim(veossf1.cust_id,'0'))
      group by veotd.mnth_id,
               veotd.mnth_no,
               veotd."year",
               veotd.qrtr,
               -- veotd.mnth_wk_no,
               eocd1.dstrbtr_grp_cd,
               veossf1.cust_id,
               coalesce(item_cd,'')) veossf,
     (select *
      from edw_mv_ph_customer_dim
      where cust_id in (select distinct cust_id
                        from edw_mv_ph_customer_dim
                        where parent_cust_cd in (select distinct parent_cust_cd
                                                 from edw_mv_ph_customer_dim
                                                 where cust_id in (select primary_soldto from itg_mds_ph_ref_pos_primary_sold_to
                                                                               where active='Y')))) eocd,

     (select * 
      from itg_mds_ph_pos_pricelist
      where active = 'Y') epp,
     (select *
      from edw_vw_ph_material_dim
      where cntry_key = 'PH') veomd,
     (select *
      from edw_vw_ph_customer_dim
      where sap_cntry_cd = 'PH') veocd
where upper(trim(eocd.cust_id)) = upper(ltrim(veossf.cust_id,'0'))
and   upper(ltrim(veomd.sap_matl_num(+),'0')) = upper(ltrim(veossf.item_cd,'0'))
and   upper(ltrim(veocd.sap_cust_id(+),'0')) = upper(ltrim(veossf.cust_id,'0'))
and   upper(trim(epp.item_cd(+))) = upper(trim(veossf.item_cd))
and   trim(epp.jj_mnth_id(+)) = trim(veossf.jj_mnth_id)
)
select * from final