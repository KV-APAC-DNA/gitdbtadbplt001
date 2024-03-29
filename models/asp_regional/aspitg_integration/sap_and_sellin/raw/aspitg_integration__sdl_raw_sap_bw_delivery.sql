{{
    config(
        materialized="incremental",
        incremental_strategy="append",
       post_hook="{{sap_transaction_processed_files('BWA_CDL_DELIVERY','vw_stg_sdl_sap_bw_delivery','sdl_raw_sap_bw_delivery')}}"
    )}}

with source as(
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_delivery') }}
),
sap_transactional_processed_files as (
    select * from {{ source('aspwks_integration', 'sap_transactional_processed_files') }}
),
final as(
    select deliv_num as deliv_numb,
        deliv_item as deliv_item,
        incoterms as incoterms,
        incoterms2 as incoterms2,
        unload_pt as unload_pt,
        grs_wgt_dl as grs_wgt_dl,
        unit_of_wt as unit_of_wt,
        dlv_qty as dlv_qty,
        denomintr as denomintr,
        numerator as numerator,
        shp_pr_tmf as shp_pr_tmf,
        shp_pr_tmv as shp_pr_tmv,
        sls_unit as sales_unit,
        volume_unit as volumeunit,
        no_del_it as no_del_it,
        volume_dl as volume_dl,
        act_dl_qty as act_dl_qty,
        net_wgt_dl as net_wgt_dl,
        base_uom as base_uom,
        refer_doc as refer_doc,
        refer_item as refer_itm,
        fisc_varnt as fiscvarnt,
        sold_to as sold_to,
        cust_grp as cust_group,
        bill_block as bill_block,
        ship_to as ship_to,
        load_pt as load_pt,
        sls_org as salesorg,
        created_by as createdby,
        created_on as createdon,
        whse_num as whse_num,
        strge_bin as strge_bin,
        strge_type as strge_type,
        matl_grp_3 as matl_grp_3,
        matl_grp_4 as matl_grp_4,
        sls_off as sales_off,
        del_wa_dh as del_wa_dh,
        wbs_elemt as wbs_elemt,
        prvdoc_ctg as prvdoc_ctg,
        st_up_dt as st_up_dte,
        distr_chan as distr_chan,
        zactdldte as zactdldte,
        matl_grp_5 as matl_grp_5,
        division as division,
        itm_type as itm_type,
        sls_grp as sales_grp,
        stat_dt as stat_date,
        crea_time as crea_time,
        item_categ as item_categ,
        plant as plant,
        bwapplnm as bwapplnm,
        creditor as creditor,
        doc_categ as doc_categ,
        forwagent as forwagent,
        matl_grp as matl_group,
        sls_emply as salesemply,
        material as material,
        prod_hier as prod_hier,
        payer as payer,
        bill_to_prty as billtoprty,
        matl_grp_2 as matl_grp_2,
        matl_grp_1 as matl_grp_1,
        mat_entrd as mat_entrd,
        stor_loc as stor_loc,
        eanupc as eanupc,
        pick_indc as pick_indc,
        consu_flag as consu_flag,
        cust_grp3 as cust_grp3,
        bus_area as bus_area,
        cust_grp5 as cust_grp5,
        cust_grp4 as cust_grp4,
        cust_grp2 as cust_grp2,
        cust_grp1 as cust_grp1,
        bilblk_dl as bilblk_dl,
        batch as batch,
        route as route,
        ship_point as ship_point,
        del_block as del_block,
        del_type as del_type,
        ship_dt as ship_date,
        goodsmv_st as goodsmv_st,
        sales_dist as sales_dist,
        gi_dt as gi_date,
        act_gi_dt as act_gi_dte,
        comp_cd as comp_code,
        rt_promo as rt_promo,
        process_key as processkey,
        ch_on as ch_on,
        pick_conf as pick_conf,
        sts_pick as sts_pick,
        stor_no as storno,
        record_mode as recordmode,
        zdelqtybu as zdelqtybu,
        zdlqtycse as zdlqtycse,
        cdl_dttm as cdl_datetime,
        crt_dttm as curr_date,
        filename as file_name
    from source
    where not exists (
    select 
        act_file_name 
    from sap_transactional_processed_files 
    where target_table_name='sdl_raw_sap_bw_delivery' and sap_transactional_processed_files.act_file_name=source.filename
  )
)

select * from final