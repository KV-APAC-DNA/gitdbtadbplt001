with

source as (

    select * from {{ source('bwa_access', 'bwa_cdl_billing') }}

),


final as (

    select
        coalesce(bill_num, '') as bill_num,
        coalesce(bill_item, '') as bill_item,
        bill_date as bill_dt,
        coalesce(bill_type, '') as bill_type,
        coalesce(sold_to, '') as sold_to,
        rt_promo,
        coalesce(s_ord_item, '') as s_ord_item,
        coalesce(doc_number, '') as doc_num,
        grs_wgt_dl,
        inv_qty as inv_qty,
        bill_qty,
        coalesce(base_uom, '') as base_uom,
        exchg_rate,
        req_qty,
        coalesce(sales_unit, '') as sls_unit,
        coalesce(payer, '') as payer,
        rebate_bas,
        no_inv_it,
        subtotal_1,
        subtotal_3,
        subtotal_4,
        subtotal_2,
        netval_inv,
        exchg_stat,
        bic_zblqtycse as zblqtycse,
        exratexacc,
        subtotal_6,
        gross_val,
        coalesce(unit_of_wt, '') as unit_of_wt,
        subtotal_5,
        numerator,
        cost,
        coalesce(plant, '') as plant,
        volume_dl,
        coalesce(loc_currcy, '') as loc_currcy,
        denomintr,
        coalesce(volumeunit, '') as volume_unit,
        scale_qty,
        cshdsc_bas,
        net_wgt_dl,
        tax_amount as tax_amt,
        rate_type,
        coalesce(salesorg, '') as sls_org,
        exrate_acc,
        coalesce(distr_chan, '') as distr_chnl,
        coalesce(doc_currcy, '') as doc_currcy,
        coalesce(co_area, '') as co_area,
        coalesce(doc_categ, '') as doc_categ,
        coalesce(fiscvarnt, '') as fisc_varnt,
        costcenter as cost_center,
        coalesce(matl_group, '') as matl_group,
        coalesce(division, '') as division,
        coalesce(material, '') as material,
        coalesce(sales_grp, '') as sls_grp,
        coalesce(div_head, '') as div_head,
        coalesce(ship_point, '') as ship_point,
        wbs_elemt,
        bill_rule,
        coalesce(bwapplnm, '') as bwapplnm,
        coalesce(processkey, '') as process_key,
        coalesce(cust_group, '') as cust_grp,
        coalesce(sales_off, '') as sls_off,
        coalesce(refer_itm, '') as refer_itm,
        coalesce(matl_grp_3, '') as matl_grp_3,
        price_date as price_dt,
        coalesce(salesemply, '') as sls_emply,
        coalesce(refer_doc, '') as refer_doc,
        st_up_dte,
        coalesce(stat_date, '') as stat_date,
        coalesce(item_categ, '') as item_categ,
        prov_group as prov_grp,
        coalesce(matl_grp_5, '') as matl_grp_5,
        coalesce(prod_hier, '') as prod_hier,
        itm_type,
        coalesce(matl_grp_4, '') as matl_grp_4,
        coalesce(ship_to, '') as ship_to,
        coalesce(billtoprty, '') as bill_to_prty,
        rebate_grp,
        coalesce(matl_grp_2, '') as matl_grp_2,
        coalesce(matl_grp_1, '') as matl_grp_1,
        coalesce(eanupc, '') as eanupc,
        coalesce(mat_entrd, '') as mat_entrd,
        batch,
        coalesce(stor_loc, '') as stor_loc,
        createdon as created_on,
        serv_date,
        cust_grp5,
        salesdeal as sls_deal,
        coalesce(bill_cat, '') as bill_cat,
        coalesce(cust_grp1, '') as cust_grp1,
        cust_grp3,
        trans_date as trans_dt,
        cust_grp4,
        coalesce(cust_grp2, '') as cust_grp2,
        coalesce(stat_curr, '') as stat_curr,
        coalesce(ch_on, '') as ch_on,
        coalesce(comp_code, '') as comp_cd,
        coalesce(sales_dist, '') as sls_dist,
        storno as stor_no,
        coalesce(recordmode, '') as record_mode,
        coalesce(customer, '') as customer,
        coalesce(cust_sales, '') as cust_sls,
        oi_ebeln as oi_ebeln,
        coalesce(oi_ebelp, '') as oi_ebelp,
        coalesce(bic_zsd_pod, '') as zsd_pod,
        _ingestiontimestamp_ as cdl_dttm,
        trim(split(file_name,'/')[array_size(split(file_name,'/'))-1],'"') as file_name,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where _deleted_='F'

)

select * from final
