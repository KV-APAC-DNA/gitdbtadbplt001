with source as(
    select * from {{ source('bwa_access', 'bwa_cdl_billing_cond') }}
),
final as(
    select
        coalesce(bill_num, '') as bill_num,
        coalesce(bill_item, '') as bill_item,
        coalesce(bic_zstepnum, '') as zstepnum,
        coalesce(kncounter, '') as kncounter,
        coalesce(doc_number, '') as doc_number,
        coalesce(s_ord_item, '') as s_ord_item,
        coalesce(knart, '') as knart,
        coalesce(ch_on, '') as ch_on,
        coalesce(comp_code, '') as comp_code,
        coalesce(sales_dist, '') as sales_dist,
        coalesce(bill_type, '') as bill_type,
        coalesce(bill_date, '') as bill_date,
        coalesce(bill_cat, '') as bill_cat,
        coalesce(loc_currcy, '') as loc_currcy,
        coalesce(cust_group, '') as cust_group,
        coalesce(sold_to, '') as sold_to,
        coalesce(payer, '') as payer,
        coalesce(exrate_acc, '') as exrate_acc,
        coalesce(stat_curr, '') as stat_curr,
        coalesce(doc_categ, '') as doc_categ,
        coalesce(salesorg, '') as salesorg,
        coalesce(distr_chan, '') as distr_chan,
        coalesce(doc_currcy, '') as doc_currcy,
        coalesce(createdon, '') as createdon,
        coalesce(co_area, '') as co_area,
        coalesce(costcenter, '') as costcenter,
        coalesce(trans_date, '') as trans_date,
        coalesce(exchg_rate, '') as exchg_rate,
        coalesce(cust_grp1, '') as cust_grp1,
        coalesce(cust_grp2, '') as cust_grp2,
        coalesce(cust_grp3, '') as cust_grp3,
        coalesce(cust_grp4, '') as cust_grp4,
        coalesce(cust_grp5, '') as cust_grp5,
        coalesce(matl_group, '') as matl_group,
        coalesce(material, '') as material,
        coalesce(mat_entrd, '') as mat_entrd,
        coalesce(matl_grp_1, '') as matl_grp_1,
        coalesce(matl_grp_2, '') as matl_grp_2,
        coalesce(matl_grp_3, '') as matl_grp_3,
        coalesce(matl_grp_4, '') as matl_grp_4,
        coalesce(matl_grp_5, '') as matl_grp_5,
        coalesce(billtoprty, '') as billtoprty,
        coalesce(ship_to, '') as ship_to,
        coalesce(itm_type, '') as itm_type,
        coalesce(prod_hier, '') as prod_hier,
        coalesce(prov_group, '') as prov_group,
        coalesce(price_date, '') as price_date,
        coalesce(item_categ, '') as item_categ,
        coalesce(div_head, '') as div_head,
        coalesce(division, '') as division,
        coalesce(stat_date, '') as stat_date,
        coalesce(refer_doc, '') as refer_doc,
        coalesce(refer_itm, '') as refer_itm,
        coalesce(sales_off, '') as sales_off,
        coalesce(sales_grp, '') as sales_grp,
        coalesce(wbs_elemt, '') as wbs_elemt,
        coalesce(calday, '') as calday,
        coalesce(calmonth, '') as calmonth,
        coalesce(calweek, '') as calweek,
        coalesce(fiscper, '') as fiscper,
        coalesce(fiscvarnt, '') as fiscvarnt,
        coalesce(knclass, '') as knclass,
        coalesce(knorigin, '') as knorigin,
        coalesce(kntyp, '') as kntyp,
        knval,
        kprice,
        coalesce(kinak, '') as kinak,
        coalesce(kstat, '') as kstat,
        coalesce(storno, '') as storno,
        coalesce(rt_promo, '') as rt_promo,
        coalesce(rebate_grp, '') as rebate_grp,
        coalesce(bwapplnm, '') as bwapplnm,
        coalesce(processkey, '') as processkey,
        coalesce(eanupc, '') as eanupc,
        coalesce(createdby, '') as createdby,
        coalesce(serv_date, '') as serv_date,
        inv_qty,
        coalesce(forwagent, '') as forwagent,
        coalesce(salesemply, '') as salesemply,
        coalesce(sales_unit, '') as sales_unit,
        coalesce(kappl, '') as kappl,
        coalesce(acrn_id, '') as acrn_id,
        coalesce(recordmode, '') as recordmode,
        trim(split(file_name,'/')[array_size(split(file_name,'/'))-1],'"') source_file_name,
        NULL as file_name,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where _deleted_='F'
)

select * from final
