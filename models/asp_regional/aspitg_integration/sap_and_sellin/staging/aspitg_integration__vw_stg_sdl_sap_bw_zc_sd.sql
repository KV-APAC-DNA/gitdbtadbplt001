with source as(
    select * from {{ source('bwa_access', 'bwa_zc_sd') }}
),
itg_crncy_mult as (
    select * from {{ source('aspitg_integration', 'itg_crncy_mult') }}
),
final as(
    select
        bic_zactdldte as zactdldte,
        act_gi_dte as act_gi_dte,
        billtoprty as billtoprty,
        bill_date as bill_date,
        bill_type as bill_type,
        bill_num as bill_num,
        comp_code as comp_code,
        customer as customer,
        bic_zdelcrdat as zdelcrdat,
        distr_chan as distr_chan,
        division as division,
        createdon as createdon,
        doc_date as doc_date,
        gi_date as gi_date,
        material as material,
        matav_date as matav_date,
        ord_reason as ord_reason,
        bic_zzabstk as zzabstk,
        bic_zcmgst as zcmgst,
        payer as payer,
        plant as plant,
        bic_zzp_itm as zzp_itm,
        bic_zzp_num as zzp_num,
        bic_zsd_pod as zsd_pod,
        bic_zrcodekey as zrcodekey,
        reason_rej as reason_rej,
        gt_cmfre as gt_cmfre,
        bic_zreq_date as zreq_date,
        route as route,
        doc_number as doc_number,
        doc_categ as doc_categ,
        s_ord_item as s_ord_item,
        doc_type as doc_type,
        salesemply as salesemply,
        salesorg as salesorg,
        item_categ as item_categ,
        ship_to as ship_to,
        sold_to as sold_to,
        bic_zblqtycse as zblqtycse,
        iff(right(bill_qty,1)='-',concat('-',replace(bill_qty,'-','')),bill_qty) as bill_qty,
        iff(right(bic_zbilqdift,1)='-',concat('-',replace(bic_zbilqdift,'-','')),bic_zbilqdift) as zbilqdift,
        iff(right(bic_zbilqotif,1)='-',concat('-',replace(bic_zbilqotif,'-','')),bic_zbilqotif) as zbilqotif,
        iff(right(inv_qty,1)='-',concat('-',replace(inv_qty,'-','')),inv_qty) as inv_qty,
        iff(right(bic_zcfmqdift,1)='-',concat('-',replace(bic_zcfmqdift,'-','')),bic_zcfmqdift) as zcfmqdift,
        iff(right(cml_cd_qty,1)='-',concat('-',replace(cml_cd_qty,'-','')),cml_cd_qty) as cml_cd_qty,
        iff(right(bic_zdlqtycse,1)='-',concat('-',replace(bic_zdlqtycse,'-','')),bic_zdlqtycse) as zdlqtycse,
        iff(right(bic_zdelqtybu,1)='-',concat('-',replace(bic_zdelqtybu,'-','')),bic_zdelqtybu) as zdelqtybu,
        iff(right(dlv_qty,1)='-',concat('-',replace(dlv_qty,'-','')),dlv_qty) as dlv_qty,
        case when curr.currkey is null 
        then iff(right(subtotal_6,1)='-',concat('-',replace(subtotal_6,'-','')),subtotal_6) 
        else iff(right(subtotal_6,1)='-',concat('-',replace(subtotal_6,'-','')),subtotal_6) * pow(10,(2-currdec)) 
        end as subtotal_6,
        case when curr.currkey is null 
        then iff(right(bic_zfs_ntsb,1)='-',concat('-',replace(bic_zfs_ntsb,'-','')),bic_zfs_ntsb) 
        else iff(right(bic_zfs_ntsb,1)='-',concat('-',replace(bic_zfs_ntsb,'-','')),bic_zfs_ntsb) * pow(10,(2-currdec)) 
        end as zfs_ntsb,
        case when curr.currkey is null 
        then iff(right(bic_zfs_ninv,1)='-',concat('-',replace(bic_zfs_ninv,'-','')),bic_zfs_ninv) 
        else iff(right(bic_zfs_ninv,1)='-',concat('-',replace(bic_zfs_ninv,'-','')),bic_zfs_ninv) * pow(10,(2-currdec)) 
        end as zfs_ninv,
        iff(right(bic_zfs_qtybu,1)='-',concat('-',replace(bic_zfs_qtybu,'-','')),bic_zfs_qtybu) as zfs_qtybu,
        case when curr.currkey is null 
        then iff(right(subtotal_1,1)='-',concat('-',replace(subtotal_1,'-','')),subtotal_1) 
        else iff(right(subtotal_1,1)='-',concat('-',replace(subtotal_1,'-','')),subtotal_1) * pow(10,(2-currdec)) 
        end as subtotal_1,
        case when curr.currkey is null 
        then iff(right(subtotal_5,1)='-',concat('-',replace(subtotal_5,'-','')),subtotal_5) 
        else iff(right(subtotal_5,1)='-',concat('-',replace(subtotal_5,'-','')),subtotal_5) * pow(10,(2-currdec)) 
        end as subtotal_5,
        case when curr.currkey is null 
        then iff(right(net_price,1)='-',concat('-',replace(net_price,'-','')),net_price) 
        else iff(right(net_price,1)='-',concat('-',replace(net_price,'-','')),net_price) * pow(10,(2-currdec)) 
        end as net_price,
        case when curr.currkey is null 
        then iff(right(netval_inv,1)='-',concat('-',replace(netval_inv,'-','')),netval_inv) 
        else iff(right(netval_inv,1)='-',concat('-',replace(netval_inv,'-','')),netval_inv) * pow(10,(2-currdec)) 
        end as netval_inv,
        case when curr.currkey is null 
        then iff(right(net_value,1)='-',concat('-',replace(net_value,'-','')),net_value) 
        else iff(right(net_value,1)='-',concat('-',replace(net_value,'-','')),net_value) * pow(10,(2-currdec)) 
        end as net_value,
        iff(right(bic_zblqtycse,1)='-',concat('-',replace(bic_zblqtycse,'-','')),bic_zblqtycse) as zorqtycse,
        iff(right(bic_zordqtybu,1)='-',concat('-',replace(bic_zordqtybu,'-','')),bic_zordqtybu) as zordqtybu,
        iff(right(bic_zordqty,1)='-',concat('-',replace(bic_zordqty,'-','')),bic_zordqty) as zordqty,
        iff(right(cml_or_qty,1)='-',concat('-',replace(cml_or_qty,'-','')),cml_or_qty) as cml_or_qty,
        iff(right(bic_zktranlt,1)='-',concat('-',replace(bic_zktranlt,'-','')),bic_zktranlt) as zktranlt,
        iff(right(bic_zunsuppc,1)='-',concat('-',replace(bic_zunsuppc,'-','')),bic_zunsuppc) as zunsuppc,
        case when curr.currkey is null 
        then iff(right(bic_zunsupva,1)='-',concat('-',replace(bic_zunsupva,'-','')),bic_zunsupva) 
        else iff(right(bic_zunsupva,1)='-',concat('-',replace(bic_zunsupva,'-','')),bic_zunsupva) * pow(10,(2-currdec)) 
        end as zunsupva,
        iff(right(volume_dl,1)='-',concat('-',replace(volume_dl,'-','')),volume_dl) as volume_dl,
        iff(right(volume_ap,1)='-',concat('-',replace(volume_ap,'-','')),volume_ap) as volume_ap,
        calday as calday,
        base_uom as base_uom,
        currency as currency,
        doc_currcy as doc_currcy,
        sales_unit as sales_unit,
        fiscper as fiscper,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    left join itg_crncy_mult curr
		on source.currency = curr.currkey
)
select * from final