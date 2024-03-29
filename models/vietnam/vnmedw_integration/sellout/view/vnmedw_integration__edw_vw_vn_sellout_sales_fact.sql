with itg_vn_distributor_sap_sold_to_mapping as
(
    select * from {{ ref('vnmitg_integration__itg_vn_distributor_sap_sold_to_mapping') }}
),
itg_vn_dms_d_sellout_sales_fact as 
(
    select * from {{ ref('vnmitg_integration__itg_vn_dms_d_sellout_sales_fact') }}
),
itg_vn_dms_product_dim  as
(
    select * from {{ ref('vnmitg_integration__itg_vn_dms_product_dim') }}
),
itg_vn_dms_distributor_dim as
(
    select * from {{ ref('vnmitg_integration__itg_vn_dms_distributor_dim') }}
),
distributor as 
(
        select dstrbtr_id,
            mapped_spk
        from (
                select dstrbtr_id,
                    mapped_spk,
                    row_number() over (
                        partition by dstrbtr_id
                        order by crtd_dttm desc
                    ) as rn
                from itg_vn_dms_distributor_dim
            )
        where rn = 1
),
final as 
(
select 
    sellout.cntry_code as cntry_cd,
    'Vietnam' as cntry_nm,
    sellout.dstrbtr_id as dstrbtr_grp_cd,
    distributor.mapped_spk,
    sst_map.sap_sold_to_code as soldto_code,
    sellout.outlet_id as cust_cd,
    sellout.material_code as dstrbtr_matl_num,
    product_dim.productcodesap as sap_matl_num,
    null as bar_cd,
    sellout.invoice_date as bill_date,
    sellout.invoice_no as bill_doc,
    sellout.salesrep_id as slsmn_cd,
    sellout.salesrep_name as slsmn_nm,
    null as doc_type,
    null as doc_type_desc,
    0 as base_sls,
    sellout.quantity,
    case
        when sellout.total_sellout_afvat_bfdisc >= 0::numeric::numeric(18, 0)::numeric(15, 4) then sellout.quantity
        else 0::numeric::numeric(18, 0)
    end as sls_qty,
    case
        when sellout.total_sellout_afvat_bfdisc < 0::numeric::numeric(18, 0)::numeric(15, 4) then sellout.quantity
        else 0::numeric::numeric(18, 0)
    end as ret_qty,
    sellout.uom,
    case
        when sellout.total_sellout_afvat_bfdisc >= 0::numeric::numeric(18, 0)::numeric(15, 4) then sellout.quantity
        else 0::numeric::numeric(18, 0)
    end as sls_qty_pc,
    case
        when sellout.total_sellout_afvat_bfdisc < 0::numeric::numeric(18, 0)::numeric(15, 4) then sellout.quantity
        else 0::numeric::numeric(18, 0)
    end as ret_qty_pc,
    case
        when sellout.total_sellout_afvat_bfdisc >= 0::numeric::numeric(18, 0)::numeric(15, 4) then sellout.total_sellout_afvat_bfdisc
        else 0::numeric::numeric(18, 0)
    end as grs_trd_sls,
    case
        when sellout.total_sellout_afvat_bfdisc < 0::numeric::numeric(18, 0)::numeric(15, 4) then sellout.total_sellout_afvat_bfdisc
        else 0::numeric::numeric(18, 0)
    end as ret_val,
    sellout.discount as trd_discnt,
    null::integer as trd_discnt_item_lvl,
    null::integer as trd_discnt_bill_lvl,
    null::integer as trd_sls,
    sellout.total_sellout_afvat_afdisc as net_trd_sls,
    null as cn_reason_cd,
    null as cn_reason_desc,
    case
        when sellout.total_sellout_afvat_bfdisc >= 0::numeric::numeric(18, 0)::numeric(15, 4) then sellout.total_sellout_afvat_bfdisc
        else 0::numeric::numeric(18, 0)
    end as jj_grs_trd_sls,
    case
        when sellout.total_sellout_afvat_bfdisc < 0::numeric::numeric(18, 0)::numeric(15, 4) then sellout.total_sellout_afvat_bfdisc
        else 0::numeric::numeric(18, 0)
    end as jj_ret_val,
    null::integer as jj_trd_sls,
    sellout.total_sellout_afvat_afdisc as jj_net_trd_sls,
    trim(
        product_dim.tax_rate::text,
        'VAT'::character varying::text
    )::integer as tax_rate,
    sellout.total_sellout_afvat_afdisc /(
        (
            100::numeric + trim(
                product_dim.tax_rate::text,
                'VAT'::character varying::text
            )::integer::numeric::numeric(18, 0)
        ) / 100::numeric::numeric(18, 0)
    ) as jj_net_sls
from itg_vn_dms_d_sellout_sales_fact sellout,
    itg_vn_dms_product_dim product_dim,
     distributor
    left join itg_vn_distributor_sap_sold_to_mapping sst_map on distributor.mapped_spk::text = sst_map.distributor_id::text
where sellout.material_code::text = product_dim.product_code::text
    and sellout.dstrbtr_id::text = distributor.dstrbtr_id::text
    and sellout.status::text <> 'V'::character varying::text
)

select * from final