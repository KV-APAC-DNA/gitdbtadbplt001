with itg_th_dstrbtr_customer_dim as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_TH_DSTRBTR_CUSTOMER_DIM
),
itg_th_sellout_sales_foc_fact as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_TH_SELLOUT_SALES_FOC_FACT
),
itg_th_dstrbtr_material_dim as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_TH_DSTRBTR_material_DIM
),
final as (
SELECT 'TH' AS cntry_cd,
    'Thailand' AS cntry_nm,
    s.dstrbtr_id as dstrbtr_grp_cd,
    null as dstrbtr_soldto_code,
    (
        ltrim((s.ar_cd)::text, ((0)::character varying)::text)
    )::character varying as cust_cd,
    (
        ltrim(
            (s.prod_cd)::text,
            ((0)::character varying)::text
        )
    )::character varying as dstrbtr_matl_num,
    s.product_name1,
    s.product_name2,
    null as sap_matl_num,
    null as bar_cd,
    s.order_dt as bill_date,
    s.order_no,
    s.iscancel,
    s.grp_cd,
    s.prom_cd1,
    s.prom_cd2,
    s.prom_cd3,
    (
        ltrim(
            (s.order_no)::text,
            ((0)::character varying)::text
        )
    )::character varying as bill_doc,
    c.sls_emp as slsmn_cd,
    c.sls_nm as slsmn_nm,
    null as wh_id,
    null as doc_type,
    null as doc_type_desc,
    0 as base_sls,
    case
        when (
            s.qty >= (((0)::numeric)::numeric(18, 0))::numeric(19, 6)
        ) then s.qty
        else ((0)::numeric)::numeric(18, 0)
    end as sls_qty,
    case
        when (
            s.qty < (((0)::numeric)::numeric(18, 0))::numeric(19, 6)
        ) then s.qty
        else ((0)::numeric)::numeric(18, 0)
    end as ret_qty,
    'DZ' as uom,
    case
        when (
            s.qty >= (((0)::numeric)::numeric(18, 0))::numeric(19, 6)
        ) then (s.qty * ((12)::numeric)::numeric(18, 0))
        else ((0)::numeric)::numeric(18, 0)
    end as sls_qty_pc,
    case
        when (
            s.qty < (((0)::numeric)::numeric(18, 0))::numeric(19, 6)
        ) then (s.qty * ((12)::numeric)::numeric(18, 0))
        else ((0)::numeric)::numeric(18, 0)
    end as ret_qty_pc,
    case
        when (
            s.qty >= (((0)::numeric)::numeric(18, 0))::numeric(19, 6)
        ) then (s.qty * m.sls_prc_credit)
        else ((0)::numeric)::numeric(18, 0)
    end as grs_trd_sls,
    case
        when (
            s.qty < (((0)::numeric)::numeric(18, 0))::numeric(19, 6)
        ) then (s.qty * m.sls_prc_credit)
        else ((0)::numeric)::numeric(18, 0)
    end as ret_val,
    (s.discnt + s.discnt_bt_ln) as trd_discnt,
    s.discnt as trd_discnt_item_lvl,
    s.grs_prc,
    s.discnt_bt_ln as trd_discnt_bill_lvl,
    null::integer as trd_sls,
    s.total_bfr_vat as net_trd_sls,
    s.subamt1 as tot_bf_discount,
    s.cn_reason_cd,
    s.cn_reason_en_desc as cn_reason_desc,
    case
        when (
            s.qty >= (((0)::numeric)::numeric(18, 0))::numeric(19, 6)
        ) then (s.qty * m.sls_prc_credit)
        else ((0)::numeric)::numeric(18, 0)
    end as jj_grs_trd_sls,
    case
        when (
            s.qty < (((0)::numeric)::numeric(18, 0))::numeric(19, 6)
        ) then (s.qty * m.sls_prc_credit)
        else ((0)::numeric)::numeric(18, 0)
    end as jj_ret_val,
    null::integer as jj_trd_sls,
    s.total_bfr_vat as jj_net_trd_sls,
    s.subamt1 as gross_sales
from (
        (
            itg_th_sellout_sales_foc_fact s
            left join itg_th_dstrbtr_material_dim m on (
                (
                    ltrim(
                        (s.prod_cd)::text,
                        ((0)::character varying)::text
                    ) = ltrim(
                        (m.item_cd)::text,
                        ((0)::character varying)::text
                    )
                )
            )
        )
        left join (
            select itg_th_dstrbtr_customer_dim.old_cust_id,
                itg_th_dstrbtr_customer_dim.dstrbtr_id,
                itg_th_dstrbtr_customer_dim.sls_emp,
                itg_th_dstrbtr_customer_dim.sls_nm,
                row_number() over(
                    partition by itg_th_dstrbtr_customer_dim.old_cust_id,
                    itg_th_dstrbtr_customer_dim.dstrbtr_id
                    order by itg_th_dstrbtr_customer_dim.old_cust_id,
                        itg_th_dstrbtr_customer_dim.dstrbtr_id
                ) as rn
            from itg_th_dstrbtr_customer_dim
        ) c on (
            (
                (
                    (c.rn = (1)::bigint)
                    and (
                        trim((c.old_cust_id)::text) = trim((s.ar_cd)::text)
                    )
                )
                and (
                    trim((c.dstrbtr_id)::text) = trim((s.dstrbtr_id)::text)
                )
            )
        )
    )
)
select * from final
