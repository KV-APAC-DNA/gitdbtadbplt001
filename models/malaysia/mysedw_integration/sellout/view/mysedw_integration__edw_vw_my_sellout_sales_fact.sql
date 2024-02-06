
with itg_my_sellout_sales_fact as
(
    select * from {{ ref('mysitg_integration__itg_my_sellout_sales_fact') }}
),
itg_my_customer_dim as
(
    select * from {{ ref('mysitg_integration__itg_my_customer_dim') }}
),
itg_my_dstrbtr_doc_type as
(
    select * from {{ ref('mysitg_integration__itg_my_dstrbtr_doc_type') }}
),
edw_vw_my_listprice as (
    select * from {{ref('mysedw_integration__edw_vw_my_listprice')}}
),
listprice as
(
    select
        edw_vw_my_listprice.cntry_key,
        edw_vw_my_listprice.cntry_nm,
        edw_vw_my_listprice.plant,
        edw_vw_my_listprice.cnty,
        edw_vw_my_listprice.item_cd,
        edw_vw_my_listprice.item_desc,
        edw_vw_my_listprice.valid_from,
        edw_vw_my_listprice.valid_to,
        edw_vw_my_listprice.rate,
        edw_vw_my_listprice.currency,
        edw_vw_my_listprice.price_unit,
        edw_vw_my_listprice.uom,
        edw_vw_my_listprice.yearmo,
        edw_vw_my_listprice.mnth_type,
        edw_vw_my_listprice.snapshot_dt
    from edw_vw_my_listprice
    where
      (
         (cast((  edw_vw_my_listprice.mnth_type) as text) = cast((  cast('CAL' as varchar)) as text)
        )
      )
),
final as
(
    SELECT
        'MY' as cntry_cd,
        'Malaysia' as cntry_nm,
        imcd.dstrbtr_grp_cd,
        imsddf.dstrbtr_id as dstrbtr_soldto_code,
        imsddf.cust_cd,
        imsddf.dstrbtr_prod_cd as dstrbtr_matl_num,
        imsddf.sap_matl_num,
        imsddf.ean_num as bar_cd,
        imsddf.sls_ord_dt as bill_date,
        imsddf.sls_ord_num as bill_doc,
        imsddf.sls_emp as slsmn_cd,
        null as slsmn_nm,
        imsddf.dstrbtr_wh_id as wh_id,
        imsddf.type as doc_type,
        coalesce
        (
        imddt.doc_type_desc,
        CASE
            when (imsddf.subtotal_1 > cast((  cast((cast((0) as decimal)  ) as decimal(18, 0))) as decimal(20, 4)))
            then cast('Invoice / DN' as varchar)
            when (imsddf.subtotal_1 < cast((cast((cast((0) as decimal)) as decimal(18, 0))) as decimal(20, 4)))
            then cast('Trade returns / CN' as varchar)
            else case
            when (imsddf.qty_pc >= cast((cast((cast((0) as decimal)) as decimal(18, 0))) as decimal(20, 4)))
            then cast('Invoice / DN' as varchar)
            else cast('Trade returns / CN' as varchar)
            end
        end
        ) as doc_type_desc,
        imsddf.subtotal_1 as base_sls,
        case
        when (
            coalesce(
            case
            when (imsddf.subtotal_1 = cast((cast((cast((0) as decimal)) as decimal(18, 0))) as decimal(20, 4)))
            then cast((cast(null as decimal)) as decimal(18, 0)) 
            else imsddf.subtotal_1 end,imsddf.qty) >= cast((cast((0) as decimal)) as decimal(18, 0))
        )
        then imsddf.qty 
        else cast((cast((0) as decimal)) as decimal(18, 0))
        end as sls_qty,
        case
        when (
        coalesce(
            case
            when (imsddf.subtotal_1 = cast((cast((cast((    0) as decimal)) as decimal(18, 0))) as decimal(20, 4)))
            then cast((cast(null as decimal)) as decimal(18, 0))
            else imsddf.subtotal_1 end,imsddf.qty) 
            < cast((cast((0) as decimal)) as decimal(18, 0)))
            then ((-cast((cast((1) as decimal)) as decimal(18, 0))) * imsddf.qty)
            else cast((cast((0) as decimal)) as decimal(18, 0))
            end as ret_qty,
        imsddf.uom,
        case
        when (
        coalesce(
        case
        when (imsddf.subtotal_1 = cast((cast((cast((0) as decimal)) as decimal(18, 0))) as decimal(20, 4)))then cast((
        cast(null as decimal)
        ) as decimal(18, 0))
        else imsddf.subtotal_1 end, imsddf.qty_pc ) >= cast(( cast(( 0 ) as decimal) ) as decimal(18, 0)) )
        then imsddf.qty_pc
        else cast((cast((0) as decimal)) as decimal(18, 0))
        end as sls_qty_pc,
        case
            when (coalesce(case when (imsddf.subtotal_1 = cast((cast((cast((0) as decimal)) as decimal(18, 0))) as decimal(20, 4)))
            then cast((cast(null as decimal)) as decimal(18, 0))
            else imsddf.subtotal_1
            end,imsddf.qty_pc) < cast((cast((0) as decimal)) as decimal(18, 0)))
            then ((-cast((cast((1) as decimal)) as decimal(18, 0))) * imsddf.qty_pc)
        else cast((cast((0) as decimal)) as decimal(18, 0))
        end as ret_qty_pc,
        case
            when (imsddf.subtotal_1 >= cast((cast((cast((0) as decimal)) as decimal(18, 0))) as decimal(20, 4)))
            then imsddf.subtotal_1
            else cast((cast((0) as decimal)) as decimal(18, 0))
        end as grs_trd_sls,
        case
            when (imsddf.subtotal_1 < cast((cast((cast((0) as decimal)) as decimal(18, 0))) as decimal(20, 4)))
            then ((-cast((cast((1) as decimal)) as decimal(18, 0))) * imsddf.subtotal_1)
            else cast((cast((0) as decimal)) as decimal(18, 0))
        end as ret_val,
        (
        imsddf.discount + imsddf.bottom_line_dscnt
        ) as trd_discnt,
        0 as trd_discnt_item_lvl,
        0 as trd_discnt_bill_lvl,
        imsddf.subtotal_2 as trd_sls,
        imsddf.total_amt_bfr_tax as net_trd_sls,
        null as cn_reason_cd,
        null as cn_reason_desc,
        case
            when (coalesce(case when (imsddf.subtotal_1 = cast((cast((cast((0) as decimal)) as decimal(18, 0))) as decimal(20, 4)))
            then cast((cast(null as decimal)) as decimal(18, 0))
            else imsddf.subtotal_1 end, imsddf.qty_pc ) >= cast(( cast(( 0 ) as decimal)) as decimal(18, 0)))
            then (imsddf.qty_pc * lp.rate)
            else cast((cast((0) as decimal)) as decimal(18, 0))
        end as jj_grs_trd_sls,
        case 
            when ( coalesce( case when ( imsddf.subtotal_1 = cast(( cast(( cast(( 0 ) as decimal) ) as decimal(18, 0)) ) as decimal(20, 4)) )
            then cast((cast(null as decimal)) as decimal(18, 0))
            else imsddf.subtotal_1 
            end,imsddf.qty_pc) < cast((cast((0) as decimal)) as decimal(18, 0)))
            then (((-cast((cast((1) as decimal)) as decimal(18, 0))) * imsddf.qty_pc) * lp.rate)
            else cast((cast((0) as decimal)) as decimal(18, 0))
        end as jj_ret_val,
        (
            imsddf.qty_pc * lp.rate
        ) as jj_trd_sls,
        (
            imsddf.qty_pc * lp.rate
        ) as jj_net_trd_sls,
        null as sls_rep_type,
        null as store,
        null as re_nm
    from (
            (
                (
                    itg_my_sellout_sales_fact as imsddf
                    left join itg_my_customer_dim as imcd
                        on (
                        (ltrim(cast((  imcd.cust_id) as text), cast((  cast('0' as varchar)) as text)) = 
                        ltrim(cast((  imsddf.dstrbtr_id) as text), cast((  cast('0' as varchar)) as text))
                        )
                        )
                )
                left join itg_my_dstrbtr_doc_type as imddt
                    on (
                        (
                            (
                                (
                                    ltrim(cast((  imddt.cust_id) as text), cast((  cast('0' as varchar)) as text))
                                    =  ltrim(cast((  imsddf.dstrbtr_id) as text), cast((  cast('0' as varchar)) as text))
                                )   
                            and (cast((  imddt.wh_id) as text) = cast((  imsddf.dstrbtr_wh_id) as text)    )  
                            )  
                        and (cast((  imddt.doc_type) as text) = cast((  imsddf.type) as text)  )
                        )
                    )
                )
            left join listprice lp
                on (
                (
                    (ltrim(cast((  lp.item_cd) as text), cast((  cast('0' as varchar)) as text)) 
                    = ltrim(cast((  imsddf.sap_matl_num) as text), cast((  cast('0' as varchar)) as text))
                    )
                    and (cast((  lp.yearmo) as text)
                    = to_char(  cast(cast((    imsddf.sls_ord_dt  ) as timestampntz) as timestampntz),  cast((    cast('YYYYMM' as varchar)  ) as text))
                    )
                )
            )
    )
)
select * from final
