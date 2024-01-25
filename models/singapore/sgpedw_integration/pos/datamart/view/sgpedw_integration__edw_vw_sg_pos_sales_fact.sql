with edw_vw_sg_listprice as(
    select * from {{ ref('sgpedw_integration__edw_vw_sg_listprice') }}
),
itg_sg_pos_sales_fact as(    
    select * from {{ source('sgpitg_integration', 'itg_sg_pos_sales_fact') }}
),

edw_vw_sg_listprice_t1 as
(
    select
        yearmo,
        item_cd,
        item_desc,
        rate,
        min(cast((yearmo) as text)) over (partition by item_cd  order by null) as min_period,
        max(cast((yearmo) as text)) over (partition by item_cd order by null) as max_period
    from edw_vw_sg_listprice
    where
    cast((edw_vw_sg_listprice.mnth_type) as text) = cast((cast('JJ' as varchar)) as text)
),

edw_vw_sg_listprice_t2 as
(
    select
        yearmo,
        item_cd,
        rate
    from edw_vw_sg_listprice
    where
    cast((mnth_type) as text) = cast((cast('JJ' as varchar)) as text)
),
combined_t1_t2_b as
(
    select
        t1.yearmo,
        t1.item_cd,
        t1.item_desc,
        t1.rate,
        t1.min_period,
        t2.rate as min_period_rate,
        t1.max_period,
        t3.rate as max_period_rate
    from
    edw_vw_sg_listprice_t1 as t1, 
    edw_vw_sg_listprice_t2 as t2,
    edw_vw_sg_listprice_t2 as t3
    where
    (cast(( t1.item_cd) as text) = cast((t2.item_cd) as text))
    and (cast((t1.item_cd) as text) = cast((t3.item_cd) as text))
    and (t1.min_period = cast((t2.yearmo) as text))
    and (t1.max_period = cast((t3.yearmo) as text))
),
combined_t1_t2_c as
(
    select distinct
        t1.item_cd,
        t1.min_period,
        t2.rate as min_period_rate,
        t1.max_period,
        t3.rate as max_period_rate
    from 
    edw_vw_sg_listprice_t1 as t1, 
    edw_vw_sg_listprice_t2 as t2,
    edw_vw_sg_listprice_t2 as t3
    where
            (
                (
                    (
                        (cast(( t1.item_cd) as text) = cast((t2.item_cd) as text))
                        and (cast((t1.item_cd) as text) = cast((t3.item_cd) as text))
                    )
                and (t1.min_period = cast((t2.yearmo) as text))
                )
                and (t1.max_period = cast((t3.yearmo) as text))
            )
),
final as
(
    select
        'SG' as cntry_cd,
        'SINGAPORE' as cntry_nm,
        null as pos_dt,
        ispsf.week as JJ_yr_week_no,
        ispsf.mnth_id as JJ_mnth_id,
        ispsf.cust_id as cust_cd,
        ispsf.item_code as item_cd,
        ispsf.item_desc,
        ispsf.sap_code as sap_matl_num,
        ispsf.product_barcode as bar_cd,
        ispsf.master_code,
        ispsf.store as cust_brnch_cd,
        ispsf.sales_qty as pos_qty,
        ispsf.net_sales as pos_gts,
        case
            when 
            (coalesce(ispsf.sales_qty, cast((cast((0) as decimal)) as decimal(18, 0))) 
                <> cast((cast((0) as decimal)) as decimal(18, 0))
            )
            then (ispsf.net_sales / ispsf.sales_qty)
        else cast((cast(null as decimal)) as decimal(18, 0))
        end as pos_item_prc,
        null as pos_tax,
        ispsf.net_sales as pos_nts,
        1 as conv_factor,
        ispsf.sales_qty as JJ_qty_pc,
        coalesce
            (b.rate,case
                        when (cast((ispsf.mnth_id) as text) < c.min_period)
                        then c.min_period_rate
                        else c.max_period_rate
                    end
            ) as JJ_item_prc_per_pc,
        coalesce
            (   (ispsf.sales_qty * 
                    coalesce
                        (
                            b.rate,
                            case
                            when (cast((ispsf.mnth_id) as text) < c.min_period)
                            then c.min_period_rate
                            else c.max_period_rate
                            end
                        )
                ),
                ispsf.net_sales
            ) as JJ_gts,
        null as JJ_vat_amt,
        coalesce
            (   (ispsf.sales_qty * 
                    coalesce
                        (
                            b.rate,
                            case
                            when (cast((ispsf.mnth_id) as text) < c.min_period)
                            then c.min_period_rate
                            else c.max_period_rate
                            end
                        )
                ),
                ispsf.net_sales
            ) as JJ_nts,
        null as dept_cd
    from 
    itg_sg_pos_sales_fact as ispsf
    left join combined_t1_t2_b as b
    on cast((ispsf.mnth_id) as text) = cast((b.yearmo) as text)
    and cast((ispsf.sap_code) as text) = cast((b.item_cd) as text)   
    left join combined_t1_t2_c as c
    on cast((ispsf.sap_code) as text) = cast((c.item_cd) as text)
)
select * from final