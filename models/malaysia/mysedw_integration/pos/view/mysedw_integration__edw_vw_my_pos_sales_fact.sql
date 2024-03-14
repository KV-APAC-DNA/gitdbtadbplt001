with itg_my_pos_sales_fact as
(
    select * from {{ ref('mysitg_integration__itg_my_pos_sales_fact') }}
),
edw_vw_my_listprice as
(
    select * from {{ ref('mysedw_integration__edw_vw_my_listprice') }}
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
        (cast((  edw_vw_my_listprice.mnth_type) as text) = cast((  cast('JJ' as varchar)) as text)
          )
        )
),

vpos as
(
      select
        itg_my_pos_sales_fact.cust_id,
        itg_my_pos_sales_fact.cust_nm,
        itg_my_pos_sales_fact.store_cd,
        itg_my_pos_sales_fact.store_nm,
        itg_my_pos_sales_fact.dept_cd,
        itg_my_pos_sales_fact.dept_nm,
        itg_my_pos_sales_fact.mt_item_cd,
        itg_my_pos_sales_fact.mt_item_desc,
        itg_my_pos_sales_fact.jj_mnth_id,
        itg_my_pos_sales_fact.jj_yr_week_no,
        itg_my_pos_sales_fact.qty,
        itg_my_pos_sales_fact.so_val,
        itg_my_pos_sales_fact.sap_matl_num,
        itg_my_pos_sales_fact.file_nm,
        cast(NULL as varchar) as cdl_dttm,
        itg_my_pos_sales_fact.crtd_dttm,
        itg_my_pos_sales_fact.updt_dttm
      from itg_my_pos_sales_fact
),

final as(
    select
        'MY' as cntry_cd,
        'Malaysia' as cntry_nm,
        NULL as pos_dt,
        vpos.jj_yr_week_no,
        vpos.jj_mnth_id,
        vpos.cust_id as cust_cd,
        vpos.mt_item_cd as item_cd,
        NULL as item_desc,
        vpos.sap_matl_num,
        NULL as bar_cd,
        NULL as master_code,
        vpos.store_cd as cust_brnch_cd,
        vpos.qty as pos_qty,
        vpos.so_val as pos_gts,
        case
        when (coalesce(vpos.qty, cast((  cast(( 0  ) as decimal)) as decimal(18, 0))) <> cast((  cast(( 0  ) as decimal)) as decimal(18, 0)))
        then (  vpos.so_val / vpos.qty)
        else cast((  cast(NULL as decimal)) as decimal(18, 0))
        end as pos_item_prc,
        NULL as pos_tax,
        vpos.so_val as pos_nts,
        1 as conv_factor,
        vpos.qty as jj_qty_pc,
        lp.rate as jj_item_prc_per_pc,
        coalesce((  vpos.qty * lp.rate), vpos.so_val) as jj_gts,
        NULL as jj_vat_amt,
        coalesce((  vpos.qty * lp.rate), vpos.so_val) as jj_nts,
        vpos.dept_cd
    from vpos
    left join listprice lp
      on (
        
          (
            ltrim(cast((  lp.item_cd) as text), cast((  cast('0' as varchar)) as text))
             = ltrim(cast((  vpos.sap_matl_num) as text), cast((  cast('0' as varchar)) as text))
          )
          and (
            cast((  lp.yearmo) as text) = cast((  vpos.jj_mnth_id) as text)
          )
        
      )
)

select * from final