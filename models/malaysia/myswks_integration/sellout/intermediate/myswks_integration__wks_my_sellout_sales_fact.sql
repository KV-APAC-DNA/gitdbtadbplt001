with source as(
      select * from {{ ref('mysitg_integration__sdl_my_daily_sellout_sales_fact') }}
),
itg_my_material_dim as(
    select * from {{ ref('mysitg_integration__itg_my_material_dim') }}
),
immd as(
  select
    item_bar_cd,
    item_cd,
    status
  from (
    select
      item_bar_cd,
      item_cd,
      status,
      case
        when status = 'ACTIVE'
        then 'A' || item_bar_cd
        when STATUS = 'INACTIVE'
        then 'B' || item_bar_cd
        when STATUS = 'DISCON'
        then 'C' || item_bar_cd
      end as flag,
      row_number() over (partition by item_bar_cd order by flag asc) as row_count
    from (
      select distinct
        item_bar_cd,
        item_cd,
        status
      from (
        select
          item_bar_cd,
          min(item_cd) over (partition by item_bar_cd, status) as item_cd, 
          status
        from itg_my_material_dim
      )
    )
  ) as t3
  where
    t3.row_count = 1
),
final as(
    select 
        source.dstrbtr_id::varchar(255) as dstrbtr_id,
        source.sls_ord_num::varchar(255) as sls_ord_num,
        CASE 
            WHEN source.sls_ord_dt LIKE '%-%' THEN TO_CHAR(TO_DATE(source.sls_ord_dt, 'DD-MM-YYYY'), 'DD/MM/YYYY')::varchar(255)
            ELSE source.sls_ord_dt::varchar(255)
        END AS sls_ord_dt,
        source.type::varchar(255) as type,
        source.cust_cd::varchar(255) as cust_cd,
        source.dstrbtr_wh_id::varchar(255) as dstrbtr_wh_id,
        source.item_cd::varchar(255) as item_cd,
        source.dstrbtr_prod_cd::varchar(255) as dstrbtr_prod_cd,
        source.ean_num::varchar(255) as ean_num,
        source.dstrbtr_prod_desc::varchar(255) as dstrbtr_prod_desc,
        source.grs_prc::varchar(255) as grs_prc,
        source.qty::varchar(255) as qty,
        source.uom::varchar(255) as uom,
        source.qty_pc::varchar(255) as qty_pc,
        source.qty_aft_conv::varchar(255) as qty_aft_conv,
        source.subtotal_1::varchar(255) as subtotal_1,
        source.discount::varchar(255) as discount,
        source.subtotal_2::varchar(255) as subtotal_2,
        source.bottom_line_dscnt::varchar(255) as bottom_line_dscnt,
        source.total_amt_aft_tax::varchar(255) as total_amt_aft_tax,
        source.total_amt_bfr_tax::varchar(255) as total_amt_bfr_tax,
        source.sls_emp::varchar(255) as sls_emp,
        source.custom_field1::varchar(255) as custom_field1,
        source.custom_field2::varchar(255) as custom_field2,
        immd.item_cd::varchar(255) as custom_field3,
        source.filename::varchar(255) as filename,
        source.cdl_dttm::varchar(255) as cdl_dttm,
        source.curr_dt::TIMESTAMP_NTZ(9) as curr_dt,
    from source, immd where LTRIM(immd.item_bar_cd(+), '0') = LTRIM(ean_num, '0')
)
select * from final