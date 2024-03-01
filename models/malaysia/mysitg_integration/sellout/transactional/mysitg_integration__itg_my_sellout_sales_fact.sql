{{
    config(
        materialized="incremental",
        incremental_strategy = "delete+insert",
        unique_key=["dstrbtr_id","sls_ord_dt"]
    )
}}

{% if var("cte_to_execute") == 'my_joint_monthly' %}

with source as (
  select * from {{ source('myssdl_raw', 'sdl_my_monthly_sellout_sales_fact') }}
),
imier as (
  select * from {{ ref('mysitg_integration__itg_my_ids_exchg_rate') }}
),
union1 as (
    select
        dstrbtr_id,
        sls_ord_num,
        to_date(sls_ord_dt,'dd.mm.yyyy') as sls_ord_dt,
        type,
        cust_cd,
        dstrbtr_wh_id,
        item_cd,
        dstrbtr_prod_cd,
        ean_num,
        dstrbtr_prod_desc,
        cast(grs_prc as decimal(20, 4)) as grs_prc,
        cast(qty as decimal(20, 4)) as qty,
        uom,
        cast(qty_pc as decimal(20, 4)) as qty_pc,
        cast(qty_aft_conv as decimal(20, 4)) as qty_aft_conv,
        cast(subtotal_1 as decimal(20, 4)) * coalesce(imier.exchng_rate, 1) as subtotal_1,
        cast(discount as decimal(20, 4)) * coalesce(imier.exchng_rate, 1) as discount,
        cast(subtotal_2 as decimal(20, 4)) * coalesce(imier.exchng_rate, 1) as subtotal_2,
        cast(bottom_line_dscnt as decimal(20, 4)) * coalesce(imier.exchng_rate, 1) as bottom_line_dscnt,
        cast(total_amt_aft_tax as decimal(20, 4)) * coalesce(imier.exchng_rate, 1) as total_amt_aft_tax,
        cast(total_amt_bfr_tax as decimal(20, 4)) * coalesce(imier.exchng_rate, 1) as total_amt_bfr_tax,
        sls_emp,
        custom_field1,
        custom_field2,
        custom_field3 as sap_matl_num,
        filename,
        source.cdl_dttm as cdl_dttm,
        source.curr_dt as crtd_dttm,
        current_timestamp() as updt_dttm
    from source, imier
    where imier.cust_id(+)=source.dstrbtr_id
        and imier.yearmo(+)=replace(substring(to_date(sls_ord_dt,'dd.mm.yyyy'), 0, 7),'-', '')
)

select * from union1

{% elif var("cte_to_execute") == 'my_sellout_sales' %}

with source as (
  select * from {{ source('myssdl_raw', 'sdl_my_monthly_sellout_sales_fact') }}
),
imier as (
  select * from {{ ref('mysitg_integration__itg_my_ids_exchg_rate') }}
),
wks_my_sellout_sales_fact as(
  select * from {{ ref('myswks_integration__wks_my_sellout_sales_fact') }}
),
itg_my_material_dim as (
    select * from {{ ref('mysitg_integration__itg_my_material_dim') }}
),
itg_my_material_map as (
    select  * from {{ ref('mysitg_integration__itg_my_material_map') }}
),
union2 as (
   select
    dstrbtr_id,
    sls_ord_num,
    to_date(sls_ord_dt, 'DD/MM/YYYY') as sls_ord_dt,
    type,
    cust_cd,
    dstrbtr_wh_id,
    item_cd,
    dstrbtr_prod_cd,
    ean_num,
    dstrbtr_prod_desc,
    try_cast(replace(grs_prc,',','') as decimal(20, 4)) as grs_prc,
    try_cast(replace(qty,',','') as decimal(20, 4)) as qty,
    uom,
    try_cast(replace(qty_pc,',','') as decimal(20, 4)) as qty_pc,
    try_cast(replace(qty_aft_conv,',','') as decimal(20, 4)) as qty_aft_conv,
    try_cast(replace(subtotal_1,',','') as decimal(20, 4)) * coalesce(t2.exchng_rate, 1) as subtotal_1,
    try_cast(replace(discount,',','') as decimal(20, 4)) * coalesce(t2.exchng_rate, 1) as discount,
    try_cast(replace(subtotal_2,',','') as decimal(20, 4)) * coalesce(t2.exchng_rate, 1) as subtotal_2,
    try_cast(replace(bottom_line_dscnt,',','') as decimal(20, 4)) * coalesce(t2.exchng_rate, 1) as bottom_line_dscnt,
    try_cast(replace(total_amt_aft_tax,',','') as decimal(20, 4)) * coalesce(t2.exchng_rate, 1) as total_amt_aft_tax,
    try_cast(replace(total_amt_bfr_tax,',','') as decimal(20, 4)) * coalesce(t2.exchng_rate, 1) as total_amt_bfr_tax,
    sls_emp,
    custom_field1,
    custom_field2,
    custom_field3 as sap_matl_num,
    filename,
    t1.cdl_dttm as cdl_dttm,
    t1.curr_dt as crtd_dttm,
    current_timestamp() as updt_dttm
    from wks_my_sellout_sales_fact as t1, imier as t2
    where
    t2.cust_id(+) = t1.dstrbtr_id
    and t2.yearmo(+) = substring(replace(to_date(sls_ord_dt, 'DD/MM/YYYY'), '-', ''), 0, 7)
),
immd as (
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
        when status = 'INACTIVE'
        then 'B' || item_bar_cd
        when status = 'DISCON'
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
          min(item_cd) over (partition by item_bar_cd, status) as item_cd, /* casting on item_cd */
          status
        from itg_my_material_dim
      )
    )
  ) as t3
  where
    t3.row_count = 1
),
a as (
    select immm.item_cd,
             immd.item_bar_cd,
             immm.ext_item_cd
      from itg_my_material_map immm,
           itg_my_material_dim immd
      where ltrim(immm.item_cd,'0') = ltrim(immd.item_cd,'0')
      qualify row_number() over (partition by immm.ext_item_cd order by null) = 1
      
),
final as (
    select  
    union2.dstrbtr_id::varchar(50) as dstrbtr_id,
    union2.sls_ord_num::varchar(50) as sls_ord_num,
    union2.sls_ord_dt::date as sls_ord_dt,
    union2.type::varchar(20) as type,
    union2.cust_cd::varchar(50) as cust_cd,
    union2.dstrbtr_wh_id::varchar(50) as dstrbtr_wh_id,
    union2.item_cd::varchar(50) as item_cd,
    union2.dstrbtr_prod_cd::varchar(50) as dstrbtr_prod_cd,
    union2.ean_num::varchar(50) as ean_num,
    union2.dstrbtr_prod_desc::varchar(100) as dstrbtr_prod_desc,
    union2.grs_prc::number(20,4) as grs_prc,
    union2.qty::number(20,4) as qty,
    union2.uom::varchar(20) as uom,
    union2.qty_pc::number(20,4) as qty_pc,
    union2.qty_aft_conv::number(20,4) as qty_aft_conv,
    union2.subtotal_1::number(20,4) as subtotal_1,
    union2.discount::number(20,4) as discount,
    union2.subtotal_2::number(20,4) as subtotal_2,
    union2.bottom_line_dscnt::number(20,4) as bottom_line_dscnt,
    union2.total_amt_aft_tax::number(20,4) as total_amt_aft_tax,
    union2.total_amt_bfr_tax::number(20,4) as total_amt_bfr_tax,
    union2.sls_emp::varchar(100) as sls_emp,
    union2.custom_field1::varchar(255) as custom_field1,
    union2.custom_field2::varchar(255) as custom_field2,
    nvl(nvl(union2.SAP_MATL_NUM, immd.item_cd), a.item_cd)::varchar(255) as sap_matl_num,
    union2.filename::varchar(255) as filename,
    union2.cdl_dttm::varchar(255) as cdl_dttm,
    union2.crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    union2.updt_dttm::timestamp_ntz(9) as updt_dttm
    from union2,immd,a 
    where ltrim(immd.item_bar_cd(+),'0') = ltrim(ean_num,'0')
    and ltrim(a.ext_item_cd(+),'0') = ltrim(dstrbtr_prod_cd,'0') 
)
select * from final
{% endif %}

