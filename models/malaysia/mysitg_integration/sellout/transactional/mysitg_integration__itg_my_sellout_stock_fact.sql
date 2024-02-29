{{
    config(
        materialized="incremental",
        incremental_strategy = "delete+insert",
        unique_key=["cust_id","inv_dt"]

    )
}}

{% set cte_to_execute = 'my_joint_monthly' %}

{% if cte_to_execute == 'my_joint_monthly' %}

with source as (
    select * from {{ source('myssdl_raw', 'sdl_my_monthly_sellout_stock_fact') }}
),

imier as (
    select * from {{ ref('mysitg_integration__itg_my_ids_exchg_rate') }}
),

logical as (
    select
        source.cust_id as cust_id,
        to_date(inv_dt, 'DD.MM.YYYY') as inv_dt,
        dstrbtr_wh_id,
        item_cd,
        dstrbtr_prod_cd,
        ean_num,
        dstrbtr_prod_desc,
        cast(replace(qty, ',', '') as decimal(20, 4)) as qty,
        uom,
        cast(replace(qty_on_ord, ',', '') as decimal(20, 4)) as qty_on_ord,
        uom_on_ord as uom_on_ord,
        cast(replace(qty_committed, ',', '') as decimal(20, 4)) as qty_committed,
        uom_committed as uom_committed,
        cast(replace(available_qty_pc, ',', '') as decimal(20, 4)) as available_qty_pc,
        cast(replace(qty_on_ord_pc, ',', '') as decimal(20, 4)) as qty_on_ord_pc,
        cast(replace(qty_committed_pc, ',', '') as decimal(20, 4)) as qty_committed_pc,
        cast(unit_prc as decimal(20, 4)) as unit_prc,
        cast(total_val as decimal(20, 4)) * coalesce(imier.exchng_rate, 1) as total_val,
        custom_field1,
        custom_field2 as sap_matl_num,
        filename,
        source.cdl_dttm as cdl_dttm,
        source.curr_dt as curr_dt,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source, imier
    where imier.cust_id(+) = source.cust_id
        and imier.yearmo(+) = replace(substring(to_date(inv_dt, 'DD.MM.YYYY'), 0, 7), '-', '')

),

final as (
    select 
        cust_id::varchar(50) as cust_id,
        inv_dt::date as inv_dt,
        dstrbtr_wh_id::varchar(50) as dstrbtr_wh_id,
        item_cd::varchar(50) as item_cd,
        dstrbtr_prod_cd::varchar(50) as dstrbtr_prod_cd,
        ean_num::varchar(50) as ean_num,
        dstrbtr_prod_desc::varchar(100) as dstrbtr_prod_desc,
        qty::number(20,4) as qty,
        uom::varchar(20) as uom,
        qty_on_ord::number(20,4) as qty_on_ord,
        uom_on_ord::varchar(100) as uom_on_ord,
        qty_committed::number(20,4) as qty_committed,
        uom_committed::varchar(100) as uom_committed,
        available_qty_pc::number(20,4) as available_qty_pc,
        qty_on_ord_pc::number(20,4) as qty_on_ord_pc,
        qty_committed_pc::number(20,4) as qty_committed_pc,
        unit_prc::number(20,4) as unit_prc,
        total_val::number(20,4) as total_val,
        custom_field1::varchar(255) as custom_field1,
        sap_matl_num::varchar(255) as sap_matl_num,
        filename::varchar(255) as filename,
        cdl_dttm::varchar(255) as cdl_dttm,
        current_timestamp::timestamp_ntz(9) as crtd_dttm,
        current_timestamp::timestamp_ntz(9) as updt_dttm
    from logical
)

select * from final

{% elif cte_to_execute == 'my_sellout_inv' %}


with wks_my_sellout_stock_fact as
(
    select  * from {{ ref('myswks_integration__wks_my_sellout_stock_fact') }}
),
itg_my_ids_exchg_rate as
(
    select  * from {{ ref('mysitg_integration__itg_my_ids_exchg_rate') }}
),

itg_my_material_dim as (
    select  * from {{ ref('mysitg_integration__itg_my_material_dim') }}
),
itg_my_material_map as (
    select  * from {{ ref('mysitg_integration__itg_my_material_map') }}
),
d as 
(
   select 
        item_bar_cd,
        min(item_cd) over (partition by item_bar_cd,status) as item_cd, 
        status
    from itg_my_material_dim
),
b as
(
    select distinct 
        item_bar_cd,
        item_cd,
        status
    from d
),  
t3 as
( 
   select 
        item_bar_cd,
        item_cd,
        status,
        case
            when status = 'ACTIVE' THEN 'A'||item_bar_cd
            when status = 'INACTIVE' THEN 'B'||item_bar_cd
            when status = 'DISCON' THEN 'C'||item_bar_cd
        end as flag,
        row_number() over (partition by item_bar_cd order by flag asc) as row_count
    from b
),

immd as 
(
    select 
        item_bar_cd,
        item_cd,
        status
    from  t3
    where t3.row_count = 1
),
a as 
(
    select 
        immm.item_cd,
        immd.item_bar_cd,
        immm.ext_item_cd
      from itg_my_material_map immm,
           itg_my_material_dim immd
      where ltrim(immm.item_cd,'0') = ltrim(immd.item_cd,'0')
      qualify row_number() over (partition by immm.ext_item_cd order by null) = 1
),

temp as 
(
    select 
       t1.cust_id,
       inv_dt,
       dstrbtr_wh_id,
       item_cd,
       dstrbtr_prod_cd,
       ean_num,
       dstrbtr_prod_desc,
       cast(replace(qty,',','') as numeric(20,4)) as qty,
       uom,
       cast(replace(qty_on_ord,',','') as numeric(20,4)) as qty_on_ord,
       uom_on_ord as uom_on_ord,
       cast(replace(qty_committed,',','') as numeric(20,4)) as qty_committed,
       uom_committed as uom_committed,
       cast(replace(available_qty_pc,',','') as numeric(20,4)) as available_qty_pc,
       cast(replace(qty_on_ord_pc,',','') as numeric(20,4)) as qty_on_ord_pc,
       cast(replace(qty_committed_pc,',','') as numeric(20,4)) as qty_committed_pc,
       cast(unit_prc as numeric(20,4)) as unit_prc,
       cast(total_val as numeric(20,4))*coalesce(t2.exchng_rate,1) as total_val,
       custom_field1,
       custom_field2 ,
       filename,
       t1.cdl_dttm,
       t1.curr_dt as crtd_dttm,
       current_timestamp()::timestamp_ntz(9)  as updt_dttm
from wks_my_sellout_stock_fact t1,
     itg_my_ids_exchg_rate t2
where t2.cust_id(+) = t1.cust_id
and   t2.yearmo(+) = substring(replace(inv_dt,'-',''),0,7)
),
trans as 
(
  select  
  temp.cust_id,
  temp.inv_dt,
  temp.dstrbtr_wh_id,
  temp.item_cd,
  temp.dstrbtr_prod_cd,
  temp.ean_num,
  temp.dstrbtr_prod_desc,
  temp.qty,
  temp.uom,
  temp.qty_on_ord,
  temp.uom_on_ord,
  temp.qty_committed,
  temp.uom_committed,
  temp.available_qty_pc,
  temp.qty_on_ord_pc,
  temp.qty_committed_pc,
  temp.unit_prc,
  temp.total_val,
  temp.custom_field1,
  nvl(nvl(temp.custom_field2,immd.item_cd),a.item_cd) as sap_matl_num,
  temp.filename,
  temp.cdl_dttm,
  temp.crtd_dttm,
  temp.updt_dttm from temp 
  left join immd on
    ltrim(temp.ean_num,'0')= ltrim(immd.item_bar_cd,'0') 
  left join a on
    ltrim(temp.dstrbtr_prod_cd,'0') =ltrim(a.ext_item_cd,'0') 
),
result as 
(
    select distinct
        cust_id::varchar(50) as cust_id,
        inv_dt::date as inv_dt,
        dstrbtr_wh_id::varchar(50) as dstrbtr_wh_id,
        item_cd::varchar(50) as item_cd,
        dstrbtr_prod_cd::varchar(50) as dstrbtr_prod_cd,
        ean_num::varchar(50) as ean_num,
        dstrbtr_prod_desc::varchar(100) as dstrbtr_prod_desc,
        qty::number(20,4) as qty,
        uom::varchar(20) as uom,
        qty_on_ord::number(20,4) as qty_on_ord,
        uom_on_ord::varchar(100) as uom_on_ord,
        qty_committed::number(20,4) as qty_committed,
        uom_committed::varchar(100) as uom_committed,
        available_qty_pc::number(20,4) as available_qty_pc,
        qty_on_ord_pc::number(20,4) as qty_on_ord_pc,
        qty_committed_pc::number(20,4) as qty_committed_pc,
        unit_prc::number(20,4) as unit_prc,
        total_val::number(20,4) as total_val,
        custom_field1::varchar(255) as custom_field1,
        sap_matl_num::varchar(255) as sap_matl_num,
        filename::varchar(255) as filename,
        cdl_dttm::varchar(255) as cdl_dttm,
        current_timestamp::timestamp_ntz(9) as crtd_dttm,
        current_timestamp::timestamp_ntz(9) as updt_dttm
    from trans
),

final as 
(
    select
        cust_id,
        inv_dt,
        dstrbtr_wh_id,
        item_cd,
        dstrbtr_prod_cd,
        ean_num,
        dstrbtr_prod_desc,
        qty,
        uom,
        qty_on_ord,
        uom_on_ord,
        qty_committed,
        uom_committed,
        available_qty_pc,
        qty_on_ord_pc,
        qty_committed_pc,
        unit_prc,
        total_val,
        custom_field1,
        sap_matl_num,
        filename,
        cdl_dttm,
        crtd_dttm,
        updt_dttm
    from result

)

select * from final

{% endif %}
