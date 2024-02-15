with sdl_my_daily_sellout_stock_fact as
(
    select  * from {{ ref('mysitg_integration__sdl_my_daily_sellout_stock_fact') }}
),
itg_my_material_dim as (
    select  * from {{ ref('mysitg_integration__itg_my_material_dim') }}
),
immd as (
    (select item_bar_cd,
             item_cd,
             status
      from (select item_bar_cd,
                   item_cd,
                   status,
                   case
                     when status = 'ACTIVE' THEN 'A'||item_bar_cd
                     when status = 'INACTIVE' THEN 'B'||item_bar_cd
                     when status = 'DISCON' THEN 'C'||item_bar_cd
                   end as flag,
                   row_number() over (partition by item_bar_cd order by flag asc) as row_count
            from (select distinct item_bar_cd,
                         item_cd,
                         status
                  from (select item_bar_cd,
                               min(item_cd) over (partition by item_bar_cd,status) as item_cd, 
                               status
                        from itg_my_material_dim))) t3
      where t3.row_count = 1)
)
,
final as (
select distinct
source.cust_id::varchar(255) as cust_id,
CASE
        
        WHEN source.inv_dt LIKE  '%__-__-____%' THEN (TO_DATE(source.inv_dt, 'DD-MM-YYYY'))
        WHEN source.inv_dt LIKE  '%____-__-__%' THEN (TO_DATE(source.inv_dt, 'YYYY-MM-DD'))
        WHEN source.inv_dt LIKE  '%__/__/__' THEN (TO_DATE(source.inv_dt, 'DD/MM/YY'))
        WHEN source.inv_dt LIKE  '%__-__-__' THEN (TO_DATE(source.inv_dt, 'DD-MM-YY'))
         WHEN source.inv_dt LIKE  '_-__-____' THEN (TO_DATE(source.inv_dt, 'DD-MM-YYYY'))
         WHEN source.inv_dt LIKE  '%M%' THEN TO_DATE(REPLACE(source.INV_DT,'M','-'))
        else TO_DATE(source.inv_dt,'DD/MM/YYYY')
        END AS inv_dt,
source.dstrbtr_wh_id::varchar(255) as dstrbtr_wh_id,
source.item_cd::varchar(255) as item_cd,
source.dstrbtr_prod_cd::varchar(255) as dstrbtr_prod_cd,
source.ean_num::varchar(255) as ean_num,
source.dstrbtr_prod_desc::varchar(255) as dstrbtr_prod_desc,
source.qty::varchar(255) as qty,
source.uom::varchar(255) as uom,
source.qty_on_ord::varchar(255) as qty_on_ord,
source.uom_on_ord::varchar(255) as uom_on_ord,
source.qty_committed::varchar(255) as qty_committed,
source.uom_committed::varchar(255) as uom_committed,
source.available_qty_pc::varchar(255) as available_qty_pc,
source.qty_on_ord_pc::varchar(255) as qty_on_ord_pc,
source.qty_committed_pc::varchar(255) as qty_committed_pc,
source.unit_prc::varchar(255) as unit_prc,
source.total_val::varchar(255) as total_val,
source.custom_field1::varchar(255) as custom_field1,
immd.item_cd::varchar(255) as custom_field2,
source.filename::varchar(255) as filename,
current_timestamp::timestamp_ntz(9) as curr_dt,
source.cdl_dttm::varchar(255) as cdl_dttm
from sdl_my_daily_sellout_stock_fact as source
left join immd on ltrim(ean_num,'0')=ltrim(immd.item_bar_cd,'0')  
)

select * from final
