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
),
final as (
    select * from sdl_my_daily_sellout_stock_fact
    left join immd on ltrim(immd.item_bar_cd(+),'0') = ltrim(ean_num,'0')
)

select * from final
