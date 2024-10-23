with ph_pos_rka_rose_pharma as
(
select * from {{ ref('phlitg_integration__itg_ph_pos_rka_rose_pharma') }}
),
price_list as 
(
select * from {{ ref('phlitg_integration__itg_mds_ph_pos_pricelist') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
transformed as
(
select jj_mnth_id,
item_cd,
 Lst_Price_Unit
  from price_list
where  active='Y'
group by all

union  all
select 
distinct 
a.mnth_id,
b.item_cd,
b.Lst_Price_Unit
from (select distinct mnth_id from  edw_vw_os_time_dim where mnth_id <=(select max(jj_mnth_id) from ph_pos_rka_rose_pharma)) a
cross join 
(select  item_cd,max(Lst_Price_Unit) as Lst_Price_Unit,max(jj_mnth_id) as jj_mnth_id from price_list
where  active='Y'
group by all) b where  (a.mnth_id > b.jj_mnth_id)
),
final as
(
    select * from transformed
)

select * from final 



