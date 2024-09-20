with item_z_h_hen_tbl
as (
	select *
	from {{ ref('jpndcledw_integration__item_z_h_hen_tbl') }}
	)

	,transformed
as (
	select item_z_h_hen_tbl.h_itemcode
		,item_z_h_hen_tbl.z_itemcode
		,item_z_h_hen_tbl.marker
	from item_z_h_hen_tbl
	)
    
select *
from transformed