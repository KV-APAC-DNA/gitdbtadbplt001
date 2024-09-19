with sdl_prox_md_material as(
    select * from {{ source('aspsdl_raw', 'sdl_prox_md_material') }}
),
final as(
    select 
	id 
	,division 
	,segment 
	,materialcode 
	,materialname
	,materialtype 
	,materialnamelocal
	,businesssegmentcode 
	,representativeitem
	,businesssegmentname
	,marketsegmentcode 
	,marketsegmentname
	,profitcenter
	,brandname 
	,brandcode 
	,subbrandcode 
	,subbrandname 
	,packsizecode 
	,packsizedesc
	,packsizegroupcode 
	,packsizegroupdesc
	,plant 
	,type 
	,bundle
	,baseunit 
	,salesunit 
	,listprice
	,standardprice
	,issensitive 
	,flavorcode 
	,flavorname
	,isnew 
	,status 
	,financeyear
	,comment 
	,applicationid 
	,version 
	,lastmodifyuserid 
	,lastmodifytime
	,packgecode 
	,packgename
	,marketsegmentdesc
	,barcode
	,representativecode
	,representativename
	,representativenameen
	,skuattr
	,categorycode 
	,categoryname 
	,materialstatus 
	,tdubarcode 
	,mcubarcode 
	,rsubarcode 
	,producttype 
	,tradedunitconfiguration ,
       filename,
       run_id,
       crt_dttm
from sdl_prox_md_material
)
select * from final


