with source as(
    select * from {{ ref('vnmitg_integration__itg_vn_dksh_otc_inventory') }}
),
final as (
select
business_unit	as	"business_unit"
recording_date	as	"recording_date"
stock_type	as	"stock_type"
product_code	as	"product_code"
product_name	as	"product_name"
batch	as	"batch"
expiry_date	as	"expiry_date"
base_uom	as	"base_uom"
shelf_life	as	"shelf_life"
cogs	as	"cogs"
region	as	"region"
quantity	as	"quantity"
value	as	"value"
plant_code	as	"plant_code"
plant_name	as	"plant_name"
syslot	as	"syslot"
crt_dttm	as	"crt_dttm"
upd_dttm	as	"upd_dttm"
from source)

select * from final