with wks_so_inv_133986 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_133986') }}
),
wks_so_inv_133980 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_133980') }}
),
wks_so_inv_133981 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_133981') }}
),
wks_so_inv_133982 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_133982') }}
),
wks_so_inv_133983 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_133983') }}
),
wks_so_inv_133984 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_133984') }}
),
wks_so_inv_133985 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_133985') }}
),
wks_so_inv_131164 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_131164') }}
),
wks_so_inv_131165 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_131165') }}
),
wks_so_inv_131166 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_131166') }}
),
wks_so_inv_131167 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_131167') }}
),
wks_so_inv_130516 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_130516') }}
),
wks_so_inv_130517 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_130517') }}
),
wks_so_inv_130518 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_130518') }}
),
wks_so_inv_130519 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_130519') }}
),
wks_so_inv_130520 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_130520') }}
),
wks_so_inv_130521 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_130521') }}
),
wks_so_inv_119024 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_119024') }}
),
wks_so_inv_119025 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_119025') }}
),
wks_so_inv_108129 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_108129') }}
),
wks_so_inv_108273 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_108273') }}
),
wks_so_inv_108565 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_108565') }}
),
wks_so_inv_118477 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_118477') }}
),
wks_so_inv_109772 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_109772') }}
),
wks_so_inv_135976 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_135976') }}
),
wks_so_inv_137021 as
(
    select  * from {{ ref('myswks_integration__wks_so_inv_137021') }}
),

union_all as (
    select * from wks_so_inv_133986
	union all
	select * from wks_so_inv_133985
	union all
	select * from wks_so_inv_133984
	union all
	select * from wks_so_inv_133983
	union all
	select * from wks_so_inv_133982
	union all
	select * from wks_so_inv_133981
	union all
	select * from wks_so_inv_133980
	union all
	select * from wks_so_inv_131167
	union all
	select * from wks_so_inv_131166
	union all
	select * from wks_so_inv_131165
	union all
	select * from wks_so_inv_131164
	union all
	select * from wks_so_inv_130521
	union all
	select * from wks_so_inv_130520
	union all
	select * from wks_so_inv_130519
	union all
	select * from wks_so_inv_130518
	union all
	select * from wks_so_inv_130517
	union all
	select * from wks_so_inv_130516
	union all
	select * from wks_so_inv_119025
	union all
	select * from wks_so_inv_119024
	union all
	select * from wks_so_inv_118477
	union all
	select * from wks_so_inv_109772
	union all
	select * from wks_so_inv_108565
	union all
	select * from wks_so_inv_108273
	union all
	select * from wks_so_inv_108129
    	union all
	select * from wks_so_inv_135976
	union all
	select * from wks_so_inv_137021
),

final as 
(
select
    distributor_id::varchar(255) as cust_id,
    date::varchar(255) as inv_dt,
    distributor_wh_id::varchar(255) as dstrbtr_wh_id,
    sap_material_id::varchar(255) as item_cd,
    product_code::varchar(255) as dstrbtr_prod_cd,
    product_ean_code::varchar(255) as ean_num,
    product_description::varchar(255) as dstrbtr_prod_desc,
    quantity_available::varchar(255) as qty,
    uom_available::varchar(255) as uom,
    quantity_on_order::varchar(255) as qty_on_ord,
    uom_on_order::varchar(255) as uom_on_ord,
    quantity_committed::varchar(255) as qty_committed,
    uom_committed::varchar(255) as uom_committed,
    quantity_available_in_pieces::varchar(255) as available_qty_pc,
    quantity_on_order_in_pieces::varchar(255) as qty_on_ord_pc,
    quantity_committed_in_pieces::varchar(255) as qty_committed_pc,
    unit_price::varchar(255) as unit_prc,
    total_value_available::varchar(255) as total_val,
    custom_field_1::varchar(255) as custom_field1,
    custom_field_2::varchar(255) as custom_field2,
    file_name::varchar(255) as filename,
    current_timestamp()::timestamp_ntz(9) as curr_dt,
    null as cdl_dttm  
from union_all
)

select * from final