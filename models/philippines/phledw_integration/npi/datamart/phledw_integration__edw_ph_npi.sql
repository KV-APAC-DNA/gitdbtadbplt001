with edw_vw_ph_material_dim as (
select * from {{ ref('phledw_integration__edw_vw_ph_material_dim') }}
),
edw_vw_ph_npi as (
select * from {{ ref('phledw_integration__edw_vw_ph_npi') }}
),
veomd as (
SELECT *

      FROM edw_vw_ph_material_dim

      WHERE CNTRY_KEY = 'PH'),
transformed  as (

select a.*,veomd.GPH_PROD_BRND as brand from edw_vw_ph_npi a

left outer join  VEOMD

      on UPPER(LTRIM(VEOMD.SAP_MATL_NUM,0)) = UPPER(LTRIM(a.sku,0))

),
final as (
select 
subsource_type::varchar(8) as subsource_type,
pipeline::varchar(2) as pipeline,
jj_mnth_id::varchar(100) as jj_mnth_id,
sku::varchar(50) as sku,
sku_desc::varchar(100) as sku_desc,
chnl_desc::varchar(255) as chnl_desc,
sub_chnl_desc::varchar(255) as sub_chnl_desc,
sls_grp_desc::varchar(255) as sls_grp_desc,
parent_customer_cd::varchar(50) as parent_customer_cd,
parent_customer::varchar(255) as parent_customer,
sap_sls_office_desc::varchar(40) as sap_sls_office_desc,
sold_to::varchar(10) as sold_to,
sold_to_nm::varchar(100) as sold_to_nm,
dstrbtr_cust_cd::varchar(1) as dstrbtr_cust_cd,
account_name::varchar(255) as account_name,
sls_grping::varchar(255) as sls_grping,
trade_type::varchar(255) as trade_type,
peg_itemcode::varchar(1) as peg_itemcode,
peg_itemdesc::varchar(1) as peg_itemdesc,
sales_cycle::varchar(20) as sales_cycle,
nts_val::number(38,7) as nts_val,
brand::varchar(30) as brand
from transformed
)
select * from final