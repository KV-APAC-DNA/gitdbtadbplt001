--Import CTE   
with edw_rpt_regional_sellout_offtake as (
    select * from {{ source('aspedw_integration', 'edw_rpt_regional_sellout_offtake') }}
),
edw_material_sales_dim as (
    select * from {{ ref( 'aspedw_integration__edw_material_sales_dim') }}
),


--Final CTE
MY_REGIONAL_SELLOUT_POS_SKU_EAN_BASE as (
select distinct mnth_id,sku_code, MATEAN.EAN		
from  edw_rpt_regional_sellout_offtake base		
      left join (select distinct ltrim(matl_num,'0') as matl_num,ltrim(ean_num,'0') as ean
					from edw_material_sales_dim where sls_org = '2100' and nullif(ean_num,'') is not null		
				)MATEAN ON LTRIM(BASE.SKU_CODE,'0') = LTRIM(MATEAN.MATL_NUM,'0')		
				WHERE COUNTRY_CODE = 'MY' and data_source = 'POS' ),
final as(
    select
mnth_id::VARCHAR(11) AS mnth_id,
sku_code::VARCHAR(11) AS sku_code,
ean::VARCHAR(100) as ean
from MY_REGIONAL_SELLOUT_POS_SKU_EAN_BASE
)				

select * from final