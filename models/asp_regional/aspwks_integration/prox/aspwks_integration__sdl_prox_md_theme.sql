with wks_prox_md_theme as(
    select * from aspwks_integration.wks_prox_md_theme
),
final as(
    select 
	id 
	,themecode 
	,themename 
	,expensesubcategory 
	,expensetypecode 
	,createuserid 
	,status 
	,createtime
	,lastmodifytime
	,lastmodifyuserid  
	,applicationid,
       filename,
       run_id,
       crt_dttm 
from wks_prox_md_theme
)
select * from final

