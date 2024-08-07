with sdl_PROX_MD_Theme as(
    select * from {{ ref('aspwks_integration__sdl_prox_md_theme') }}
),
final as(   
    SELECT 
	ID 
	,ThemeCode 
	,ThemeName 
	,ExpenseSubCategory 
	,ExpenseTypeCode 
	,CreateUserID 
	,Status 
	,CreateTime
	,LastModifyTime
	,LastModifyUserID  
	,ApplicationID ,
       filename,
       run_id,
       crt_dttm
FROM sdl_PROX_MD_Theme)
select * from final