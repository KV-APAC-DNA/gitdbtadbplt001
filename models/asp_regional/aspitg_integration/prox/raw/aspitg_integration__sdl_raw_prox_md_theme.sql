with sdl_PROX_MD_Theme as(
    select * from {{ source('aspsdl_raw', 'sdl_prox_md_theme') }}
    where filename not in (
        select distinct file_name from {{source('aspwks_integration','TRATBL_ssdl_prox_md_theme____null_test')}}
    )
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