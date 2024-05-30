{{
    config(
        materialized='incremental',
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_watson_sellout') }}
),
final as
(
    select
    Loc_Idnt, 
	Sale_Date, 
	Str_Code,  
	Str_Name,  
	Str_Class, 
	Str_Format,
	Division,	
	Div_Desc,	
	Dept_Idnt,
	Dept_Desc,
	Sup_Code,	
	Sup_Name,	
	Prdt_Code,
	Prdt_Desc,
	Brand,
	Sale_Qty,	
	Net_Sale,	
	week,	 
	year,		 
	Range_Desc,
	pos_cust,
	yearmonth,
    run_id,
    current_timestamp()::timestamp_ntz AS crtd_dttm,
    filename
    from source
)
select * from final