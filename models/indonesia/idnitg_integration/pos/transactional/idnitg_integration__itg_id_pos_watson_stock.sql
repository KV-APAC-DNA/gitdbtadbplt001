{{
    config(
        materialized='incremental',
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_watson_stock') }}
),
final as
(
    select
    grp,          
	group_name,   
	dept,       	
	dept_name,  	
	item,      	
	item_name,    
	Product_Type, 
	Item_Brand,	
	Supplier,     
	Supplier_Name,
	Sup_code2,    
	product_plnmod,
	POG,			
	Top500,		
	SOH_Qty,		
	AVG_Sales,	
	Week_Cover,	
	STORE,		
	"values",
    pos_cust,
    yearmonth,
    run_id,
    current_timestamp()::timestamp_ntz AS crtd_dttm,
    filename
    from source
)
select * from final