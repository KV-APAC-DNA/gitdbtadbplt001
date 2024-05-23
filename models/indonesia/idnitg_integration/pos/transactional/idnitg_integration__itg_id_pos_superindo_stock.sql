{{
    config(
        materialized='incremental',
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_superindo_stock') }}
),
final as
(
    select
    COMPANY,          	
	CODE,            			
	DESCRIPTION,       		
	tag1, 	  				
	CNV,     				
	EQ,     					
	Stock_DC_Regular_Qty,    
	Stock_DC_Regular_DSI,       
	Stock_all_stores_QTY,     
	Stock_all_stores_DSI,        
	STOK_ALL,       	 	 	
	Day_SALES,       	  		
	DSI,
    pos_cust,
    run_id,
    filename,
    yearmonth,
    current_timestamp()::timestamp_ntz AS crtd_dttm
    from source
)
select * from final