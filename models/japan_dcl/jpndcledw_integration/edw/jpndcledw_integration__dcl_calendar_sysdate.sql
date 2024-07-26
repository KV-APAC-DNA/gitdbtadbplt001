{{
    config(
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = ["
            update {{this}} set is_active = false;",
            "delete FROM {{this}} WHERE to_CHAR(curr_date,'YYYYMMDD' ) =TO_CHAR(convert_timezone('Asia/Tokyo', sysdate())::DATE,'YYYYMMDD') ;
            "]
    )
}}

WITH final AS 
(
select
	convert_timezone('Asia/Tokyo',sysdate())::date AS CURR_DATE,
    TRUE::boolean as is_active,
	case
        when to_char(convert_timezone('Asia/Tokyo', sysdate()), 'DD') <= '20' 
		     then TO_CHAR(ADD_MONTHS(convert_timezone('Asia/Tokyo', sysdate()),-1), 'YYYYMM')
		else TO_CHAR(convert_timezone('Asia/Tokyo', sysdate()), 'YYYYMM')
	end::varchar(9) as target_month
)
Select * from final