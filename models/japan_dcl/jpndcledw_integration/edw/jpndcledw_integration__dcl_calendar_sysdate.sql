{{
    config(
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = "
            {% if is_incremental() %}
            update {{this}} set is_active = false;
            delete FROM {{this}} WHERE to_CHAR(curr_date,'YYYYMMDD' ) =TO_CHAR(convert_timezone('Asia/Tokyo', current_timestamp())::DATE,'YYYYMMDD') ;
            {% endif %}"
    )
}}

WITH final AS 
(
select
	convert_timezone('Asia/Tokyo',current_timestamp())::date AS CURR_DATE,
    TRUE::boolean as is_active,
	case
        when to_char(convert_timezone('Asia/Tokyo', current_timestamp()), 'DD') <= '20' 
		     then TO_CHAR(ADD_MONTHS(convert_timezone('Asia/Tokyo', current_timestamp()),-1), 'YYYYMM')
		else TO_CHAR(convert_timezone('Asia/Tokyo', current_timestamp()), 'YYYYMM')
	end::varchar(9) as target_month
)
Select * from final