with itg_holiday_list as (
    select * from {{ ref('hcposeitg_integration__itg_holiday_list') }}
),  

result as ( 
    select country,
        holiday_key 
    from itg_holiday_list
),

final as (
    select 
        country::varchar(5) as country,
	    holiday_key::varchar(20) as holiday_key
    from result
)

select * from final