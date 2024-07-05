with source as 
(
    select * from {{ ref('inditg_integration__itg_salesman_target') }}
),
final as 
(
    SELECT 
    ctry_cd::varchar(2) as ctry_cd,
	crncy_cd::varchar(3) as crncy_cd,
	fisc_year::number(18,0) as fisc_year,
	fisc_mnth::varchar(2) as fisc_mnth,
	month_nm::varchar(20) as month_nm,
	dist_code::varchar(50) as dist_code,
	sm_code::varchar(50) as sm_code,
	channel::varchar(100) as channel,
	brand_focus::varchar(50) as brand_focus,
	measure_type::varchar(50) as measure_type,
	sm_tgt_amt::number(38,6) as sm_tgt_amt,
	current_timestamp()::timestamp_ntz(9) as crt_dttm
       
FROM source
)

select * from final