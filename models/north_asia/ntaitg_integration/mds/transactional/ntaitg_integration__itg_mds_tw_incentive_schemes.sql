with source as
(
    select * from {{ source('ntasdl_raw', 'sdl_mds_tw_incentive_schemes') }}
),
final as 
(   
    select 
    type_code::varchar(100) as incentive_type,
	begin::number(28,4) as begin,
	end::number(28,4) as end,
	nts_si::number(28,4) as nts_si,
	offtake_si::number(28,4) as offtake_si,
	tp_si::number(28,4) as tp_si,
	ciw_si::number(28,4) as ciw_si,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final