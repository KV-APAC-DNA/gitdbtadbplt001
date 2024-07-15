with source as (
    select * from dev_dna_core.snapjpnedw_integration.mt_constant_seq
),

result as (
    select
        identify_cd::varchar(40) as identify_cd,
	    max_value::number(10,0) as max_value,
	    current_timestamp()::timestamp_ntz(9) as update_dt
    from source
)

select * from result