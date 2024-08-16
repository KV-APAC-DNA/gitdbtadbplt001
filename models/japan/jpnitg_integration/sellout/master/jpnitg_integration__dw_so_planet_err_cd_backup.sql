

with source as(
    select * from {{ ref('jpnitg_integration__dw_so_planet_err_cd') }}
),

result as(
    select
        jcp_rec_seq::number(10,0) as jcp_rec_seq,
	    error_cd::varchar(20) as error_cd,
	    export_flag::varchar(1) as export_flag
    from source

)

select * from result