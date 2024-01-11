{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "table"
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_account_attr_ciw') }}
),

--Logical CTE

--Final CTE
final as (
    select
    chrt_accts::varchar(10) as chrt_accts ,
	account::varchar(10) as account,
	objvers::varchar(1) as objvers,
	changed::varchar(1) as changed,
	bal_flag::varchar(1) as bal_flag,
	cstel_flag::varchar(1) as cstel_flag,
	glacc_flag::varchar(1) as glacc_flag,
	logsys::varchar(10) as logsys,
	sem_posit::varchar(16) as sem_posit,
	zbravol1::varchar(30) as zbravol1,
	zbravol2::varchar(30) as zbravol2,
	zbravol3::varchar(30) as zbravol3 ,
	zbravol4::varchar(30) as zbravol4,
	zbravol5::varchar(30) as zbravol5,
	zbravol6::varchar(30) as zbravol6,
	zciwhl1::varchar(30) as zciwhl1,
	zciwhl2::varchar(30) as zciwhl2,
	zciwhl3::varchar(30) as zciwhl3,
	zciwhl4::varchar(30) as zciwhl4,
	zciwhl5::varchar(30) as zciwhl5,
	zciwhl6::varchar(30) as zciwhl6,
	zpb_gl_ty ::varchar(40) as zpb_gl_ty,
	filename::varchar(100) as filename,
	run_id::varchar(100) as run_id,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm 
    from source
)
   

--Final select
select * from final 