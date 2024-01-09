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
    chrt_accts,
	account ,
	objvers,
	changed ,
	bal_flag,
	cstel_flag ,
	glacc_flag,
	logsys ,
	sem_posit ,
	zbravol1 ,
	zbravol2 ,
	zbravol3 ,
	zbravol4 ,
	zbravol5 ,
	zbravol6 ,
	zciwhl1 ,
	zciwhl2 ,
	zciwhl3 ,
	zciwhl4 ,
	zciwhl5 ,
	zciwhl6 ,
	zpb_gl_ty ,
	--filename ,
	--run_id ,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm 
    from source
)
   

--Final select
select * from final 