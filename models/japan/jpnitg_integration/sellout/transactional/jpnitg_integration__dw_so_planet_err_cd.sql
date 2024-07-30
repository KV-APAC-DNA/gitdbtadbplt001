{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        post_hook = "
                    delete from {{ ref('jpnwks_integration__wk_so_planet_today')}}
                    where JCP_REC_SEQ in 
                    (Select distinct JCP_REC_SEQ from  {{ ref('jpnitg_integration__dw_so_planet_err') }} where EXPORT_FLAG = '0');
                    "
    )
}}

with dw_so_planet_err as (
    select * from {{ ref('jpnitg_integration__dw_so_planet_err') }}
),

cte1 as (
    select jcp_rec_seq,	
        'DUPL' as error_cd,
		'0' as export_flag,
        from dw_so_planet_err
        where
			export_flag = '0'
        
)
,


result as(
    select 
        jcp_rec_seq::number(10,0) as jcp_rec_seq,
        error_cd::varchar(20) as error_cd,
        export_flag::varchar(1) as export_flag,
    from cte1
)

select * from result
