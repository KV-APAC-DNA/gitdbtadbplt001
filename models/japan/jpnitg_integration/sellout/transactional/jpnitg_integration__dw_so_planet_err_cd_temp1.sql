{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        post_hook = "
                update {{ this }}
                set export_flag = '1'
                where export_flag = '0'
                and jcp_rec_seq in (
                            select jcp_rec_seq
                            from dev_dna_core.snapjpnwks_integration.consistency_error_2
                            where exec_flag in ('manual', 'autocorrect')
                            )
                and jcp_rec_seq in (
                            select jcp_rec_seq
                            from dev_dna_core.snapjpnwks_integration.wk_so_planet_no_dup
                            )
                    "
    )
}}

with err as (
    select * from dev_dna_core.snapjpnitg_integration.dw_so_planet_err
),

no_dup as (
    select * from dev_dna_core.snapjpnwks_integration.wk_so_planet_no_dup
),

err_2 as (
    select * from dev_dna_core.snapjpnwks_integration.consistency_error_2
),

clean as (
    select * from dev_dna_core.snapjpnwks_integration.wk_so_planet_cleansed
)
,
inser1 as (
    select jcp_rec_seq,	
        'DUPL' as error_cd,
		'0' as export_flag,
        current_timestamp()::timestamp_ntz(9) as date_check
        from err
        where
			export_flag = '0'
        {% if is_incremental() %}
            and err.jcp_create_date > (select max(date_check) from {{ this }})
        {% endif %}
)
,
inser2 as (
   select jcp_rec_seq,
    'DUPL' as error_cd,
    '0' as export_flag,
    current_timestamp()::timestamp_ntz(9) as date_check
from err
where export_flag = '0'
    and jcp_rec_seq in (
        select distinct jcp_rec_seq
        from no_dup
        )
    {% if is_incremental() %}
        and err.jcp_create_date > (select max(date_check) from {{ this }})
    {% endif %}
),

insert1 as (
    select * from inser1
    union all
    select * from inser2
)

,

result as
    (select 
        jcp_rec_seq::number(10,0) as jcp_rec_seq,
        error_cd::varchar(20) as error_cd,
        export_flag::varchar(1) as export_flag,
        date_check
    from insert1
    )

select * from result