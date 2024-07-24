{{
    config(
        post_hook = 
            "update {{ this }} set export_flag = '1' where export_flag = '0' and (jcp_rec_seq in (
                                                select jcp_rec_seq from dev_dna_core.snapjpnwks_integration.consistency_error_2
                                                where exec_flag in ('delete')
                                                )
                                            or jcp_rec_seq in (
                                                select jcp_rec_seq from dev_dna_core.snapjpnwks_integration.wk_so_planet_cleansed
                                                )
                                            or jcp_rec_seq in (
                                                select jcp_rec_seq from {{ this }}
                                                where error_cd = 'nrtl'
                                                ))
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
),

insert2 as (
    select a.jcp_rec_seq,
    a.error_cd,
    a.export_flag,
    current_timestamp()::timestamp_ntz(9) as date_check
from (
    (
        select a.jcp_rec_seq,
            case 
                when error_cd = 'ERR_001'
                    then 'E001'
                else '0'
                end error_cd,
            '0' export_flag
        from err_2 a
        )
    
    union
    
    (
        select a.jcp_rec_seq,
            case 
                when error_cd = 'ERR_002'
                    then 'E002'
                else '0'
                end error_cd,
            '0' export_flag
        from err_2 a
        )
    
    union
    
    (
        select a.jcp_rec_seq,
            case 
                when error_cd = 'ERR_004'
                    then 'E004'
                else '0'
                end error_cd,
            '0' export_flag
        from err_2 a
        )
    
    union
    
    (
        select a.jcp_rec_seq,
            case 
                when error_cd = 'ERR_007'
                    then 'E007'
                else '0'
                end error_cd,
            '0' export_flag
        from err_2 a
        )
    
    union
    
    (
        select a.jcp_rec_seq,
            case 
                when error_cd = 'ERR_008'
                    then 'E008'
                else '0'
                end error_cd,
            '0' export_flag
        from err_2 a
        )
    
    union
    
    (
        select a.jcp_rec_seq,
            case 
                when error_cd = 'ERR_009'
                    then 'E009'
                else '0'
                end error_cd,
            '0' export_flag
        from err_2 a
        )
    
    union
    
    (
        select a.jcp_rec_seq,
            case 
                when error_cd = 'ERR_012'
                    then 'E012'
                else '0'
                end error_cd,
            '0' export_flag
        from err_2 a
        )
    
    union
    
    (
        select a.jcp_rec_seq,
            case 
                when error_cd = 'ERR_014'
                    then 'E014'
                else '0'
                end error_cd,
            '0' export_flag
        from err_2 a
        )
    
    union
    
    (
        select a.jcp_rec_seq,
            case 
                when error_cd = 'ERR_017'
                    then 'E017'
                else '0'
                end error_cd,
            '0' export_flag
        from err_2 a
        )
    
    union
    
    (
        select a.jcp_rec_seq,
            case 
                when error_cd = 'ERR_018'
                    then 'E018'
                else '0'
                end error_cd,
            '0' export_flag
        from err_2 a
        )
    
    union
    
    (
        select a.jcp_rec_seq,
            case 
                when error_cd = 'ERR_020'
                    then 'E020'
                else '0'
                end error_cd,
            '0' export_flag
        from err_2 a
        )
    ) a
where a.error_cd != '0'
{% if is_incremental() %}
    and err.jcp_create_date > (select max(date_check) from {{ this }})
{% endif %}
)
--select * from insert2;
,

intermed_insert as (
select * from {{ ref('jpnitg_integration__dw_so_planet_err_cd_temp1') }}
union all
select * from insert2
)

--select distinct jcp_rec_seq from intermed_insert;
--group by export_flag;

,

result as 
(select 
    jcp_rec_seq::number(10,0) as jcp_rec_seq,
	error_cd::varchar(20) as error_cd,
	export_flag::varchar(1) as export_flag,
    date_check
from intermed_insert)

select * from result
