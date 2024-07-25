{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with dw_so_planet_err as (
    select * from {{ ref('jpnitg_integration__dw_so_planet_err') }}
),

wk_so_planet_no_dup as (
    select * from {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
),

consistency_error_2 as (
    select * from {{ ref('jpnwks_integration_consistency_error_2') }}
),

wk_so_planet_cleansed as (
    select * from {{ ref('jpnwks_integration__wk_so_planet_cleansed') }}
)
,
cte1 as (
    select jcp_rec_seq,	
        'DUPL' as error_cd,
		'0' as export_flag,
        from dw_so_planet_err
        where
			export_flag = '0'
        
)
,
cte2 as (
   select jcp_rec_seq,
    'DUPL' as error_cd,
    '0' as export_flag,
from dw_so_planet_err
where export_flag = '0'
    and jcp_rec_seq in (
        select distinct jcp_rec_seq
        from wk_so_planet_no_dup
        )
    
),

insert1 as (
    select * from cte1
    union all
    select * from cte2
),
cond1 as(
    select jcp_rec_seq
    from dev_dna_core.jpnwks_integration.consistency_error_2
    where exec_flag in ('MANUAL', 'AUTOCORRECT')
),
cond2 as(
    select jcp_rec_seq
    from wk_so_planet_no_dup
),
update_table as(
    select 
        insert1.jcp_rec_seq,
        error_cd,
        --'1' as export_flag,
        case when export_flag='0'
        then '1' end as export_flag,
    from insert1
    left join cond1 on cond1.jcp_rec_seq = insert1.jcp_rec_seq
    left join cond2 on cond2.jcp_rec_seq = insert1.jcp_rec_seq
     where cond1.jcp_rec_seq is not null and cond2.jcp_rec_seq is not null
    union all
     select 
        insert1.jcp_rec_seq,
        error_cd,
        export_flag,
    from insert1
   left join cond1 on cond1.jcp_rec_seq = insert1.jcp_rec_seq
    left join cond2 on cond2.jcp_rec_seq = insert1.jcp_rec_seq
     where cond1.jcp_rec_seq is null and cond2.jcp_rec_seq is null
),
insert2 as (
    select * from update_table
    union all
    select a.jcp_rec_seq,
    a.error_cd,
    a.export_flag,
    from (
        (
            select a.jcp_rec_seq,
                case 
                    when error_cd = 'ERR_001'
                        then 'E001'
                    else '0'
                    end error_cd,
                '0' export_flag
            from consistency_error_2 a
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
            from consistency_error_2 a
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
            from consistency_error_2 a
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
            from consistency_error_2 a
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
            from consistency_error_2 a
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
            from consistency_error_2 a
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
            from consistency_error_2 a
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
            from consistency_error_2 a
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
            from consistency_error_2 a
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
            from consistency_error_2 a
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
            from consistency_error_2 a
            )
        ) a
    where a.error_cd != '0'
    
),
cond3 as(
    select jcp_rec_seq from consistency_error_2
        where exec_flag in ('DELETE')
),
cond4 as(
     select jcp_rec_seq from wk_so_planet_cleansed

),
cond5 as(
 select jcp_rec_seq from insert2 where error_cd = 'NRTL'
),
unionofcond as(
select * from cond3
union 
select * from cond4
union 
select * from cond5
),
update_table2 as(
    select 
        insert2.jcp_rec_seq,
        error_cd,
       --  '1' as export_flag,
        case when export_flag='0'
        then '1' end as export_flag,
    from insert2
    left join unionofcond on unionofcond.jcp_rec_seq = insert2.jcp_rec_seq
    -- left join cond4 on cond4.jcp_rec_seq = insert2.jcp_rec_seq
    -- left join cond5 on cond5.jcp_rec_seq = insert2.jcp_rec_seq
    where unionofcond.jcp_rec_seq is not null
    union all
     select 
        insert2.jcp_rec_seq,
        error_cd,
        export_flag,
    from insert2 
   left join unionofcond on unionofcond.jcp_rec_seq = insert2.jcp_rec_seq
    -- left join cond4 on cond4.jcp_rec_seq = insert2.jcp_rec_seq
    -- left join cond5 on cond5.jcp_rec_seq = insert2.jcp_rec_seq
    where unionofcond.jcp_rec_seq is null
),
insert3 as (
    select * from update_table2
    union all
    select a.jcp_rec_seq,
        'FUTR' as error_cd,
        '0' as export_flag,
    from dw_so_planet_err a where a.export_flag = 0
    and a.jcp_rec_seq not in (
        select b.jcp_rec_seq
        from update_table2 b
        where b.export_flag = '0'
        )     
),
result as(
    select 
        jcp_rec_seq::number(10,0) as jcp_rec_seq,
        error_cd::varchar(20) as error_cd,
        export_flag::varchar(1) as export_flag,
    from insert3
)

select * from result
