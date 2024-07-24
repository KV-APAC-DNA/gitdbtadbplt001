{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
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
insert3 as (
    select a.jcp_rec_seq,
        'FUTR' as error_cd,
        '0' as export_flag,
        current_timestamp()::timestamp_ntz(9) as date_check
    from err a where a.export_flag = 0
    and a.jcp_rec_seq not in (
        select b.jcp_rec_seq
        from {{ ref('jpnitg_integration__dw_so_planet_err_cd_temp2') }} b
        where b.export_flag = '0'
        ) 
)
-- select * from insert3;
,

result as (
    select * from {{ ref('jpnitg_integration__dw_so_planet_err_cd_temp2') }}
    union all
    select * from insert3
),

final as 
(select 
    r.jcp_rec_seq::number(10,0) as jcp_rec_seq,
	r.error_cd::varchar(20) as error_cd,
	r.export_flag::varchar(1) as export_flag,
    current_timestamp()::timestamp_ntz(9) as date_check
from result r,err
{% if is_incremental() %}
   where err.jcp_create_date > (select max(date_check) from {{ this }})
{% endif %}
)

select * from final
