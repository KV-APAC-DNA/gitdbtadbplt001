{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['jj_mnth_id'],
        pre_hook= "delete from {{this}} where (jj_mnth_id) in (
        select distinct jj_mnth_id from {{ source('phlsdl_raw', 'sdl_ph_bp_trgt') }});"
    )
}}
with source as
(
    select * from {{ source('phlsdl_raw', 'sdl_ph_bp_trgt') }}
),
final as
(
    select
    jj_mnth_id::number(18,0) as jj_mnth_id,
	cust_id::number(18,0) as cust_id,
    trgt_type::varchar(10) as trgt_type,
    brnd_cd::varchar(100) as brnd_cd,
    tp_trgt_amt::number(15,4) as tp_trgt_amt,
    concat(left(filename, 10), '.xlsx')::varchar(100) as filename,
    run_id::varchar(50) as run_id,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final