{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['jj_mnth_id'],
        pre_hook= "delete from {{this}} where (jj_mnth_id) in (
        select distinct jj_mnth_id from {{ source('phlsdl_raw', 'sdl_ph_bp_trgt') }}
        where filename not in (
        select distinct file_name from {{SOURCE('phlwks_integration','TRATBL_sdl_ph_bp_trgt__null_test')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','TRATBL_sdl_ph_bp_trgt__duplicate_test')}}
    )
        );"
    )
}}
with source as
(
    select *,dense_rank() over(partition by jj_mnth_id  order by file_name desc) as rnk
    from {{ source('phlsdl_raw', 'sdl_ph_bp_trgt') }}
    where filename not in (
        select distinct file_name from {{SOURCE('phlwks_integration','TRATBL_sdl_ph_bp_trgt__null_test')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','TRATBL_sdl_ph_bp_trgt__duplicate_test')}}
    )
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
    current_timestamp()::timestamp_ntz(9) as updt_dttm,
    filename
    from source
)
select * from final