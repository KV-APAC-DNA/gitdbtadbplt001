{{
    config(
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{ this }} where 0 != (
                    select count(*) from {{ source('jpnsdl_raw', 'sdl_mds_jp_mt_chn_cstm_rt') }});
                    {% endif %}"
        
    )
}}

with source as(
    select * from {{ source('jpnsdl_raw', 'sdl_mds_jp_mt_chn_cstm_rt') }}
)
,
result as(
    select 
        chn_cd::varchar(5) as chn_cd,
        cstm_cd::varchar(10) as cstm_cd,
        mjr_prod_cd::varchar(18) as mjr_prod_cd,
        dst_rt::number(20,3) as dst_rt,
        current_timestamp()::timestamp_ntz(9) as update_dt,
        NULL::varchar(30) as update_user
    from source
)

select * from result