{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook = "delete from {{this}} itg using {{ ref('ntawks_integration__wks_itg_pos_str_cd_sold_to_map') }} wks where wks.src_sys_cd = itg.src_sys_cd and wks.cust_str_cd = itg.cust_str_cd and wks.chng_flg = 'U';"
    )
}}

with source as(
    select * from {{ ref('ntawks_integration__wks_itg_pos_str_cd_sold_to_map') }} 
),
final as(
    select 
       	name::varchar(50) as clnt,
        seqid::varchar(50) as seqid,
        str_nm::varchar(100) as str_nm,
        src_sys_cd::varchar(50) as src_sys_cd,
        conv_sys_cd::varchar(50) as conv_sys_cd,
        str_type::varchar(50) as str_type,
        cust_str_cd::varchar(50) as cust_str_cd,
        sold_to_cd::varchar(50) as sold_to_cd,
        case
            when chng_flg = 'I' then convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)
            else tgt_crt_dttm::timestamp_ntz(9)
        end as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as upd_dttm 
    from source
)
select * from final