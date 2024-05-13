{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["src_sys_cd","cust_str_cd"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with source as(
    select * from DEV_DNA_CORE.SNAPNTAWKS_INTEGRATION.WKS_ITG_POS_STR_CD_SOLD_TO_MAP
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
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm 
    from source
)
select * from final