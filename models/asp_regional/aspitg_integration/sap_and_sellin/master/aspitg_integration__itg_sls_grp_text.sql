{{
    config(
        materialized="incremental",
        incremental_strategy= "merge",
        unique_key=  ['clnt','lang_key','sls_grp'],
        merge_exclude_columns=["crt_dttm"]
    )
}}

--import CTE

with source as (
    select * from {{ ref('aspwks_integration__wks_itg_sls_grp_text' )}}
),

--Logical CTE

trans as(
    select 
        mandt as clnt,
        spras as lang_key,
        vkgrp as sls_grp,
        bezei as de,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
),
final as(
    select 
        clnt::number(18,0) as clnt,
        lang_key::varchar(1) as lang_key,
        sls_grp::varchar(3) as sls_grp,
        de::varchar(40) as de,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)
--final select
select * from final