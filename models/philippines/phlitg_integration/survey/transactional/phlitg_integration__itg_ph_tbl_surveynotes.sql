{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

with source as(
    select * from {{ source('phlsdl_raw', 'sdl_ph_tbl_surveynotes') }}
),
final as(
    select 
        custcode::varchar(10) as custcode,
        slsperid::varchar(10) as slsperid,
        branchcode::varchar(30) as branchcode,
        iseid::varchar(100) as iseid,
        questionno::number(18,0) as questionno,
        answerseq::number(18,0) as answerseq,
        answernotes::varchar(500) as answernotes,
        createddate::timestamp_ntz(9) as createddate,
        filename::varchar(50) as filename,
        filename_dt::number(14,0) as filename_dt,
        runid::number(14,0) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
     {% if is_incremental() %}
    -- -- this filter will only be applied on an incremental run
     where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
     {% endif %}
)
select * from final