with 

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_bw_0account') }}

),

final as (

    select
        chrt_accts,
        account,
        objvers,
        changed,
        bal_flag,
        cstel_flag,
        glacc_flag,
        logsys,
        sem_posit,
        zbravol1,
        zbravol2,
        zbravol3,
        zbravol4,
        zbravol5,
        glacctext,
        crt_dttm,
        updt_dttm

    from source

)

select * from final
