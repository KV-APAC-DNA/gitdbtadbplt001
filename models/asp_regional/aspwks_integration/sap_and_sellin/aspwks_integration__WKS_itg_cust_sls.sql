{{
    config(
        alias= "wks_itg_cust_sls",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="view",
        tags=["daily"]
    )
}}

--import CTE
with sources as(
    SELECT *
    FROM {{ ref('aspitg_integration__stg_sdl_sap_ecc_customer_sales') }}
),
--logical CTE
final as(
    select 
        lprio,
        awahr,
        zterm,
        prata,
        prat9,
        prat8,
        prat7,
        prat6,
        prat5,
        prat4,
        prat3,
        prat2,
        prat1,
        prfre,
        kurst,
        bokre,
        kvgr5,
        kvgr4,
        kvgr3,
        kvgr2,
        kvgr1,
        vsort,
        vkbur,
        vkgrp,
        vwerk,
        ktgrd,
        klabc,
        waers,
        kvakz,
        perrl,
        perfk,
        mrnkz,
        faksd,
        vsbed,
        eikto,
        chspl,
        kzazu,
        kztlf,
        autlf,
        lifsd,
        inco2,
        inco1,
        pltyp,
        konda,
        bzirk,
        kdgrp,
        kalks,
        aufsd,
        versg,
        loevm,
        begru,
        ernam,
        spart,
        vtweg,
        vkorg,
        kunnr,
        mandt,
        boidt,
        erdat,
        zzsalesrep,
        kvawt,
        antlf,
        updt_dttm,
        crt_dttm
     from sources
)
--final select
select * from final