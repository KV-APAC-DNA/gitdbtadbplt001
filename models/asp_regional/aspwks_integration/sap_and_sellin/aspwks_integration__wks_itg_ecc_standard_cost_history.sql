with sdl_ecc_standard_cost_history as
(
    select * from {{ source('aspsdl_raw', 'sdl_ecc_standard_cost_history') }}
),
itg_ecc_standard_cost_history as
(
    select * from {{ source('aspitg_integration', 'itg_ecc_standard_cost_history') }}
),
final as
(
    SELECT SRC.mandt,
        SRC.matnr,
        SRC.bwkey,
        SRC.bwtar,
        SRC.lfgja,
        SRC.lfmon,
        SRC.lbkum,
        SRC.salk3,
        SRC.vprsv,
        SRC.verpr,
        SRC.stprs,
        SRC.peinh,
        SRC.bklas,
        SRC.salkv,
        SRC.vksal,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        UPDT_DTTM,
        CASE WHEN 
    TGT.CRT_DTTM IS NULL
    THEN 'I' ELSE 'U' END
    AS CHNG_FLG
    FROM 
    sdl_ecc_standard_cost_history SRC
    LEFT OUTER JOIN (SELECT mandt,matnr,bwkey,bwtar,lfgja,lfmon,CRT_DTTM FROM itg_ecc_standard_cost_history) TGT
    ON SRC.mandt=TGT.mandt
    AND SRC.matnr=TGT.matnr
    AND SRC.bwkey=TGT.bwkey
    AND SRC.bwtar=TGT.bwtar
    AND SRC.lfgja=TGT.lfgja
    AND SRC.lfmon=TGT.lfmon 
)
select * from final