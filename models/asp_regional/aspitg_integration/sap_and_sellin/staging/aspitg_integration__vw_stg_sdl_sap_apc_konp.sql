
with source as(
    select * from {{ source('apc_access', 'apc_konp') }}
),
final as(
    select
        mandt as mandt,
        knumh as knumh,
        kopos as kopos,
        kappl as kappl,
        kschl as kschl,
        knumt as knumt,
        stfkz as stfkz,
        kzbzg as kzbzg,
        kstbm as kstbm,
        konms as konms,
        kstbw as kstbw,
        konws as konws,
        krech as krech,
        kbetr as kbetr,
        konwa as konwa,
        kpein as kpein,
        kmein as kmein,
        prsch as prsch,
        kumza as kumza,
        kumne as kumne,
        meins as meins,
        mxwrt as mxwrt,
        gkwrt as gkwrt,
        pkwrt as pkwrt,
        fkwrt as fkwrt,
        rswrt as rswrt,
        kwaeh as kwaeh,
        ukbas as ukbas,
        kznep as kznep,
        kunnr as kunnr,
        lifnr as lifnr,
        mwsk1 as mwsk1,
        loevm_ko as loevm_ko,
        zaehk_ind as zaehk_ind,
        bomat as bomat,
        kbrue as kbrue,
        kspae as kspae,
        bosta as bosta,
        knuma_pi as knuma_pi,
        knuma_ag as knuma_ag,
        knuma_sq as knuma_sq,
        valtg as valtg,
        valdt as valdt,
        zterm as zterm,
        anzauf as anzauf,
        mikbas as mikbas,
        mxkbas as mxkbas,
        komxwrt as komxwrt,
        klf_stg as klf_stg,
        klf_kal as klf_kal,
        vkkal as vkkal,
        aktnr as aktnr,
        knuma_bo as knuma_bo,
        mwsk2 as mwsk2,
        vertt as vertt,
        vertn as vertn,
        vbewa as vbewa,
        mdflg as mdflg,
        kfrst as kfrst,
        uasta as uasta,
        -- _bev1_ecrtt as _bev1_ecrtt,
        -- _bev1_ecrtn as _bev1_ecrtn,
        -- _bev1_ecewa as _bev1_ecewa,
        -- _deleted_ as _deleted_,
        -- _pk_ as _pk_,
        _createtime_::timestamp_ntz(9) as crt_dttm,
        -- _kafkaoffset_ as _kafkaoffset_,
        -- _upt_ as _upt_,
        -- _kafkatimestamp_ as _kafkatimestamp_,
        -- _infa_bigint_sequence_ as _infa_bigint_sequence_,
        pk_md5 as pk_md5,
        null as file_name
    from source
    where _deleted_='F'

)
select * from final